//! Edge TTS synthesizer. Uses `msedge-tts`'s synchronous API on a tokio
//! blocking thread because the crate's async API is built on async-std and
//! its types don't interop with tokio runtimes.
//!
//! Microsoft's free read-aloud endpoint only accepts
//! `audio-24khz-48kbitrate-mono-mp3` — raw PCM formats are silently
//! rejected. We collect all mp3 frames for one sentence, decode to PCM
//! once, and forward through the channel. Per-sentence batching keeps
//! the implementation simple and time-to-first-audio is still bounded by
//! one sentence's synthesis time.

use anyhow::{Context, Result, anyhow};
use msedge_tts::tts::SpeechConfig;
use msedge_tts::tts::stream::{SynthesizedResponse, msedge_tts_split};
use std::io::Cursor;
use symphonia::core::audio::{AudioBufferRef, Signal};
use symphonia::core::codecs::DecoderOptions;
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::FormatOptions;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::meta::MetadataOptions;
use symphonia::core::probe::Hint;
use tokio::sync::mpsc;

/// Sample rate of the PCM we emit. Microsoft serves 24 kHz mp3.
pub const SAMPLE_RATE: u32 = 24_000;

const AUDIO_FORMAT: &str = "audio-24khz-48kbitrate-mono-mp3";

/// Synthesize one sentence, decode the resulting mp3 to PCM, and forward
/// 16-bit little-endian mono samples through `tx`. The synthesis +
/// decode runs on a blocking thread.
pub async fn synthesize_one(text: String, voice: String, tx: mpsc::Sender<Vec<u8>>) -> Result<()> {
    tokio::task::spawn_blocking(move || -> Result<()> {
        let mp3 = synthesize_mp3(&text, &voice)?;
        if mp3.is_empty() {
            return Ok(());
        }
        let pcm = decode_mp3(&mp3)?;
        if tx.blocking_send(pcm).is_err() {
            // Receiver dropped — caller hung up.
            return Ok(());
        }
        Ok(())
    })
    .await
    .context("spawn_blocking join")?
}

/// Open a fresh WebSocket to Microsoft, send the synthesize request,
/// drain audio frames, and return concatenated mp3 bytes.
fn synthesize_mp3(text: &str, voice: &str) -> Result<Vec<u8>> {
    let (mut sender, mut reader) = msedge_tts_split().context("opening Edge TTS WebSocket")?;
    let cfg = SpeechConfig {
        voice_name: voice.to_string(),
        audio_format: AUDIO_FORMAT.to_string(),
        pitch: 0,
        rate: 0,
        volume: 0,
    };
    sender
        .send(text, &cfg)
        .context("sending Edge TTS synthesize request")?;
    let mut mp3 = Vec::new();
    // Sync `read()` returns None for non-audio frames; end-of-stream is
    // when `can_read()` flips to false after turn_start+response+turn_end.
    loop {
        if let Some(SynthesizedResponse::AudioBytes(bytes)) =
            reader.read().context("reading Edge TTS response")?
        {
            mp3.extend_from_slice(&bytes);
        }
        if !reader.can_read() {
            break;
        }
    }
    Ok(mp3)
}

/// Decode mp3 bytes to little-endian 16-bit mono PCM. Microsoft serves
/// mono mp3 at 24 kHz; if either changes, we'll loudly fail rather than
/// silently misinterpret the format.
fn decode_mp3(mp3: &[u8]) -> Result<Vec<u8>> {
    let mss = MediaSourceStream::new(Box::new(Cursor::new(mp3.to_vec())), Default::default());
    let mut hint = Hint::new();
    hint.with_extension("mp3");
    let probed = symphonia::default::get_probe()
        .format(
            &hint,
            mss,
            &FormatOptions::default(),
            &MetadataOptions::default(),
        )
        .context("probing mp3 stream")?;
    let mut format = probed.format;
    let track = format
        .default_track()
        .ok_or_else(|| anyhow!("no default track in mp3 stream"))?;
    let track_id = track.id;
    let track_rate = track.codec_params.sample_rate.unwrap_or(SAMPLE_RATE);
    let track_channels = track.codec_params.channels.map(|c| c.count()).unwrap_or(1);
    if track_rate != SAMPLE_RATE {
        anyhow::bail!(
            "expected {} Hz from Edge TTS, got {} Hz",
            SAMPLE_RATE,
            track_rate
        );
    }
    if track_channels != 1 {
        anyhow::bail!("expected mono mp3, got {} channels", track_channels);
    }
    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .context("creating mp3 decoder")?;

    let mut pcm: Vec<u8> = Vec::with_capacity(mp3.len() * 8);
    loop {
        let packet = match format.next_packet() {
            Ok(p) => p,
            Err(SymphoniaError::IoError(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break;
            }
            Err(SymphoniaError::ResetRequired) => break,
            Err(e) => return Err(e).context("reading mp3 packet"),
        };
        if packet.track_id() != track_id {
            continue;
        }
        match decoder.decode(&packet) {
            Ok(audio_buf) => append_samples(&audio_buf, &mut pcm),
            Err(SymphoniaError::DecodeError(_)) => continue, // skip malformed frame
            Err(e) => return Err(e).context("decoding mp3 frame")?,
        }
    }
    Ok(pcm)
}

fn append_samples(buf: &AudioBufferRef<'_>, out: &mut Vec<u8>) {
    // We've validated mono above, so just take channel 0.
    match buf {
        AudioBufferRef::F32(b) => {
            for &s in b.chan(0) {
                let v = (s.clamp(-1.0, 1.0) * 32767.0) as i16;
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        AudioBufferRef::S16(b) => {
            for &s in b.chan(0) {
                out.extend_from_slice(&s.to_le_bytes());
            }
        }
        AudioBufferRef::S32(b) => {
            for &s in b.chan(0) {
                let v = (s >> 16) as i16;
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        _ => {
            // mp3 decodes to F32 in symphonia; other variants are
            // theoretical. Leaving them out keeps the size down.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// End-to-end synthesis test. Hits Microsoft's read-aloud endpoint
    /// for real, so this catches:
    ///   * `msedge-tts` upstream breakage if MS rotates auth tokens
    ///   * audio-format rejection (only one mp3 variant works on the
    ///     free endpoint — see module docs)
    ///   * symphonia mp3 decode regressions
    /// The whole point of the crate is that this round-trip works, so
    /// CI failure here is the right signal.
    #[tokio::test]
    async fn end_to_end_synthesis() {
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn(synthesize_one(
            "Hello, world. This is a smoke test.".to_string(),
            "en-US-AvaNeural".to_string(),
            tx,
        ));
        let mut total = 0usize;
        while let Some(chunk) = rx.recv().await {
            total += chunk.len();
        }
        task.await
            .expect("task panicked")
            .expect("synthesis failed");
        // ~3s of speech at 24 kHz mono 16-bit ≈ 144 KB. Set a generous
        // floor so the test isn't flaky on faster/slower delivery, but
        // big enough to catch "decode produced nothing" regressions.
        assert!(
            total > 30_000,
            "expected substantial PCM output, got {} bytes",
            total,
        );
        // PCM is 16-bit so byte count must be even.
        assert_eq!(total % 2, 0, "PCM byte count not aligned to i16");
    }
}
