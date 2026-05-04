//! Quick standalone Edge TTS test that doesn't need the Wyoming server.
//! Useful for diagnosing whether `msedge-tts` works in this environment
//! without the WS server in the loop. `cargo run --example smoke`.

use msedge_tts::tts::SpeechConfig;
use msedge_tts::tts::stream::{SynthesizedResponse, msedge_tts_split};

fn main() -> anyhow::Result<()> {
    let (mut sender, mut reader) = msedge_tts_split()?;
    let cfg = SpeechConfig {
        voice_name: "en-US-AvaNeural".to_string(),
        audio_format: "audio-24khz-48kbitrate-mono-mp3".to_string(),
        pitch: 0,
        rate: 0,
        volume: 0,
    };
    sender.send("Hello, world.", &cfg)?;
    let mut total = 0usize;
    let mut chunks = 0usize;
    loop {
        if let Some(SynthesizedResponse::AudioBytes(b)) = reader.read()? {
            chunks += 1;
            total += b.len();
        }
        if !reader.can_read() {
            break;
        }
    }
    eprintln!("done: chunks={chunks} mp3_bytes={total}");
    Ok(())
}
