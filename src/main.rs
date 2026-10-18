//! Wyoming TTS server backed by Microsoft Edge's read-aloud API.
//!
//! Synthesis is sentence-pipelined: input text is split on `.!?`, each
//! sentence opens its own WebSocket to MS in parallel (capped by a
//! semaphore), and PCM chunks are forwarded in sentence order. Time to
//! first audio is roughly one sentence's synthesis time regardless of
//! total reply length.
//!
//! Advertises `supports_synthesize_streaming` so HA's pipeline flips
//! `stream_response: true`, which is what Wyoming voice satellites watch
//! for to consume audio chunks live (rather than waiting on the
//! tts_proxy URL to fully buffer).

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, mpsc};
use tokio::task::JoinHandle;

mod edge;
mod sentence;

/// PCM chunk size emitted to Wyoming clients (in samples; 16-bit mono so
/// 4096 samples = 8192 bytes ≈ 170 ms at 24 kHz). Matches the cadence
/// other Wyoming TTS servers use.
const CHUNK_SAMPLES: usize = 4096;

/// Default curated voice list. Override with the `VOICES` env var
/// (space-separated). The Edge TTS API has 100+ voices; this list is
/// the popular English subset.
const DEFAULT_VOICES: &[&str] = &[
    "en-GB-SoniaNeural",
    "en-GB-RyanNeural",
    "en-GB-LibbyNeural",
    "en-GB-ThomasNeural",
    "en-US-AvaNeural",
    "en-US-AvaMultilingualNeural",
    "en-US-EmmaNeural",
    "en-US-JennyNeural",
    "en-US-AndrewNeural",
    "en-US-BrianNeural",
    "en-US-GuyNeural",
    "en-US-AriaNeural",
];

const DEFAULT_DEFAULT_VOICE: &str = "en-US-AvaNeural";

/// Concurrency cap on parallel WebSockets to Microsoft per synthesis
/// request. Three matches `wyoming_openai`'s default and is plenty for
/// keeping audio playback ahead of generation.
const DEFAULT_MAX_CONCURRENT: usize = 3;

#[derive(Debug, Deserialize, Serialize)]
struct Header {
    #[serde(rename = "type")]
    event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    data_length: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    payload_length: Option<usize>,
}

async fn read_event<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
) -> Result<Option<Header>> {
    let mut line = String::new();
    let n = reader.read_line(&mut line).await?;
    if n == 0 {
        return Ok(None);
    }
    let mut hdr: Header = serde_json::from_str(line.trim())?;
    if let Some(dl) = hdr.data_length
        && dl > 0
    {
        let mut buf = vec![0u8; dl];
        reader.read_exact(&mut buf).await?;
        hdr.data = Some(serde_json::from_slice(&buf)?);
    }
    if let Some(pl) = hdr.payload_length
        && pl > 0
    {
        let mut buf = vec![0u8; pl];
        reader.read_exact(&mut buf).await?;
    }
    Ok(Some(hdr))
}

async fn write_event<W: AsyncWriteExt + Unpin>(
    writer: &mut W,
    event_type: &str,
    data: Option<Value>,
    payload: Option<&[u8]>,
) -> Result<()> {
    let data_bytes = data.as_ref().map(serde_json::to_vec).transpose()?;
    let mut hdr = json!({ "type": event_type });
    if let Some(d) = &data_bytes {
        hdr["data_length"] = json!(d.len());
    }
    if let Some(p) = payload {
        hdr["payload_length"] = json!(p.len());
    }
    let mut line = serde_json::to_vec(&hdr)?;
    line.push(b'\n');
    writer.write_all(&line).await?;
    if let Some(d) = data_bytes {
        writer.write_all(&d).await?;
    }
    if let Some(p) = payload {
        writer.write_all(p).await?;
    }
    writer.flush().await?;
    Ok(())
}

struct Config {
    voices: Vec<String>,
    default_voice: String,
    max_concurrent: usize,
}

impl Config {
    fn resolve_voice(&self, requested: Option<&str>) -> String {
        match requested {
            Some(v) if self.voices.iter().any(|x| x == v) => v.to_string(),
            _ => self.default_voice.clone(),
        }
    }
}

fn build_info(cfg: &Config) -> Value {
    let voice_list: Vec<Value> = cfg
        .voices
        .iter()
        .map(|name| {
            json!({
                "name": name,
                "description": name,
                "attribution": {"name": "Microsoft Edge TTS", "url": "https://learn.microsoft.com/azure/ai-services/speech-service/language-support"},
                "installed": true,
                "languages": [language_tag(name)],
                "version": null
            })
        })
        .collect();

    json!({
        "tts": [{
            "name": "edge-tts",
            "description": "Microsoft Edge TTS (streaming, sentence-pipelined)",
            "attribution": {"name": "Microsoft Edge read-aloud", "url": "https://github.com/domdomegg/wyoming-edge-tts"},
            "installed": true,
            "version": env!("CARGO_PKG_VERSION"),
            "supports_synthesize_streaming": true,
            "voices": voice_list
        }]
    })
}

/// Convert an Edge voice id like `en-GB-SoniaNeural` to a Wyoming
/// language code. Wyoming wants the BCP-47-ish locale prefix; the engine
/// itself uses the full voice id.
fn language_tag(voice: &str) -> String {
    voice.splitn(3, '-').take(2).collect::<Vec<_>>().join("-")
}

struct SentenceJob {
    rx: mpsc::Receiver<Vec<u8>>,
    handle: JoinHandle<Result<()>>,
}

/// Kick off parallel synthesis for each sentence and return per-sentence
/// receivers in order. Permits cap concurrency so we don't open more
/// WebSockets to Microsoft than we promised.
fn spawn_sentence_jobs(
    sentences: Vec<String>,
    voice: String,
    sem: Arc<Semaphore>,
) -> Vec<SentenceJob> {
    sentences
        .into_iter()
        .map(|sentence| {
            let (tx, rx) = mpsc::channel(8);
            let voice = voice.clone();
            let sem = Arc::clone(&sem);
            let handle = tokio::spawn(async move {
                let _permit = sem
                    .acquire_owned()
                    .await
                    .context("acquiring concurrency permit")?;
                edge::synthesize_one(sentence, voice, tx).await
            });
            SentenceJob { rx, handle }
        })
        .collect()
}

/// Stream synthesized audio for `text` to the Wyoming client. Splits the
/// input on sentence boundaries and consumes per-sentence channels in
/// order so audio plays back in the correct sequence. Emits
/// `audio-start` and `audio-chunk*`; the caller is responsible for the
/// terminator (`audio-stop` for non-streaming, `synthesize-stopped` for
/// streaming) since the two protocol modes use different events.
async fn synthesize_and_stream(
    write_half: &mut tokio::net::tcp::OwnedWriteHalf,
    text: &str,
    voice: &str,
    sem: Arc<Semaphore>,
) -> Result<()> {
    let sentences = sentence::split(text);
    if sentences.is_empty() {
        return Ok(());
    }

    let audio_meta = json!({
        "rate": edge::SAMPLE_RATE,
        "width": 2,
        "channels": 1,
    });
    write_event(write_half, "audio-start", Some(audio_meta.clone()), None).await?;

    let chunk_bytes = CHUNK_SAMPLES * 2;
    let jobs = spawn_sentence_jobs(sentences, voice.to_string(), sem);

    for SentenceJob { mut rx, handle } in jobs {
        // Pending unflushed PCM kept here until we have a full chunk to
        // emit. Edge TTS hands us variable-size frames; satellites
        // generally prefer fixed-size chunks.
        let mut leftover: Vec<u8> = Vec::new();
        while let Some(bytes) = rx.recv().await {
            leftover.extend_from_slice(&bytes);
            while leftover.len() >= chunk_bytes {
                let rest = leftover.split_off(chunk_bytes);
                write_event(
                    write_half,
                    "audio-chunk",
                    Some(audio_meta.clone()),
                    Some(&leftover),
                )
                .await?;
                leftover = rest;
            }
        }
        if !leftover.is_empty() {
            write_event(
                write_half,
                "audio-chunk",
                Some(audio_meta.clone()),
                Some(&leftover),
            )
            .await?;
        }
        // Surface synthesis errors before moving to the next sentence.
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("sentence synth error: {:#}", e),
            Err(e) => eprintln!("sentence task panicked: {:#}", e),
        }
    }
    Ok(())
}

fn extract_voice<'a>(data: Option<&'a Value>, fallback: &'a str) -> &'a str {
    data.and_then(|d| d.get("voice"))
        .and_then(|v| v.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or(fallback)
}

async fn handle_client(
    stream: TcpStream,
    info: Arc<Value>,
    cfg: Arc<Config>,
    sem: Arc<Semaphore>,
) -> Result<()> {
    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Streaming-input protocol state. HA's streaming flow is:
    //   synthesize-start (voice) → synthesize-chunk (text) [* N] →
    //   synthesize (full text, "for compatibility") → synthesize-stop
    // and the response terminator is `synthesize-stopped` instead of
    // `audio-stop`. We track whether we're inside a streaming session
    // (after seeing synthesize-start) so we can:
    //   * ignore the compatibility `synthesize` echo (would otherwise
    //     fire synthesis twice and leave HA waiting forever for a
    //     `synthesize-stopped` that never came),
    //   * emit the right terminator at the end.
    let mut stream_text = String::new();
    let mut stream_voice: Option<String> = None;
    let mut streaming_session = false;

    while let Some(hdr) = read_event(&mut reader).await? {
        match hdr.event_type.as_str() {
            "describe" => {
                write_event(&mut write_half, "info", Some((*info).clone()), None).await?;
            }
            "synthesize" => {
                if streaming_session {
                    // Compatibility echo from HA — synthesis happens on
                    // synthesize-stop, not here. Drop it.
                    continue;
                }
                let data = hdr.data.as_ref();
                let text = data
                    .and_then(|d| d.get("text"))
                    .and_then(|t| t.as_str())
                    .unwrap_or("");
                if text.is_empty() {
                    continue;
                }
                let voice = cfg.resolve_voice(Some(extract_voice(data, &cfg.default_voice)));
                synthesize_and_stream(&mut write_half, text, &voice, Arc::clone(&sem)).await?;
                write_event(&mut write_half, "audio-stop", None, None).await?;
            }
            "synthesize-start" => {
                stream_text.clear();
                let v = extract_voice(hdr.data.as_ref(), &cfg.default_voice);
                stream_voice = Some(cfg.resolve_voice(Some(v)));
                streaming_session = true;
            }
            "synthesize-chunk" => {
                if let Some(text) = hdr
                    .data
                    .as_ref()
                    .and_then(|d| d.get("text"))
                    .and_then(|t| t.as_str())
                {
                    stream_text.push_str(text);
                }
            }
            "synthesize-stop" => {
                if !stream_text.is_empty() {
                    let voice = stream_voice
                        .clone()
                        .unwrap_or_else(|| cfg.default_voice.clone());
                    synthesize_and_stream(&mut write_half, &stream_text, &voice, Arc::clone(&sem))
                        .await?;
                }
                write_event(&mut write_half, "synthesize-stopped", None, None).await?;
                stream_text.clear();
                stream_voice = None;
                streaming_session = false;
            }
            _ => {}
        }
    }
    Ok(())
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn parse_voice_list(raw: &str) -> Vec<String> {
    raw.split_whitespace()
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

#[tokio::main]
async fn main() -> Result<()> {
    let port: u16 = env_or("PORT", 10210u16);
    let max_concurrent: usize = env_or("MAX_CONCURRENT", DEFAULT_MAX_CONCURRENT);
    let voices = std::env::var("VOICES")
        .ok()
        .map(|s| parse_voice_list(&s))
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_VOICES.iter().map(|s| s.to_string()).collect());
    let default_voice =
        std::env::var("DEFAULT_VOICE").unwrap_or_else(|_| DEFAULT_DEFAULT_VOICE.to_string());
    if !voices.contains(&default_voice) {
        anyhow::bail!(
            "DEFAULT_VOICE {} not in VOICES list ({:?})",
            default_voice,
            voices
        );
    }

    let cfg = Arc::new(Config {
        voices,
        default_voice,
        max_concurrent,
    });
    let info = Arc::new(build_info(&cfg));
    let sem = Arc::new(Semaphore::new(cfg.max_concurrent));

    eprintln!(
        "Advertising {} voices; default={} max_concurrent={}",
        cfg.voices.len(),
        cfg.default_voice,
        cfg.max_concurrent,
    );

    let addr = format!("[::]:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    eprintln!("Listening on {}", addr);

    loop {
        let (stream, peer) = listener.accept().await?;
        eprintln!("connection from {}", peer);
        let info = Arc::clone(&info);
        let cfg = Arc::clone(&cfg);
        let sem = Arc::clone(&sem);
        tokio::spawn(async move {
            if let Err(e) = handle_client(stream, info, cfg, sem).await {
                eprintln!("client error: {:#}", e);
            }
        });
    }
}
