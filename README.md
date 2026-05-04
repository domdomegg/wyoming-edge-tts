# wyoming-edge-tts

Wyoming protocol TTS server backed by Microsoft Edge's free read-aloud API. Sentence-pipelined streaming, ~10-30 MB RSS, single static binary.

## Why

Most Edge-TTS-on-Wyoming setups stack `wyoming_openai` (Wyoming → OpenAI proxy) on top of `openai-edge-tts` (OpenAI API → edge-tts shim) — two Python processes adding up to ~300-400 MB RSS, plus an OpenAI API translation layer that contributes nothing. This skips both layers: a single Rust binary speaks Wyoming on one side and Microsoft's read-aloud WebSocket on the other.

## Features

- **Sentence-pipelined streaming.** Input text is split on `.!?`, each sentence opens its own WebSocket to Microsoft (capped at `MAX_CONCURRENT`, default 3), and PCM is forwarded in sentence order. Time-to-first-audio is ~one sentence's synthesis time regardless of total reply length.
- **Advertises `supports_synthesize_streaming: true`** so HA's pipeline flips `stream_response: true` — Wyoming voice satellites then consume audio chunks live rather than waiting on the `tts_proxy` URL to fully buffer.
- **Accepts both `synthesize` (one-shot) and `synthesize-start` / `synthesize-chunk` / `synthesize-stop` (streamed text)** for compatibility with future HA streaming-input pipelines.

## Run

```bash
docker run --rm -p 10210:10210 ghcr.io/domdomegg/wyoming-edge-tts
```

Then add a Wyoming Protocol integration in Home Assistant pointing at the host on port 10210.

## Config

All env-var-driven:

| Variable | Default | Description |
|---|---|---|
| `PORT` | `10210` | Listen port. |
| `VOICES` | _(curated 12-voice English set)_ | Space-separated list of Edge voice IDs (e.g. `en-GB-SoniaNeural en-US-AvaNeural`). Full list: see [Microsoft's voice gallery](https://speech.microsoft.com/portal/voicegallery). |
| `DEFAULT_VOICE` | `en-US-AvaNeural` | Voice used when the client doesn't specify one. Must be in `VOICES`. |
| `MAX_CONCURRENT` | `3` | Cap on parallel WebSockets to Microsoft per synthesis request. Higher values speed up multi-sentence synthesis but burn more upstream connections. |

## Caveats

- The free read-aloud endpoint is reverse-engineered, not officially supported. Microsoft has rotated auth tokens before; if MS changes the protocol, `msedge-tts` (the upstream Rust crate) needs to update.
- Only `audio-24khz-48kbitrate-mono-mp3` is accepted by the free endpoint; we decode to PCM with `symphonia` before forwarding. (Raw PCM formats listed in MS's API docs work only on paid Azure endpoints.)
- Sentence splitting uses a simple `.!?` heuristic; abbreviations like `Dr.` mid-sentence may cause extra splits. Edge TTS handles each fragment fine; the audible effect is a slightly longer pause.

## License

MIT
