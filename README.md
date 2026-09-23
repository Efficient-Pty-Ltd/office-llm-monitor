# Office LLM Monitor

A single page that shows what the office llama-server is doing: who is asking, how fast it
answers, how much of the KV cache is left, and what the two GPUs are up to.

It is deliberately small: **one Python file, standard library only, one HTML file.** No build
step, no dependencies, no framework. Clone it, run it, open the page.

```
┌───────────┐      journald + /slots + /props      ┌──────────────┐
│ llama.cpp │ ───────────────────────────────────► │llm_monitor.py│
│   :8080   │                                      │    :8765     │
└───────────┘                                      └──────┬───────┘
                                                          │  one HTML page
                                                   ┌──────▼───────┐
                                                   │ your browser │
                                                   └──────────────┘
```

## Run it locally

```bash
python3 llm_monitor.py --port 8765 --llm http://localhost:8080
```

| flag | default | what it is |
|---|---|---|
| `--port` | 8765 | port to serve the page on |
| `--host` | this machine's tailscale IPv4 | bind address; use `127.0.0.1` behind a proxy |
| `--llm` | `http://<tailscale ip>:8080` | the llama-server to watch |
| `--unit` | `llm.service` | the systemd unit whose journal is parsed |
| `--page` | `monitor_page.html` | the page to serve |
| `--db` | `monitor.db` | where history is kept (SQLite, created on first run) |

Without both `--host` and `--llm`, the monitor needs a tailscale address and exits if there is
none yet, so systemd retries it at boot instead of watching `127.0.0.1`, where llama-server does
not listen.

Nothing about a conversation is stored: the database holds token counts, timings and the
caller's name, never a prompt or an answer.

## Files

| path | what |
|---|---|
| `llm_monitor.py` | the whole backend: journal parser, SQLite history, HTTP API, page server |
| `monitor_page.html` | the whole frontend, one file, no build |
| `app/` | icons |
| `deploy/` | systemd units and the pull-based updater |

## How a change reaches the server

Push to `main`. The box checks this repo every minute, and when `main` has moved it pulls,
restarts the service, and waits for the health check. There is no deploy key in GitHub and
nothing reaches in from outside — the box pulls. See `deploy/README.md`.

## Deployed from

`main` on this repo, checked out on the box that runs it. Nothing is edited in place
there — a change reaches it by being pushed here.

## Contributing

CI runs on every push and pull request: it compiles the Python, runs the smoke test (which
boots the server against a fake llama-server and checks the endpoints), and checks the page
for the mistakes this project has actually made before — a stray `}` that silently kills the
rest of the stylesheet, a `<script>` that never closes, an icon referenced but missing.

Run the same checks locally before pushing:

```bash
python3 -m compileall -q llm_monitor.py && python3 tests/smoke.py
```
