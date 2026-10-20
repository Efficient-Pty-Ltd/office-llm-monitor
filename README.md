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
| `--host` | this machine's tailscale IPv4 | bind address; repeat to listen on several (the first is primary; extra ones retry until their address exists) |
| `--llm` | `http://<tailscale ip>:8080` | the llama-server to watch |
| `--unit` | `llm.service` | the systemd unit whose journal is parsed |
| `--page` | `monitor_page.html` | the page to serve |
| `--db` | `monitor.db` | where history is kept (SQLite, created on first run) |

Without both `--host` and `--llm`, the monitor needs a tailscale address and exits if there is
none yet, so systemd retries it at boot instead of watching `127.0.0.1`, where llama-server does
not listen.

Nothing about a conversation is stored: the database holds token counts, timings and the
caller's name, never a prompt or an answer.

## Who asked

Every call to the model comes through tailscale serve (`https://<host>/v1`) and this monitor:
llama-server listens on 127.0.0.1 only (efficient-ops `llm_bind`), and serve's `/v1` route points
here (`ansible/inventory/group_vars/llm.yml`, Tailscale Serve). The monitor passes each call on to
llama-server unchanged, reads who made it from the headers serve stamps (the person's login, or a
tagged device's address), and names the request's row by when it ended and how many tokens it wrote.
A call serve did not name, such as one from the LAN, is refused with 403. The monitor finds
llama-server from its own `--host`/`--port`, so it follows a move. A row can still read "unknown"
when two named requests end together with the same token count, or for a llama-server still on
its tailnet address, where a direct caller is named from its open connection only when nobody else
could have sent the request.

## What the console gets

Since 2026-10-21 Server Status shows this box's model server on the LLM server's **LLM** tab, and
this page is on its way out. The box's metrics agent (efficient-backend `ops/ci-runner-metrics`,
1.9.0) reads llama-server itself and asks this monitor for every call and model-server event since
the last ones it sent:

```
GET http://127.0.0.1:8765/agent/log?after_request=<id>&after_event=<id>&limit=500
```

It answers a process on this box only (loopback, with no header tailscale serve stamps). A call is
a row whether it was answered, failed, or refused by the /v1 proxy for having no caller (`status`),
and a row is held back until its caller can no longer change. Nothing about a conversation is in it.

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
python3 -m compileall -q llm_monitor.py && python3 tests/smoke.py && python3 tests/attribution.py
```
