#!/usr/bin/env python3
"""Who asked: the caller each request is recorded against.

Measured on the box 2026-10-16: requests through tailscale serve reached llama-server from the box's
own address, so 529 of 998 rows had no caller and showed as "LLM Server"; and whenever a direct
caller held a keep-alive open, serve's requests were given to it (6 of henry@'s requests in two
minutes were recorded as victors-macbook-pro). These checks pin both down:

  in process  pick_caller never hands a request to a peer that merely holds a connection, and a
              proxied request names its journal row whichever of the two finishes first
  end to end  the /v1 proxy passes answers through unchanged (one JSON, one stream), reads the
              tokens written off the end, keeps llama-server's key check, and notes the caller
"""
import json, os, signal, subprocess, sys, tempfile, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
fails = []


def check(name, fn):
    try:
        fn(); print('  ok   %s' % name)
    except Exception as e:
        fails.append(name); print('  FAIL %s -> %r' % (name, e))


def eq(got, want):
    assert got == want, 'got %r, want %r' % (got, want)


# ------------------------------------------------------------------ in process
TMP = tempfile.mkdtemp()
sys.argv = ['llm_monitor.py', '--host', '127.0.0.1', '--llm', 'http://127.0.0.1:1',
            '--db', os.path.join(TMP, 'a.db'), '--api-key-file', os.path.join(TMP, 'none')]
sys.path.insert(0, ROOT)
import llm_monitor as M  # noqa: E402  (parses sys.argv at import)

MAC, HENRY = '100.64.0.5', 'henry@efficient-erp.com'


def finish(t, slot, task, gen):
    """The lines llama-server logs when a request ends, as the box's journal has them."""
    pre = 'slot print_timing: id  %d | task %d | ' % (slot, task)
    for line in (pre + 'prompt eval time =     536.04 ms /    21 tokens (   25.53 ms per token,    39.18 tokens per second)',
                 pre + '       eval time =    4835.28 ms / %5d tokens (   24.80 ms per token,    40.33 tokens per second)' % gen,
                 pre + '      total time =    5371.31 ms / %5d tokens' % (21 + gen),
                 'slot      release: id  %d | task %d | stop processing: n_tokens = %d, truncated = 0' % (slot, task, 2000 + gen)):
        M.handle_line(t, line, 'llama-server')


def client_of(task):
    r = M.DB.execute('select client from requests where task=?', (task,)).fetchone()
    return r and r[0]


def reset():
    M.PROXIED['live'].clear(); M.PROXIED['done'].clear(); M.CLAIMED.clear(); M.TASK_CLIENT.clear()


def pick():
    eq(M.pick_caller({MAC: 1}, {}, []), (None, MAC))                         # a direct request, nothing else
    eq(M.pick_caller({MAC: 1}, {}, [(3, HENRY)]), (None, None))              # the measured mix-up: no guess
    eq(M.pick_caller({MAC: 1}, {MAC: 1}, [(3, HENRY)]), (3, HENRY))          # mac's only connection is busy
    eq(M.pick_caller({}, {}, [(3, HENRY)]), (3, HENRY))                      # serve alone
    eq(M.pick_caller({}, {}, [(3, HENRY), (4, 'lewis@efficient-erp.com')]), (None, None))
    eq(M.pick_caller({MAC: 1, '100.64.0.9': 2}, {}, []), (None, None))       # two direct callers
    eq(M.pick_caller({}, {}, []), (None, None))                              # nobody: "unknown", not the server


def proxy_ends_first():
    reset(); t = time.time()
    rid = M.proxy_start(HENRY); M.proxy_end(rid, 200, 40, now=t)
    finish(t + 0.3, 0, 501, 40)
    eq(client_of(501), HENRY)


def journal_ends_first():
    reset(); t = time.time()
    M.TASK_CLIENT[502] = MAC                                                 # a wrong live guess
    rid = M.proxy_start(HENRY)
    finish(t, 1, 502, 41)
    eq(client_of(502), MAC)
    M.proxy_end(rid, 200, 41, now=t + 0.4)
    eq(client_of(502), HENRY)


def direct_row_kept():
    reset(); t = time.time()
    M.TASK_CLIENT[503] = MAC
    rid = M.proxy_start(HENRY); M.proxy_end(rid, 200, 42, now=t)
    finish(t + 0.1, 2, 503, 77)                                              # other token count: not henry's
    eq(client_of(503), MAC)
    finish(t + 0.2, 3, 504, 42)
    eq(client_of(504), HENRY)


def no_caller_stays_empty():
    reset()
    finish(time.time(), 0, 505, 9)
    eq(client_of(505), None)


def ambiguous_left_alone():
    reset(); t = time.time()
    for who in (HENRY, 'lewis@efficient-erp.com'):
        rid = M.proxy_start(who); M.proxy_end(rid, 200, 50, now=t)
    finish(t, 0, 506, 50)                                                    # two could own it
    eq(client_of(506), None)


def tokens_read_off_the_end():
    eq(M.gen_tokens_of(b'{"usage":{"completion_tokens":7},"timings":{"predicted_n":7}}'), 7)
    sse = (b'event: content_block_delta\ndata: {"delta":{"text":"say \\"output_tokens\\": 99"}}\n\n'
           b'event: message_delta\ndata: {"usage":{"output_tokens":11}}\n\nevent: message_stop\ndata: {}\n\n')
    eq(M.gen_tokens_of(sse), 11)
    eq(M.gen_tokens_of(b'{"error":"x"}'), None)
    eq([M.generates(p) for p in ('/v1/chat/completions', '/v1/messages', '/v1/completions', '/v1/responses',
                                  '/v1/messages/count_tokens', '/v1/embeddings')],
       [True, True, True, True, False, False])


# ------------------------------------------------------------------ end to end
def end_to_end():
    # the proxy as tailscale serve meets it: in this process (so PROXIED can be read back), in front
    # of the fake llama-server, called from loopback with the headers serve stamps
    import threading
    key, fake_port = 'attr-test-key', 8199
    fake = subprocess.Popen([sys.executable, os.path.join(HERE, 'fake_llama.py'), str(fake_port)],
                            env=dict(os.environ, FAKE_API_KEY=key))
    M.LLM, M.API_KEY = 'http://127.0.0.1:%d' % fake_port, key
    srv = M.ThreadingHTTPServer(('127.0.0.1', 0), M.H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = 'http://127.0.0.1:%d' % srv.server_address[1]
    for _ in range(40):
        try:
            urllib.request.urlopen('http://127.0.0.1:%d/health' % fake_port, timeout=2); break
        except Exception:
            time.sleep(0.25)

    def post(path, auth, body=b'{}', serve=True):
        h = {'Content-Type': 'application/json'}
        if serve: h.update({'Tailscale-User-Login': HENRY, 'X-Forwarded-For': MAC})
        if auth: h['Authorization'] = auth
        try:
            with urllib.request.urlopen(urllib.request.Request(base + path, data=body, headers=h), timeout=10) as r:
                return r.status, r.headers.get('Content-Type'), r.read()
        except urllib.error.HTTPError as e:
            return e.code, None, b''

    def last_done():
        # the proxy notes a request after the last byte is sent, so a client holding a full
        # Content-Length answer can get here first
        for _ in range(40):
            if M.PROXIED['done']: break
            time.sleep(0.05)
        d = M.PROXIED['done'][-1]
        return d['client'], d['gen'], d['failed']

    try:
        def chat():
            reset()
            st, ct, b = post('/v1/chat/completions', 'Bearer ' + key)
            eq(st, 200); eq(ct, 'application/json'); eq(json.loads(b)['choices'][0]['message']['content'], 'hello')
            eq(last_done(), (HENRY, 7, False)); eq(M.PROXIED['live'], {})

        def stream():
            reset()
            st, ct, b = post('/v1/messages', 'Bearer ' + key)
            eq(st, 200); eq(ct, 'text/event-stream')
            eq(b.count(b'event: '), 4); assert b.endswith(b'data: {"type": "message_stop"}\n\n'), b[-80:]
            eq(last_done(), (HENRY, 11, False))

        def refused(auth):
            reset()
            eq(post('/v1/chat/completions', auth)[0], 401)
            eq(M.PROXIED['done'], [])                                        # never reached a slot

        def unnamed():
            # with the key but not through serve: no caller to name, so it never runs
            reset()
            eq(post('/v1/chat/completions', 'Bearer ' + key, serve=False)[0], 403)
            eq(M.PROXIED['done'], [])

        def device_only():
            # a tagged device (the production backend) has no login, only its address
            reset()
            h = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + key, 'X-Forwarded-For': '100.74.7.82'}
            with urllib.request.urlopen(urllib.request.Request(base + '/v1/chat/completions', data=b'{}', headers=h), timeout=10) as r:
                eq(r.status, 200)
            eq(last_done(), ('100.74.7.82', 7, False))

        def models():
            rq = urllib.request.Request(base + '/v1/models', headers={'Authorization': 'Bearer ' + key})
            eq(json.loads(urllib.request.urlopen(rq, timeout=5).read())['object'], 'list')

        check('/v1 proxy passes a JSON answer through and notes who asked', chat)
        check('/v1 proxy streams an answer through and notes who asked', stream)
        check('/v1 proxy refuses no key', lambda: refused(None))
        check('/v1 proxy refuses a wrong key', lambda: refused('Bearer nope'))
        check('/v1 proxy refuses a keyed call serve did not name', unnamed)
        check('/v1 proxy names a tagged device by its address', device_only)
        check('/v1/models is still answered by the monitor', models)
    finally:
        srv.shutdown(); fake.send_signal(signal.SIGTERM); fake.wait(timeout=10)


def finds_llama_server():
    # llama-server moved to 127.0.0.1 so only serve and this monitor reach it; the monitor follows
    # whatever --host/--port it was started with, and the tailnet address when it is not running
    real = M.read_cmdline
    try:
        M.read_cmdline = lambda: ['/x/llama-server', '--model', 'm.gguf', '--host', '127.0.0.1', '--port', '8080']
        eq(M.find_llm(), 'http://127.0.0.1:8080')
        M.read_cmdline = lambda: ['/x/llama-server', '--host', '100.111.183.77', '--port', '9090']
        eq(M.find_llm(), 'http://100.111.183.77:9090')
        M.read_cmdline = lambda: []
        M.TS_IP = '100.111.183.77'
        eq(M.find_llm(), 'http://100.111.183.77:8080')
    finally:
        M.read_cmdline = real


if __name__ == '__main__':
    check('pick_caller gives a request only to its one possible caller', pick)
    check('proxied request names its row (proxy finished first)', proxy_ends_first)
    check('proxied request names its row (journal finished first)', journal_ends_first)
    check('a direct request keeps its caller beside a proxied one', direct_row_kept)
    check('a request nobody can claim records no caller', no_caller_stays_empty)
    check('two proxied requests that could own a row claim neither', ambiguous_left_alone)
    check('tokens written are read off the end of each answer shape', tokens_read_off_the_end)
    check('the monitor finds llama-server where it was started', finds_llama_server)
    end_to_end()
    if fails:
        print('\n%d check(s) failed' % len(fails)); sys.exit(1)
    print('\nall good')
