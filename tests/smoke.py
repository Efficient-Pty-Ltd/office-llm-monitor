#!/usr/bin/env python3
"""Boot the monitor against a fake llama-server and check that it serves what it promises.

This is the test that would have caught every outage this page has had: a syntax error, a
missing icon, an endpoint that returns 500 because a column was renamed.
"""
import json, os, signal, subprocess, sys, tempfile, time, urllib.error, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
FAKE_PORT, MON_PORT = 8099, 8765
fails = []


def check(name, fn):
    try:
        fn(); print('  ok   %s' % name)
    except Exception as e:
        fails.append(name); print('  FAIL %s -> %s' % (name, e))


def get(path, want_json=True):
    with urllib.request.urlopen('http://127.0.0.1:%d%s' % (MON_PORT, path), timeout=20) as r:
        body = r.read()
        assert r.status == 200, 'status %s' % r.status
        return json.loads(body) if want_json else body


def exits_without_tailnet():
    # The box booted with the monitor ahead of tailscale, fell back to 127.0.0.1, and showed the
    # model down for 18 minutes while it was answering. With no tailscale address and no --llm,
    # the monitor must exit so systemd retries, not start watching the wrong address.
    empty = tempfile.mkdtemp()                    # PATH with no tailscale binary on it
    r = subprocess.run([sys.executable, os.path.join(ROOT, 'llm_monitor.py'), '--host', '127.0.0.1',
                        '--port', str(MON_PORT), '--db', os.path.join(empty, 'x.db')],
                       env=dict(os.environ, PATH=empty), capture_output=True, text=True, timeout=20)
    assert r.returncode != 0, 'started without a tailscale address (exit %s)' % r.returncode
    assert 'no tailscale IPv4' in r.stderr, 'unexpected stderr: %r' % r.stderr[-200:]


def main():
    check('exits when tailscale has no address yet', exits_without_tailnet)
    db = tempfile.mktemp(suffix='.db')
    fake = subprocess.Popen([sys.executable, os.path.join(HERE, 'fake_llama.py'), str(FAKE_PORT)])
    mon = subprocess.Popen([sys.executable, os.path.join(ROOT, 'llm_monitor.py'),
                            '--host', '127.0.0.1', '--port', str(MON_PORT),
                            '--llm', 'http://127.0.0.1:%d' % FAKE_PORT,
                            '--db', db, '--unit', 'nonexistent.service'])
    try:
        for _ in range(60):                       # wait for it to come up
            try:
                urllib.request.urlopen('http://127.0.0.1:%d/live' % MON_PORT, timeout=2); break
            except Exception:
                time.sleep(0.5)
        else:
            raise SystemExit('monitor never started')

        check('/ serves the page', lambda: (lambda b: (
            b.lower().count(b'<script') == b.lower().count(b'</script>') or
            (_ for _ in ()).throw(AssertionError('unbalanced <script> tags')),
            b.count(b'{') == b.count(b'}') or
            (_ for _ in ()).throw(AssertionError('unbalanced braces: %d { vs %d }'
                                                 % (b.count(b'{'), b.count(b'}')))),
        ))(get('/', False)))
        check('/live has slots and model', lambda: (lambda d: (
            'slots' in d or (_ for _ in ()).throw(AssertionError('no slots')),
            len(d['slots']) == 4 or (_ for _ in ()).throw(AssertionError('want 4 slots'))))(get('/live')))
        check('/series answers', lambda: get('/series?range=1h'))
        check('/logs answers', lambda: get('/logs'))
        check('/v1/models advertises a model', lambda: (lambda d:
            d['data'][0]['id'] or (_ for _ in ()).throw(AssertionError('no model id')))(get('/v1/models')))
        check('icons referenced by the page exist', check_icons)
        check('/download/requests.csv answers', lambda: get('/download/requests.csv', False))
    finally:
        for p in (mon, fake):
            p.send_signal(signal.SIGTERM); p.wait(timeout=10)
        if os.path.exists(db): os.remove(db)

    if fails:
        print('\n%d check(s) failed' % len(fails)); sys.exit(1)
    print('\nall good')


def check_icons():
    import re
    page = open(os.path.join(ROOT, 'monitor_page.html'), 'rb').read().decode('utf-8', 'replace')
    for ref in set(re.findall(r'["\'](/?app/[\w.\-]+)["\']', page)):
        p = os.path.join(ROOT, ref.lstrip('/'))
        assert os.path.exists(p), 'page references %s but the file is missing' % ref


if __name__ == '__main__':
    main()
