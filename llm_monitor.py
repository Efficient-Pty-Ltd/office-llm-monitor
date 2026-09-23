#!/usr/bin/env python3
"""Efficient ERP Office LLM monitor backend for the Linux LLM server (llama.cpp under systemd).

Serves the monitor page and its JSON on the tailnet address only.
  live state  : llama-server /slots, /metrics, /health, /props + nvidia-smi + /proc
  request log : the llm.service journal (print_timing / release lines), kept in SQLite
Standard library only.
"""
import argparse, datetime as dt, json, os, re, sqlite3, subprocess, sys, threading, time, urllib.request
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

AP = argparse.ArgumentParser()
AP.add_argument('--port', type=int, default=8765)
AP.add_argument('--host', default='')            # default: this machine's tailscale IPv4
AP.add_argument('--llm', default='')             # default: http://<tailscale ip>:8080
AP.add_argument('--unit', default='llm.service')
AP.add_argument('--page', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'monitor_page.html'))
AP.add_argument('--db', default=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'monitor.db'))
ARGS = AP.parse_args()

def sh(cmd, timeout=10):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout).stdout
    except Exception:
        return ''

TS_IP = (sh(['tailscale', 'ip', '-4']).strip().splitlines() or [''])[0]
if not TS_IP and not (ARGS.host and ARGS.llm):
    # llama-server listens on the tailnet address only, so a 127.0.0.1 fallback watches nothing and
    # reports the model down until someone restarts us. At boot we can start before tailscale has an
    # address; exiting lets systemd's Restart=always retry until it does.
    sys.exit('llm_monitor: no tailscale IPv4 yet; pass --host and --llm to run without tailscale')
HOST = ARGS.host or TS_IP
LLM = ARGS.llm or f'http://{TS_IP}:8080'
LLM_PORT = int(urlparse(LLM).port or 8080)
STARTED = time.time()
LOCK = threading.Lock()

# ---------------------------------------------------------------- storage
DB = sqlite3.connect(ARGS.db, check_same_thread=False)
DB.executescript('''
create table if not exists requests(t_end real, task int, slot int, prompt_tokens int, prompt_ms real, prompt_tps real,
  gen_tokens int, gen_ms real, gen_tps real, total_ms real, draft_acc real, ctx_tokens int, client text);
create index if not exists requests_t on requests(t_end);
create table if not exists failures(t real, task int, client text, error text);
create table if not exists gpu(t real, gpu int, util real, power real, temp real, used real);
create index if not exists gpu_t on gpu(t);
create table if not exists outages(t_from real, t_to real, kind text);
create table if not exists events(t real, text text);
create table if not exists meta(k text primary key, v text);
''')
DB.commit()

def meta_get(k, d=None):
    r = DB.execute('select v from meta where k=?', (k,)).fetchone()
    return r[0] if r else d

def meta_set(k, v):
    DB.execute('insert or replace into meta values(?,?)', (k, str(v)))

def add_event(t, text):
    with LOCK:
        DB.execute('insert into events values(?,?)', (t, text)); DB.commit()

# ---------------------------------------------------------------- helpers
def get_json(path, timeout=3):
    with urllib.request.urlopen(LLM + path, timeout=timeout) as r:
        return json.loads(r.read())

def get_text(path, timeout=3):
    with urllib.request.urlopen(LLM + path, timeout=timeout) as r:
        return r.read().decode()

NAMES = {'t': 0, 'map': {}}
def peer_names():
    if time.time() - NAMES['t'] > 60:
        try:
            d = json.loads(sh(['tailscale', 'status', '--json']))
            m = {}
            for p in list(d.get('Peer', {}).values()) + [d.get('Self', {})]:
                for ip in p.get('TailscaleIPs', []):
                    m[ip] = p.get('HostName') or p.get('DNSName', '').split('.')[0]
            NAMES['map'] = m
        except Exception:
            pass
        NAMES['t'] = time.time()
    return NAMES['map']

def name_of(ip):
    if not ip: return ''
    return peer_names().get(ip, ip)

def established_peers():
    """IPs with an open connection to the model server port, and how many each."""
    out = sh(['ss', '-Htn', 'state', 'established', f'( sport = :{LLM_PORT} )'])
    c = {}
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 4:
            peer = parts[-1].rsplit(':', 1)[0].strip('[]')
            if peer.startswith('::ffff:'): peer = peer[7:]
            if peer in (TS_IP, '127.0.0.1', '::1'): continue     # our own polling of /slots and /metrics
            c[peer] = c.get(peer, 0) + 1
    return c

# ---------------------------------------------------------------- live sampler
STATE = {'slots': {}, 'gpus': [], 'gpu_t': 0, 'sys': {}, 'lm_up': False, 'lm_since': None, 'down_since': None,
         'clients': [], 'queued': 0, 'props': {}, 'models': {}, 'cmd': [], 'loaded_at': None, 'log_t': None}
SLOT_MEMO = {}        # slot id -> {task, since, client, last_dec, last_t, tps}
TASK_CLIENT = {}      # task id -> client ip (kept for the journal parser)
GPU_RING = []         # (t, [util], [power]) every ~2 s, last 10 min
CPU_PREV = [None]

def read_cmdline():
    if not os.path.isdir('/proc'): return {}      # not Linux: no journal, no /proc, still runnable
    for pid in os.listdir('/proc'):
        if not pid.isdigit(): continue
        try:
            a = open(f'/proc/{pid}/cmdline', 'rb').read().split(b'\0')
        except Exception:
            continue
        if a and a[0].endswith(b'llama-server'):
            return [x.decode() for x in a if x]
    return []

def unit_since():
    out = sh(['systemctl', 'show', ARGS.unit, '-p', 'ActiveEnterTimestamp', '-p', 'ActiveState', '--timestamp=unix'])
    m = re.search(r'ActiveEnterTimestamp=@(\d+)', out)
    return float(m.group(1)) if m else None

def sample_sys():
    s = {}
    try:
        f = open('/proc/stat').readline().split()[1:]
        v = list(map(int, f)); idle = v[3] + v[4]; tot = sum(v)
        if CPU_PREV[0]:
            di, dt_ = idle - CPU_PREV[0][0], tot - CPU_PREV[0][1]
            s['cpu'] = round(100 * (1 - di / dt_), 1) if dt_ else 0
        CPU_PREV[0] = (idle, tot)
    except Exception:
        pass
    try:
        mi = {l.split(':')[0]: int(l.split()[1]) * 1024 for l in open('/proc/meminfo')}
        s['ram_total'] = mi['MemTotal']; s['ram_used'] = mi['MemTotal'] - mi['MemAvailable']
    except Exception:
        pass
    st = os.statvfs('/')
    s['disk_total'] = st.f_blocks * st.f_frsize; s['disk_free'] = st.f_bavail * st.f_frsize; s['disk'] = '/'
    s['cores'] = os.cpu_count()
    try:
        s['cpu_name'] = next(l.split(':', 1)[1].strip() for l in open('/proc/cpuinfo') if l.startswith('model name'))
    except Exception:
        s['cpu_name'] = ''
    return s

def sample_gpu():
    out = sh(['nvidia-smi', '--query-gpu=index,utilization.gpu,memory.used,memory.total,power.draw,temperature.gpu',
              '--format=csv,noheader,nounits'], timeout=5)
    g = []
    for line in out.strip().splitlines():
        try:
            i, u, used, tot, p, tmp = [x.strip() for x in line.split(',')]
            g.append({'index': int(i), 'util': float(u), 'used': float(used), 'total': float(tot),
                      'power': round(float(p), 2), 'temp': float(tmp)})
        except Exception:
            pass
    return g

def gpu_loop():
    acc, last_flush = [], time.time()
    while True:
        g = sample_gpu()
        now = time.time()
        if g:
            with LOCK:
                STATE['gpus'] = g; STATE['gpu_t'] = now
                GPU_RING.append((now, [x['util'] for x in g], [x['power'] for x in g]))
                while GPU_RING and GPU_RING[0][0] < now - 600: GPU_RING.pop(0)
            acc.append(g)
        if now - last_flush >= 10 and acc:
            rows = []
            for i in range(len(acc[0])):
                pick = [a[i] for a in acc if len(a) > i]
                avg = lambda k: sum(p[k] for p in pick) / len(pick)
                rows.append((now, i, round(avg('util'), 1), round(avg('power'), 1), round(avg('temp'), 1), round(avg('used'))))
            with LOCK:
                DB.executemany('insert into gpu values(?,?,?,?,?,?)', rows); DB.commit()
            acc, last_flush = [], now
        time.sleep(2)

def live_loop():
    fails = 0
    while True:
        now = time.time()
        # up/down comes from /health, which the HTTP thread answers at once. /slots waits in the task queue and can
        # take seconds when every slot is busy; a slow /slots is not an outage, so keep the last slot state then.
        try:
            ok = get_json('/health', 4).get('status') == 'ok'
        except Exception:
            ok = False
        try:
            slots = get_json('/slots', 8) if ok else None
        except Exception:
            slots = None
        if ok and slots is None:              # server up but /slots too slow this time
            with LOCK:
                STATE['sys'] = sample_sys(); meta_set('heartbeat', now); DB.commit()
            time.sleep(1); continue
        with LOCK:
            if ok:
                fails = 0
                if not STATE['lm_up']:
                    if STATE['down_since']:
                        DB.execute('insert into outages values(?,?,?)', (STATE['down_since'], now, 'server')); DB.commit()
                    STATE['down_since'] = None
                    STATE['lm_up'] = True
                    STATE['lm_since'] = unit_since() or now
                    try:
                        STATE['props'] = get_json('/props'); STATE['models'] = get_json('/v1/models')
                    except Exception:
                        pass
                    STATE['cmd'] = read_cmdline()
                    STATE['loaded_at'] = STATE['lm_since']
            else:
                fails += 1
                if fails >= 3 and STATE['lm_up']:
                    STATE['lm_up'] = False; STATE['down_since'] = now - 3; STATE['lm_since'] = now - 3
                elif fails >= 3 and not STATE['down_since']:
                    STATE['down_since'] = now; STATE['lm_since'] = now
            peers = established_peers() if ok else {}
            out = {}
            busy_ips = {}
            for s in (slots or []):
                k = str(s['id'])
                memo = SLOT_MEMO.get(k)
                if s.get('is_processing'):
                    task = s.get('id_task')
                    nt = s.get('next_token') or [{}]
                    dec = (nt[0] or {}).get('n_decoded', 0) or 0
                    n_p = s.get('n_prompt_tokens') or 0
                    done = (s.get('n_prompt_tokens_processed') or 0) + (s.get('n_prompt_tokens_cache') or 0)
                    if not memo or memo['task'] != task:
                        memo = {'task': task, 'since': now, 'client': None, 'last_dec': dec, 'last_t': now, 'tps': None}
                        SLOT_MEMO[k] = memo
                    if dec > memo['last_dec'] and now > memo['last_t']:
                        inst = (dec - memo['last_dec']) / (now - memo['last_t'])
                        memo['tps'] = round(inst if memo['tps'] is None else 0.6 * memo['tps'] + 0.4 * inst, 1)
                    memo['last_dec'], memo['last_t'] = dec, now
                    reading = dec == 0 and n_p and done < n_p
                    out[k] = {'task': task, 'phase': 'reading prompt' if reading else 'answering',
                              'progress': round(100 * done / n_p, 1) if reading else None, 'since': memo['since'],
                              'read_tokens': n_p, 'gen_tokens': dec, 'gen_tps': memo['tps'], 'client': memo['client']}
                    if memo['client']: busy_ips[memo['client']] = busy_ips.get(memo['client'], 0) + 1
                else:
                    SLOT_MEMO.pop(k, None)
                    out[k] = {'task': None, 'phase': 'idle', 'progress': None, 'since': None, 'read_tokens': 0,
                              'gen_tokens': 0, 'gen_tps': None, 'client': None}
            # give each new request the connected client that has more open connections than known requests
            for k, v in out.items():
                if v['task'] is not None and not v['client']:
                    cand = [ip for ip, n in peers.items() if n > busy_ips.get(ip, 0)] or list(peers)
                    if cand:
                        ip = cand[0]; v['client'] = ip; SLOT_MEMO[k]['client'] = ip
                        busy_ips[ip] = busy_ips.get(ip, 0) + 1
                if v['task'] is not None and v['client']:
                    TASK_CLIENT[v['task']] = v['client']
            if len(TASK_CLIENT) > 2000:
                for t in sorted(TASK_CLIENT)[:1000]: TASK_CLIENT.pop(t, None)
            STATE['slots'] = out
            STATE['clients'] = [{'ip': ip, 'name': name_of(ip), 'conns': n} for ip, n in sorted(peers.items())]
            try:
                m = get_text('/metrics', 2) if ok else ''
                q = re.search(r'^llamacpp:requests_deferred (\S+)', m, re.M)
                STATE['queued'] = int(float(q.group(1))) if q else 0
            except Exception:
                STATE['queued'] = 0
            STATE['sys'] = sample_sys()
            meta_set('heartbeat', now); DB.commit()
        time.sleep(1)

# ---------------------------------------------------------------- journal parser
RX_T = re.compile(r'slot print_timing: id\s+(\d+) \| task (\d+) \| (.*)$')
RX_REL = re.compile(r'slot\s+release: id\s+(\d+) \| task (\d+) \| stop processing: n_tokens = (\d+)')
RX_ERR = re.compile(r'got exception: (\{.*)$')
PENDING = {}

def num(s):
    return float(s.replace(',', ''))

def handle_line(t, msg, ident):
    if ident == 'systemd':
        for key, txt in (('Started ', 'model server started'), ('Stopping ', 'model server stopping'),
                         ('Failed with result', 'model server failed'), ('Scheduled restart', 'systemd restarted the model server')):
            if key in msg:
                add_event(t, txt); break
        return
    STATE['log_t'] = t
    m = RX_T.search(msg)
    if m:
        slot, task, rest = int(m.group(1)), int(m.group(2)), m.group(3)
        p = PENDING.setdefault(task, {'slot': slot})
        a = re.search(r'prompt eval time =\s*([\d.]+) ms /\s*(\d+) tokens.*?([\d.]+) tokens per second', rest)
        if a: p['prompt_ms'], p['prompt_tokens'], p['prompt_tps'] = num(a.group(1)), int(a.group(2)), num(a.group(3))
        a = re.search(r'^\s*eval time =\s*([\d.]+) ms /\s*(\d+) tokens.*?([\d.]+) tokens per second', rest)
        if a: p['gen_ms'], p['gen_tokens'], p['gen_tps'] = num(a.group(1)), int(a.group(2)), num(a.group(3))
        a = re.search(r'total time =\s*([\d.]+) ms', rest)
        if a: p['total_ms'] = num(a.group(1))
        a = re.search(r'draft acceptance = ([\d.]+)', rest)
        if a: p['draft_acc'] = num(a.group(1))
        return
    m = RX_REL.search(msg)
    if m:
        task = int(m.group(2)); p = PENDING.pop(task, {'slot': int(m.group(1))})
        if 'total_ms' not in p: return            # cancelled before any timing: not a finished request
        client = TASK_CLIENT.get(task)
        with LOCK:
            DB.execute('insert into requests values(?,?,?,?,?,?,?,?,?,?,?,?,?)',
                       (t, task, p['slot'], p.get('prompt_tokens', 0), p.get('prompt_ms', 0), p.get('prompt_tps', 0),
                        p.get('gen_tokens', 0), p.get('gen_ms', 0), p.get('gen_tps', 0), p.get('total_ms', 0),
                        p.get('draft_acc'), int(m.group(3)), client))
            DB.commit()
        return
    m = RX_ERR.search(msg)
    if m:
        try:
            e = json.loads(m.group(1)).get('error', {})
            text = (e.get('message') or '').strip()
        except Exception:
            text = m.group(1)[:200]
        why = re.search(r"raise_exception\('([^']+)'", text)
        text = (why.group(1).strip() if why else
                next((l.strip() for l in text.splitlines() if l.strip() and not l.startswith('---')), text))[:300]
        busy = [v for v in STATE['slots'].values() if v.get('client')]
        client = busy[0]['client'] if len(busy) == 1 else (next(iter(established_peers()), None))
        with LOCK:
            DB.execute('insert into failures values(?,?,?,?)', (t, None, client, text)); DB.commit()

def journal_loop():
    # Poll rather than follow: "journalctl -f" skips entries from earlier boots, which lost requests made before a reboot.
    while True:
        cur = meta_get('cursor')
        cmd = ['journalctl', '-o', 'json', '--no-pager', f'_SYSTEMD_UNIT={ARGS.unit}', '+', 'SYSLOG_IDENTIFIER=systemd',
               f'UNIT={ARGS.unit}'] + (['--after-cursor', cur] if cur else [])
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True, errors='replace')
        last = None
        for line in p.stdout:
            try:
                j = json.loads(line)
            except Exception:
                continue
            msg = j.get('MESSAGE')
            if isinstance(msg, list): msg = bytes(msg).decode('utf-8', 'replace')
            t = int(j.get('__REALTIME_TIMESTAMP', 0)) / 1e6
            try:
                handle_line(t, msg or '', j.get('SYSLOG_IDENTIFIER', ''))
            except Exception as e:
                print('journal line skipped:', repr(e), (msg or '')[:120], flush=True)
            last = j.get('__CURSOR') or last
        p.wait()
        if last:
            with LOCK:
                meta_set('cursor', last); DB.commit()
        time.sleep(2)

def boot_gap_check():
    """If the monitor was not running for a while (machine off), record that as an outage."""
    hb = meta_get('heartbeat')
    if hb and time.time() - float(hb) > 120:
        DB.execute('insert into outages values(?,?,?)', (float(hb), time.time(), 'machine')); DB.commit()

# ---------------------------------------------------------------- model info
def model_info():
    props, models, cmd = STATE['props'], STATE['models'], STATE['cmd']
    def opt(name, d=None):
        return cmd[cmd.index(name) + 1] if name in cmd and cmd.index(name) + 1 < len(cmd) else d
    path = props.get('model_path') or opt('--model', '')
    base = os.path.basename(path).replace('.gguf', '')
    meta = ((models.get('data') or [{}])[0] or {}).get('meta', {})
    n_par = meta.get('n_params') or 0
    ctx = int(opt('--ctx-size', meta.get('n_ctx_train') or 0) or 0)
    par = props.get('total_slots') or int(opt('--parallel', 1))
    return {'identifier': props.get('model_alias') or opt('--alias', ''), 'displayName': re.sub(r'[-_]+', ' ', base),
            'modelKey': base, 'path': path, 'paramsString': f'{round(n_par / 1e9)}B' if n_par else '',
            'architecture': '', 'sizeBytes': meta.get('size') or 0, 'contextLength': ctx, 'parallel': par,
            'vision': bool((props.get('modalities') or {}).get('vision')), 'trainedForToolUse': '--jinja' in cmd,
            'format': 'gguf', 'build': props.get('build_info', ''), 'settings': {'contextLength': ctx, 'offloadRatio': 1 if opt('--n-gpu-layers') in ('all', '999', '-1') else None,
            'kCacheQuantizationType': opt('--cache-type-k', 'f16'), 'vCacheQuantizationType': opt('--cache-type-v', 'f16'),
            'numParallelSessions': par, 'temperature': float(opt('--temp', 0.8))},
            'draft': {'name': os.path.basename(opt('--spec-draft-model', '') or opt('--model-draft', '') or '').replace('.gguf', ''),
                      'kind': 'MTP' if 'mtp' in (opt('--spec-type', '') or '').lower() else 'draft',
                      'n_max': int(opt('--spec-draft-n-max', opt('--draft-max', 3)) or 3)}}, ctx, par, '--kv-unified' in cmd

ASK_CHATS = {}          # session id -> {'t': last used, 'msgs': [...]}  a short memory so follow-ups work
ASK_KEEP = 16           # turns kept per session
ASK_TTL = 45 * 60       # a session is forgotten after 45 quiet minutes
ASK_SYS = ('You are talking out loud. Answer in one to three short spoken sentences. '
           'No lists, no markdown, no headings.\n'
           'ALWAYS reply in the same language the person just wrote in. Chinese in, Chinese out.\n'
           'If the person is asking to CREATE something on their phone rather than to chat, do not answer in words. '
           'Reply with exactly one line and nothing else:\n'
           '#REMIND <what to remember>   (a reminder or a task, e.g. buy milk)\n'
           '#TIMER <minutes>             (a countdown)\n'
           '#NOTE <text>                 (something to write down)\n'
           'Keep the text of those lines in the person\'s own words and language.\n'
           'Only use these for a clear request to create one. Everything else is a normal spoken answer.')

ASK_ALLOW = {w.strip() for w in os.environ.get('ASK_ALLOW', 'lewis@efficient-erp.com').split(',') if w.strip()}

def ask_ok(who):
    """Only these tailnet logins may use /ask, so nobody else can start a chat on this box."""
    return not ASK_ALLOW or who in ASK_ALLOW

def ask_model(q, sid='default', who=''):
    """Plain text in, plain text out, with a short memory per session — for a phone shortcut."""
    now = time.time()
    for k, v in list(ASK_CHATS.items()):
        if now - v['t'] > ASK_TTL: ASK_CHATS.pop(k, None)
    if q.strip().lower() in ('new', 'reset', 'clear', '新话题', '重新开始', '清空'):
        ASK_CHATS.pop((who or '?') + '|' + sid, None); return 'Fresh start. What do you want to talk about?'
    sid = (who or '?') + '|' + sid            # one caller's session name never meets another's
    chat = ASK_CHATS.setdefault(sid, {'t': now, 'msgs': []})
    chat['t'] = now
    chat['msgs'].append({'role': 'user', 'content': q})
    chat['msgs'] = chat['msgs'][-ASK_KEEP * 2:]
    body = json.dumps({'model': (model_info()[0]['identifier'] or 'model'), 'reasoning_effort': 'low',
                       'max_tokens': 400, 'temperature': 0.7,
                       'messages': [{'role': 'system', 'content': ASK_SYS}] + chat['msgs']}).encode()
    try:
        rq = urllib.request.Request(LLM + '/v1/chat/completions', data=body,
                                    headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(rq, timeout=120) as r:
            d = json.loads(r.read().decode())
        txt = (((d.get('choices') or [{}])[0].get('message') or {}).get('content') or '').strip()
    except Exception as e:
        chat['msgs'].pop()
        return 'The model did not answer: %s' % e
    chat['msgs'].append({'role': 'assistant', 'content': txt})
    return txt

def live():
    with LOCK:
        now = time.time()
        mi, ctx, par, kvu = model_info()
        return {'now': now, 'gpus': STATE['gpus'], 'gpu_age': now - STATE['gpu_t'] if STATE['gpu_t'] else None,
                'slots': STATE['slots'], 'model': mi['identifier'], 'context': ctx, 'lm_up': STATE['lm_up'],
                'lm_since': STATE['lm_since'] or now, 'log_file': 'journal', 'clients': STATE['clients'],
                'queued': STATE['queued'], 'waiting': [], 'can_act': False, 'action': None, 'sys': STATE['sys'],
                'lm_status': 'idle', 'parallel': par, 'app_last': None, 'model_info': mi if STATE['lm_up'] else {},
                'loaded_at': STATE['loaded_at'], 'n_ctx_slot': ctx, 'kv_unified': kvu,
                'log_age': now - STATE['log_t'] if STATE['log_t'] else None, 'started': STARTED, 'port': LLM_PORT}

# ---------------------------------------------------------------- series
def span_of(rng, now):
    if rng == 'all':
        r = DB.execute('select min(t_end) from requests').fetchone()[0]
        return max(3600, now - (r or now))
    return {'5m': 300, '1h': 3600, '24h': 86400, '7d': 604800}.get(rng, 3600)

def agg(a, b):
    r = DB.execute('select count(*), coalesce(sum(prompt_tokens),0), coalesce(sum(gen_tokens),0), coalesce(sum(gen_ms),0) '
                   'from requests where t_end>=? and t_end<?', (a, b)).fetchone()
    return {'requests': r[0], 'prompt': r[1], 'answer': r[2], 'answer_tps': round(r[2] / (r[3] / 1000), 1) if r[3] else 0}

def local_midnight(now):
    d = dt.datetime.fromtimestamp(now).replace(hour=0, minute=0, second=0, microsecond=0)
    return d.timestamp()

def series(rng):
    with LOCK:
        now = time.time(); span = span_of(rng, now); a = now - span
        # gpu
        gb = 2 if span <= 300 else max(10, int(span / 360))
        gpu = []
        if span <= 600:
            buckets = {}
            for t, u, p in GPU_RING:
                if t >= a: buckets.setdefault(int(t // gb * gb), []).append((u, p))
            for t in sorted(buckets):
                v = buckets[t]; n = len(v[0][0])
                gpu.append({'t': t, 'util': [round(sum(x[0][i] for x in v) / len(v), 1) for i in range(n)],
                            'power': [round(sum(x[1][i] for x in v) / len(v)) for i in range(n)]})
        else:
            rows = DB.execute('select cast(t/? as int)*?, gpu, avg(util), avg(power) from gpu where t>=? group by 1,2 order by 1,2',
                              (gb, gb, a)).fetchall()
            cur = {}
            for t, g, u, p in rows:
                cur.setdefault(t, {})[g] = (u, p)
            for t in sorted(cur):
                n = max(cur[t]) + 1
                gpu.append({'t': t, 'util': [round(cur[t].get(i, (0, 0))[0], 1) for i in range(n)],
                            'power': [round(cur[t].get(i, (0, 0))[1]) for i in range(n)]})
        # request buckets
        rb = {300: 10, 3600: 120, 86400: 3600, 604800: 21600}.get(span, max(120, int(span / 30)))
        pts = []
        for t, n, pr, an, gms, pms in DB.execute(
                'select cast(t_end/? as int)*?, count(*), sum(prompt_tokens), sum(gen_tokens), sum(gen_ms), sum(prompt_ms) '
                'from requests where t_end>=? group by 1 order by 1', (rb, rb, a)):
            pts.append({'t': t, 'n': n, 'prompt': pr, 'answer': an, 'answer_tps': round(an / (gms / 1000), 1) if gms else 0,
                        'read_tps': round(pr / (pms / 1000)) if pms else 0})
        first = DB.execute('select min(t) from (select min(t_end) t from requests union select min(t) from gpu)').fetchone()[0]
        stats = {'today': agg(local_midnight(now), now + 1), 'week': agg(now - 604800, now + 1), 'range': agg(a, now + 1),
                 'before': agg(a - span, a), 'before_complete': bool(first and first <= a - span),
                 'failures': [{'t': t, 'task': task, 'client': c, 'name': name_of(c), 'error': e} for t, task, c, e in
                              DB.execute('select * from failures where t>=? order by t', (a,))],
                 'outages': [{'from': f, 'to': t} for f, t in DB.execute(
                     'select t_from, t_to from outages where t_to>=? order by t_from', (a,))],
                 'heals': [dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime('%Y-%m-%d %H:%M:%SZ') + '  ' + x for t, x in
                           DB.execute('select * from events where t>=? order by t', (now - 604800,))]}
        if STATE['down_since']:
            stats['outages'].append({'from': STATE['down_since'], 'to': now})
        cols = ['t_end', 'task', 'slot', 'prompt_tokens', 'prompt_ms', 'prompt_tps', 'gen_tokens', 'gen_ms', 'gen_tps',
                'total_ms', 'draft_acc', 'ctx_tokens', 'client']
        table = [dict(zip(cols, r)) for r in DB.execute(
            'select * from requests where t_end>=? order by t_end desc limit 1000', (a,))]
        bc = [{'ip': c, 'name': name_of(c) or 'unknown', 'requests': n, 'prompt': p, 'answer': an} for c, n, p, an in DB.execute(
            'select client, count(*), sum(prompt_tokens), sum(gen_tokens) from requests where t_end>=? group by client '
            'order by sum(prompt_tokens)+sum(gen_tokens) desc', (a,))]
        return {'now': now, 'gpu': gpu, 'gpu_bucket': gb, 'requests': {'bucket': rb, 'points': pts}, 'stats': stats,
                'table': table, 'by_client': bc, 'span': span}

def outage(f, to):
    with LOCK:
        gpu = [{'t': t, 'gpu': g, 'util': round(u), 'power': round(p), 'temp': round(tp), 'vram_gb': round(used / 1024, 1)}
               for t, g, u, p, tp, used in DB.execute('select * from gpu where t>=? and t<=? order by t', (f - 120, f + 5))]
        kind = DB.execute('select kind from outages where abs(t_from-?)<5', (f,)).fetchone()
    boots = []
    try:
        boots = json.loads(sh(['journalctl', '--list-boots', '-o', 'json']) or '[]')
    except Exception:
        pass
    booted = [b for b in boots if f - 60 <= b.get('first_entry', 0) / 1e6 <= to + 600]
    if booted or (kind and kind[0] == 'machine'):
        cause = 'The whole machine was off or restarted (power loss, crash or reboot). The monitor could not run then.'
    else:
        cause = 'The model server stopped answering while the machine stayed on. See the log lines below.'
    fmt = lambda t: dt.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
    ev = []
    out = sh(['journalctl', '--no-pager', '-o', 'json', '--since', '@%d' % (f - 300), '--until', '@%d' % (to + 120),
              '-p', 'warning'], timeout=15)
    for line in out.splitlines()[-40:]:
        try:
            j = json.loads(line); m = j.get('MESSAGE')
            if isinstance(m, list): m = bytes(m).decode('utf-8', 'replace')
            ev.append({'time': fmt(int(j['__REALTIME_TIMESTAMP']) / 1e6), 'id': j.get('SYSLOG_IDENTIFIER', ''), 'text': (m or '')[:200]})
        except Exception:
            pass
    log = sh(['journalctl', '--no-pager', '-u', ARGS.unit, '-o', 'short-iso', '--until', '@%d' % (f + 5), '-n', '25'], timeout=15)
    return {'cause': cause, 'gpu': gpu, 'events': ev[-15:], 'watchdog': [], 'log': log.strip().splitlines()}

def log_days():
    out = []
    first = DB.execute('select min(t) from events').fetchone()[0]
    boots = json.loads(sh(['journalctl', '--list-boots', '-o', 'json']) or '[]')
    start = min([b.get('first_entry', 0) / 1e6 for b in boots] + ([first] if first else []) or [time.time()])
    d = dt.date.fromtimestamp(time.time())
    while dt.datetime.combine(d, dt.time()).timestamp() >= start - 86400 and len(out) < 30:
        a = dt.datetime.combine(d, dt.time()); b = a + dt.timedelta(days=1)
        n = sh(['journalctl', '-u', ARGS.unit, '--no-pager', '-o', 'short-iso', '--since', a.strftime('%Y-%m-%d %H:%M:%S'),
                '--until', b.strftime('%Y-%m-%d %H:%M:%S')], timeout=20)
        if n.strip() and not n.startswith('-- No entries'):
            out.append({'name': d.isoformat() + '.log', 'size': len(n.encode())})
        d -= dt.timedelta(days=1)
    return out

# ---------------------------------------------------------------- http
class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def send(self, code, body, ctype='application/json', extra=None):
        if isinstance(body, (dict, list)): body = json.dumps(body)
        if isinstance(body, str): body = body.encode()
        self.send_response(code)
        self.send_header('Content-Type', ctype); self.send_header('Content-Length', str(len(body)))
        if 'Cache-Control' not in (extra or {}): self.send_header('Cache-Control', 'no-store')
        for k, v in (extra or {}).items(): self.send_header(k, v)
        self.end_headers(); self.wfile.write(body)

    def do_GET(self):
        u = urlparse(self.path); q = {k: v[0] for k, v in parse_qs(u.query).items()}
        try:
            if u.path in ('/', '/index.html'):
                return self.send(200, open(ARGS.page, 'rb').read(), 'text/html; charset=utf-8')
            # /test is the same page from a second file: try a change there before it goes live.
            # It reads the same /live and /series, so it shows real numbers.
            if u.path in ('/test', '/test/'):
                f = os.path.join(os.path.dirname(os.path.abspath(ARGS.page)), 'monitor_page_test.html')
                if os.path.isfile(f):
                    return self.send(200, open(f, 'rb').read(), 'text/html; charset=utf-8')
                return self.send(404, {'error': 'no monitor_page_test.html yet'})
            # the app icons and the web manifest, from the app/ folder next to this file
            if re.fullmatch(r'/[a-z0-9-]+\.(png|svg|ico|webmanifest)', u.path):
                f = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app', u.path[1:])
                if os.path.isfile(f):
                    ct = {'png': 'image/png', 'svg': 'image/svg+xml', 'ico': 'image/x-icon', 'webmanifest': 'application/manifest+json'}[u.path.rsplit('.', 1)[1]]
                    return self.send(200, open(f, 'rb').read(), ct, {'Cache-Control': 'max-age=3600'})
            # /v1/models in the shape Claude Code's model discovery wants (Anthropic: type + display_name),
            # with the OpenAI fields kept as well, so every client sees the model. Tailscale routes this
            # path here; everything else under /v1 goes straight to llama-server.
            if u.path in ('/v1/models', '/v1/models/'):
                mi = model_info()[0]; ident = mi['identifier'] or 'model'
                made = int(STATE.get('loaded_at') or time.time())
                iso = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime(made))
                # llama-server ignores the model name and always answers with the model it has loaded, so we can
                # also advertise a claude-* id: Claude Code wants a gateway route that names an Anthropic model.
                def row(i, name):
                    return {'type': 'model', 'id': i, 'display_name': name, 'created_at': iso,
                            'object': 'model', 'created': made, 'owned_by': 'llamacpp'}
                rows = [row('claude-' + ident, (mi['displayName'] or ident) + ' (office)'), row(ident, mi['displayName'] or ident)]
                return self.send(200, {'data': rows, 'object': 'list', 'has_more': False,
                                       'first_id': rows[0]['id'], 'last_id': rows[-1]['id']})
            # /ask?q=... : plain text in, plain text out. It exists so an iPhone shortcut can be
            # three steps (dictate -> fetch -> speak) with no JSON to unpack.
            if u.path == '/ask':
                sid = (q.get('s') or 'default')[:40]
                q = (q.get('q') or '').strip()
                if not q: return self.send(400, 'ask me something', 'text/plain; charset=utf-8')
                if not ask_ok(self.caller()): return self.send(403, 'not for you', 'text/plain; charset=utf-8')
                return self.send(200, ask_model(q, sid, self.caller()), 'text/plain; charset=utf-8')
            if u.path == '/live': return self.send(200, live())
            if u.path == '/series': return self.send(200, series(q.get('range', '1h')))
            if u.path == '/logs': return self.send(200, log_days())
            if u.path == '/outage': return self.send(200, outage(float(q['from']), float(q['to'])))
            if u.path == '/download/requests.csv':
                s = series(q.get('range', '24h'))
                cols = ['t_end', 'task', 'slot', 'prompt_tokens', 'prompt_ms', 'gen_tokens', 'gen_ms', 'gen_tps', 'total_ms',
                        'draft_acc', 'ctx_tokens', 'client']
                rows = [','.join(['time'] + cols[1:] + ['client_name'])]
                for r in reversed(s['table']):
                    rows.append(','.join([dt.datetime.fromtimestamp(r['t_end']).isoformat(timespec='seconds')] +
                                         [str(r[c] if r[c] is not None else '') for c in cols[1:]] + ['"' + name_of(r['client']) + '"']))
                return self.send(200, '\n'.join(rows) + '\n', 'text/csv',
                                 {'Content-Disposition': f'attachment; filename="requests-{q.get("range", "24h")}.csv"'})
            if u.path == '/download/server-log':
                name = q.get('name', '')
                if not re.fullmatch(r'\d{4}-\d\d-\d\d\.log', name): return self.send(400, {'error': 'bad name'})
                a = dt.datetime.fromisoformat(name[:10]); b = a + dt.timedelta(days=1)
                txt = sh(['journalctl', '-u', ARGS.unit, '--no-pager', '-o', 'short-iso', '--since', a.strftime('%Y-%m-%d %H:%M:%S'),
                          '--until', b.strftime('%Y-%m-%d %H:%M:%S')], timeout=60)
                return self.send(200, txt, 'text/plain; charset=utf-8', {'Content-Disposition': f'attachment; filename="{name}"'})
            return self.send(404, {'error': 'not found'})
        except Exception as e:
            return self.send(500, {'error': str(e)})

    def caller(self):
        """Who is asking: the tailnet login when the call came through tailscale serve, else the peer address."""
        return (self.headers.get('Tailscale-User-Login') or self.headers.get('X-Forwarded-For')
                or self.client_address[0] or '?').split(',')[0].strip()

    def do_POST(self):
        if urlparse(self.path).path == '/ask':
            n = int(self.headers.get('Content-Length') or 0)
            raw = self.rfile.read(n).decode('utf-8', 'replace').strip() if n else ''
            if raw.startswith('{'):
                try: raw = (json.loads(raw).get('q') or '').strip()
                except Exception: pass
            if not raw: return self.send(400, 'ask me something', 'text/plain; charset=utf-8')
            sid = ({k: v[0] for k, v in parse_qs(urlparse(self.path).query).items()}.get('s') or 'default')[:40]
            if not ask_ok(self.caller()): return self.send(403, 'not for you', 'text/plain; charset=utf-8')
            return self.send(200, ask_model(raw, sid, self.caller()), 'text/plain; charset=utf-8')
        return self.send(403, {'error': 'actions are not available on this server'})

if __name__ == '__main__':
    boot_gap_check()
    for fn in (live_loop, gpu_loop, journal_loop):
        threading.Thread(target=fn, daemon=True).start()
    print(f'Efficient ERP Office LLM on http://{HOST}:{ARGS.port}  (model server {LLM})', flush=True)
    ThreadingHTTPServer((HOST, ARGS.port), H).serve_forever()
