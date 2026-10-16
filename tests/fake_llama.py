#!/usr/bin/env python3
"""A stand-in for llama-server, so the monitor can be tested without a GPU.

It answers the three endpoints the monitor actually reads: /health, /props and /slots. With
FAKE_API_KEY set it refuses all but /health without that key, as llama-server does with --api-key.
"""
import json, os, sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PROPS = {'default_generation_settings': {'n_ctx': 131072, 'params': {'n_predict': -1}},
         'total_slots': 4, 'model_path': '/models/Qwen3.8-27B-Test-Q6_K.gguf',
         'chat_template': '', 'build_info': 'test'}
# shaped like the real /slots: next_token is a LIST, and the prompt counters are the ones
# the monitor reads to tell "reading" from "answering"
SLOTS = [{'id': i, 'id_task': (100 + i) if i < 2 else -1, 'is_processing': i < 2,
          'prompt': '', 'next_token': [{'n_decoded': 40 * (i + 1), 'has_next_token': i < 2}],
          'n_ctx': 131072, 'params': {'n_predict': -1},
          'n_prompt_tokens': 1200 * (i + 1), 'n_prompt_tokens_processed': 1200 * (i + 1),
          'n_prompt_tokens_cache': 0} for i in range(4)]


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def do_GET(self):
        key = os.environ.get('FAKE_API_KEY')
        if key and self.path != '/health' and self.headers.get('Authorization') != 'Bearer ' + key:
            self.send_response(401); self.end_headers(); return
        body = {'/health': {'status': 'ok'}, '/props': PROPS, '/slots': SLOTS}.get(self.path.split('?')[0])
        if body is None:
            self.send_response(404); self.end_headers(); return
        b = json.dumps(body).encode()
        self.send_response(200); self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(b))); self.end_headers(); self.wfile.write(b)


if __name__ == '__main__':
    ThreadingHTTPServer(('127.0.0.1', int(sys.argv[1] if len(sys.argv) > 1 else 8099)), H).serve_forever()
