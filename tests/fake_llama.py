#!/usr/bin/env python3
"""A stand-in for llama-server, so the monitor can be tested without a GPU.

It answers the three endpoints the monitor actually reads: /health, /props and /slots, and the two
answer routes the /v1 proxy passes on: /v1/chat/completions (one JSON answer, with usage and
timings) and /v1/messages (streamed, Anthropic events). With FAKE_API_KEY set it refuses all but
/health without that key, as llama-server does with --api-key.
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


CHAT = {'id': 'chatcmpl-test', 'object': 'chat.completion', 'model': 'qwen27b',
        'choices': [{'index': 0, 'message': {'role': 'assistant', 'content': 'hello'}, 'finish_reason': 'stop'}],
        'usage': {'completion_tokens': 7, 'prompt_tokens': 12, 'total_tokens': 19},
        'timings': {'prompt_n': 12, 'prompt_ms': 40.1, 'predicted_n': 7, 'predicted_ms': 90.2}}
MESSAGES = [('message_start', {'type': 'message_start', 'message': {'id': 'msg_test', 'usage': {'input_tokens': 12}}}),
            ('content_block_delta', {'type': 'content_block_delta', 'index': 0,
                                     'delta': {'type': 'text_delta', 'text': 'say "output_tokens": 99'}}),   # a lookalike inside the answer
            ('message_delta', {'type': 'message_delta', 'delta': {'stop_reason': 'end_turn'}, 'usage': {'output_tokens': 11}}),
            ('message_stop', {'type': 'message_stop'})]


class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass

    def refused(self):
        key = os.environ.get('FAKE_API_KEY')
        if key and self.path != '/health' and self.headers.get('Authorization') != 'Bearer ' + key:
            self.send_response(401); self.end_headers(); return True
        return False

    def do_POST(self):
        if self.refused(): return
        self.rfile.read(int(self.headers.get('Content-Length') or 0))
        if self.path == '/v1/chat/completions':
            b = json.dumps(CHAT).encode()
            self.send_response(200); self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(b))); self.end_headers(); self.wfile.write(b); return
        if self.path == '/v1/messages':
            self.send_response(200); self.send_header('Content-Type', 'text/event-stream'); self.end_headers()
            for ev, data in MESSAGES:
                self.wfile.write(('event: %s\ndata: %s\n\n' % (ev, json.dumps(data))).encode()); self.wfile.flush()
            return
        self.send_response(404); self.end_headers()

    def do_GET(self):
        if self.refused(): return
        body = {'/health': {'status': 'ok'}, '/props': PROPS, '/slots': SLOTS}.get(self.path.split('?')[0])
        if body is None:
            self.send_response(404); self.end_headers(); return
        b = json.dumps(body).encode()
        self.send_response(200); self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(b))); self.end_headers(); self.wfile.write(b)


if __name__ == '__main__':
    ThreadingHTTPServer(('127.0.0.1', int(sys.argv[1] if len(sys.argv) > 1 else 8099)), H).serve_forever()
