"""A local stand-in for IBM Cloud IAM and a COS endpoint that records what clients send."""
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse


class FakeIBMCloud:
    def __init__(self, token_lifetime=3600):
        self.token_lifetime = token_lifetime
        self.token_requests = []
        self.requests = []
        self.objects = {}
        self.tokens_issued = 0
        cloud = self

        class Handler(BaseHTTPRequestHandler):
            # HTTP/1.1 answers boto3's Expect: 100-continue at once.
            protocol_version = 'HTTP/1.1'

            def log_message(self, *args):
                pass

            def _body(self):
                length = int(self.headers.get('Content-Length') or 0)
                return self.rfile.read(length) if length else b''

            def _reply(self, status, body=b'', content_type='application/xml'):
                self.send_response(status)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self):
                body = self._body()
                if self.path == '/identity/token':
                    form = {k: v[0] for k, v in parse_qs(body.decode()).items()}
                    cloud.token_requests.append(form)
                    if form.get('apikey') == 'bad-key':
                        return self._reply(400, json.dumps(dict(
                            errorMessage='Provided API key could not be found.',
                        )).encode(), 'application/json')
                    cloud.tokens_issued += 1
                    return self._reply(200, json.dumps(dict(
                        access_token=f'token-{cloud.tokens_issued}',
                        expires_in=cloud.token_lifetime,
                    )).encode(), 'application/json')
                self._reply(404)

            def do_PUT(self):
                body = self._body()
                cloud.requests.append(('PUT', self.path, dict(self.headers), body))
                if not self._authorized():
                    return
                cloud.objects[urlparse(self.path).path] = body
                self._reply(200)

            def do_GET(self):
                cloud.requests.append(('GET', self.path, dict(self.headers), b''))
                if not self._authorized():
                    return
                url = urlparse(self.path)
                query = {k: v[0] for k, v in parse_qs(url.query).items()}
                if query.get('list-type') == '2':
                    return self._reply(200, self._list(url.path.strip('/'), query))
                if url.path in cloud.objects:
                    return self._reply(200, cloud.objects[url.path], 'application/octet-stream')
                self._reply(404, b'<Error><Code>NoSuchKey</Code></Error>')

            def do_HEAD(self):
                cloud.requests.append(('HEAD', self.path, dict(self.headers), b''))
                if not self._authorized():
                    return
                body = cloud.objects.get(urlparse(self.path).path)
                self.send_response(200 if body is not None else 404)
                self.send_header('Content-Length', str(len(body or b'')))
                self.end_headers()

            def _list(self, bucket, query):
                from xml.sax.saxutils import escape

                prefix = query.get('prefix', '')
                delimiter = query.get('delimiter')
                keys, prefixes = [], set()
                for path, body in sorted(cloud.objects.items()):
                    owner, _, key = path.lstrip('/').partition('/')
                    if owner != bucket or not key.startswith(prefix):
                        continue
                    rest = key[len(prefix):]
                    if delimiter and delimiter in rest:
                        prefixes.add(prefix + rest.split(delimiter)[0] + delimiter)
                    else:
                        keys.append((key, len(body)))
                contents = ''.join(
                    f'<Contents><Key>{escape(k)}</Key><Size>{n}</Size>'
                    '<LastModified>2026-01-01T00:00:00.000Z</LastModified></Contents>'
                    for k, n in keys
                )
                common = ''.join(
                    f'<CommonPrefixes><Prefix>{escape(p)}</Prefix></CommonPrefixes>'
                    for p in sorted(prefixes)
                )
                return (
                    '<?xml version="1.0" encoding="UTF-8"?>'
                    '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                    f'<Name>{bucket}</Name><Prefix>{escape(prefix)}</Prefix>'
                    f'<KeyCount>{len(keys)}</KeyCount><IsTruncated>false</IsTruncated>'
                    f'{contents}{common}</ListBucketResult>'
                ).encode()

            def _authorized(self):
                auth = self.headers.get('Authorization') or ''
                if auth.startswith('Bearer token-') or auth.startswith('AWS4-HMAC-SHA256'):
                    return True
                self._reply(403, b'<Error><Code>AccessDenied</Code></Error>')
                return False

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.url = f'http://127.0.0.1:{self.server.server_port}'
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()
