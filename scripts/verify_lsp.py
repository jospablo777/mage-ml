import asyncio
import json
import sys

from websockets.asyncio.client import connect


async def verify_lsp(url):
    async with asyncio.timeout(30):
        while True:
            try:
                connection = await connect(url, open_timeout=5)
                break
            except OSError:
                await asyncio.sleep(0.2)
        try:
            async def send(method, params, request_id=None):
                message = {'jsonrpc': '2.0', 'method': method, 'params': params}
                if request_id is not None:
                    message['id'] = request_id
                await connection.send(json.dumps(message))

            async def receive(predicate):
                while True:
                    message = json.loads(await connection.recv())
                    if predicate(message):
                        return message

            await send('initialize', {
                'processId': None, 'rootUri': 'file:///tmp', 'capabilities': {},
            }, 1)
            initialized = await receive(lambda message: message.get('id') == 1)
            if 'capabilities' not in initialized.get('result', {}):
                raise RuntimeError(f'LSP initialization failed: {initialized}')

            await send('initialized', {})
            await send('textDocument/didOpen', {'textDocument': {
                'uri': 'file:///tmp/mage_lsp_unsaved.py',
                'languageId': 'python', 'version': 1, 'text': 'value =\n',
            }})
            diagnostics = await receive(
                lambda message: message.get('method') == 'textDocument/publishDiagnostics',
            )
            if not diagnostics['params']['diagnostics']:
                raise RuntimeError('LSP did not report the syntax error in an unsaved file')

            await send('shutdown', None, 2)
            shutdown = await receive(lambda message: message.get('id') == 2)
            if shutdown.get('error'):
                raise RuntimeError(f'LSP shutdown failed: {shutdown}')
            await send('exit', None)
            print('LSP initialization, unsaved-file diagnostics, and shutdown passed')
        finally:
            await connection.close()


if __name__ == '__main__':
    asyncio.run(verify_lsp(sys.argv[1]))
