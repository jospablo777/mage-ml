import asyncio
import codecs
import json
import os
import re
import struct
import sys
from collections import deque

import terminado
from tornado.websocket import WebSocketClosedError

from mage_ai.api.utils import authenticate_client_and_token, has_at_least_editor_role
from mage_ai.data_preparation.models.errors import FileNotInProjectError
from mage_ai.data_preparation.models.file import ensure_file_is_in_project
from mage_ai.data_preparation.repo_manager import get_project_uuid
from mage_ai.orchestration.constants import Entity
from mage_ai.orchestration.db.models.oauth import Oauth2Application
from mage_ai.settings import (
    DISABLE_TERMINAL,
    REQUIRE_USER_AUTHENTICATION,
    is_disable_pipeline_edit_access,
)
from mage_ai.shared.array import find_index


class AsyncSpawnMixin:
    """
    Starts a terminal's shell in a thread: forking the server took about a second, all of
    it on the event loop, while every other request waited. Reading its output is set up
    on the loop, as terminado needs.
    """

    async def spawn(self, **kwargs):
        term = await asyncio.to_thread(self.new_terminal, **kwargs)
        return term


# Run by a new interpreter on the pty: it makes the pty its controlling terminal (job
# control, Ctrl-C) and becomes the shell.
_SHELL_START = '''
import fcntl, os, sys, termios
try:
    if sys.argv[1]:
        os.chdir(sys.argv[1])
    fcntl.ioctl(0, termios.TIOCSCTTY, 0)
    os.execvpe(sys.argv[2], sys.argv[2:], os.environ)
except OSError as error:
    print(f'The terminal could not start {sys.argv[2]}: {error}', file=sys.stderr)
    sys.exit(127)
'''


def spawn_pty(argv, env=None, cwd=None, dimensions=(24, 80)):
    """
    Starts a shell on a new pty without forking the server. ptyprocess forks it and runs
    Python in the copy, which on Python 3.14 clears the other threads' objects there: on
    macOS, finalizing a thread's SQLite statements in the copy crashed it (I-555). Here
    the server opens the pty and posix_spawn starts a new interpreter on it, which runs
    no code of the server's and becomes the shell.
    """
    import fcntl
    import termios

    from ptyprocess import PtyProcessUnicode

    master, replica = os.openpty()
    try:
        rows, cols = dimensions
        fcntl.ioctl(replica, termios.TIOCSWINSZ, struct.pack('HHHH', rows, cols, 0, 0))
        pid = os.posix_spawn(
            sys.executable,
            [sys.executable, '-I', '-S', '-c', _SHELL_START, cwd or '', *argv],
            dict(env or os.environ),
            file_actions=[(os.POSIX_SPAWN_DUP2, replica, fd) for fd in (0, 1, 2)],
            setsid=True,
        )
    except BaseException:
        os.close(master)
        raise
    finally:
        os.close(replica)
    process = PtyProcessUnicode(pid, master)
    process.argv = argv
    process.env = env
    process.launch_dir = cwd
    return process


class SpawnedPty(terminado.management.PtyWithClients):
    """terminado's terminal, with its shell started by spawn_pty."""

    def __init__(self, argv, env=None, cwd=None):
        self.clients = []
        self.read_buffer = deque([], maxlen=1000)
        self.ptyproc = spawn_pty(argv, env, cwd)
        # The output might not be strict UTF-8, as terminado allows.
        self.ptyproc.decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')


class SpawnedShellMixin:
    """Terminals whose shells start with spawn_pty."""

    def new_terminal(self, **kwargs):
        if not hasattr(os, 'posix_spawn'):
            # Windows: terminado starts the shell with pywinpty, which does not fork.
            return super().new_terminal(**kwargs)
        options = self.term_settings.copy()
        options['shell_command'] = self.shell_command
        options.update(kwargs)
        return SpawnedPty(
            options['shell_command'], self.make_term_env(**options), options.get('cwd'),
        )


def _alive(term) -> bool:
    """Whether a terminal's shell still runs and its pty is open."""
    process = getattr(term, 'ptyproc', None)
    if process is None:
        return True
    try:
        return not process.closed and process.isalive()
    except Exception:
        return False


class MageTermManager(SpawnedShellMixin, AsyncSpawnMixin, terminado.NamedTermManager):
    def _live(self, term_name: str):
        """The named terminal while its shell runs; a dead one is dropped, so the next
        connection gets a new shell instead of a pty that refuses every write."""
        term = self.terminals.get(term_name)
        if term is not None and not _alive(term):
            self.log.info('Terminal %s exited; starting a new one.', term_name)
            self.terminals.pop(term_name, None)
            return None
        return term

    async def get_terminal_async(self, term_name: str, **kwargs):
        assert term_name is not None
        if self._live(term_name) is not None:
            return self.terminals[term_name]
        if self.max_terminals and len(self.terminals) >= self.max_terminals:
            raise terminado.management.MaxTerminalsReached(self.max_terminals)

        self.log.info("New terminal with specified name: %s", term_name)
        term = await self.spawn(**kwargs)
        if term_name in self.terminals:
            # Another connection started it meanwhile.
            term.kill()
            return self.terminals[term_name]
        term.term_name = term_name
        self.terminals[term_name] = term
        self.start_reading(term)
        return term

    def get_terminal(self, term_name: str, **kwargs):
        assert term_name is not None

        if self._live(term_name) is not None:
            return self.terminals[term_name]

        if self.max_terminals and len(self.terminals) >= self.max_terminals:
            raise terminado.management.MaxTerminalsReached(self.max_terminals)

        # Create new terminal
        self.log.info("New terminal with specified name: %s", term_name)
        term = self.new_terminal(**kwargs)
        term.term_name = term_name
        self.terminals[term_name] = term
        self.start_reading(term)
        return term


class MageUniqueTermManager(SpawnedShellMixin, AsyncSpawnMixin, terminado.UniqueTermManager):
    async def get_terminal_async(self, term_name: str = None, **kwargs):
        if self.max_terminals and len(self.ptys_by_fd) >= self.max_terminals:
            raise terminado.management.MaxTerminalsReached(self.max_terminals)
        term = await self.spawn(**kwargs)
        self.start_reading(term)
        return term

    def get_terminal(self, url_component=None, **kwargs):
        if self.max_terminals and len(self.ptys_by_fd) >= self.max_terminals:
            raise terminado.management.MaxTerminalsReached(self.max_terminals)

        term = self.new_terminal(**kwargs)
        self.start_reading(term)
        return term


class TerminalWebsocketServer(terminado.TermSocket):
    @property
    def term_command(self):
        return next(iter(self.term_manager.shell_command))

    def check_origin(self, origin):
        return True

    def on_pty_read(self, text):
        """Data read from pty; send to frontend"""
        updated_text = text
        if self.term_command == 'cmd':
            xterm_escape = re.compile(r'(?:\x1B\]0;).*\x07')
            updated_text = xterm_escape.sub('', text)
        try:
            self.send_json_message(["stdout", updated_text])
        except WebSocketClosedError:
            # The PTY can produce output after the terminal websocket disconnects.
            # This is an expected race during tab closes and network interruptions.
            pass

    async def open(self, url_component=None):
        """Websocket connection opened.

        Call our terminal manager to get a terminal, and connect to it as a
        client.
        """
        # Jupyter has a mixin to ping websockets and keep connections through
        # proxies alive. Call super() to allow that to set up:
        super(terminado.TermSocket, self).open(url_component)

        cwd = self.get_argument('cwd', None, True)
        if cwd:
            try:
                ensure_file_is_in_project(cwd)
                if not os.path.exists(cwd):
                    self._logger.warning(
                        f'The specified path {cwd} does not exist in the project directory.')
                    cwd = None
            except FileNotInProjectError:
                self._logger.warning(f'The specified path {cwd} is not in the project directory.')
                cwd = None
            if cwd is None:
                self._logger.warning('Using default path for terminal cwd...')

        term_name = self.get_argument('term_name', None, True)
        term_name = term_name if term_name else 'tty'
        self._logger.info("TermSocket.open: %s", term_name)

        # A terminal's clients get all its output, its recent output first. With
        # authentication on, a connection attaches only once signed in as an editor: by
        # the session cookie of its handshake, or by its first message with a valid token.
        # Any connection used to start a shell and read the output of anyone's terminal.
        self.__pending = (term_name, cwd)
        if DISABLE_TERMINAL:
            return
        if not (REQUIRE_USER_AUTHENTICATION or is_disable_pipeline_edit_access()) or \
                self.__editor_by_cookie():
            await self.__attach()

    def __editor_by_cookie(self) -> bool:
        from mage_ai.api.middleware import user_from_cookie

        if is_disable_pipeline_edit_access():
            return False
        try:
            user = user_from_cookie(self.request.headers.get('Cookie', ''))
            return user is not None and has_at_least_editor_role(
                user, Entity.PROJECT, get_project_uuid(),
            )
        except Exception:
            return False

    async def __attach(self):
        # open and the first message both attach; the second waits for the first.
        if getattr(self, '_attaching', None) is None:
            if getattr(self, 'terminal', None) is not None or self.__pending is None:
                return
            term_name, cwd = self.__pending
            self.__pending = None
            self._attaching = asyncio.ensure_future(
                self.__initialize_terminal(term_name, cwd=cwd),
            )
        await self._attaching

    async def on_message(self, raw_message):
        message = json.loads(raw_message)

        api_key = message.get('api_key')
        token = message.get('token')
        # This is a list of strings
        command = message.get('command')

        index = find_index(lambda x: x == '__CLEAR_OUTPUT__', command or [])
        if index >= 0:
            command[index] = r"clear -x && history -c && history -w && clear -x"

        # If terminal access disable return
        if DISABLE_TERMINAL:
            return self.send_json_message(
                ['stdout', f'{command[1]}\nUnauthorized access to the terminal.'])

        if REQUIRE_USER_AUTHENTICATION or is_disable_pipeline_edit_access():
            valid = False
            if api_key and token:
                oauth_client = Oauth2Application.query.filter(
                    Oauth2Application.client_id == api_key,
                ).first()
                if oauth_client:
                    oauth_token, valid = authenticate_client_and_token(oauth_client.id, token)
                    if valid and oauth_token and oauth_token.user:
                        valid = has_at_least_editor_role(
                            oauth_token.user,
                            Entity.PROJECT,
                            get_project_uuid(),
                        )
                    else:
                        # If the user is deleted, set valid to False
                        valid = False
            elif not token:
                # The pages cannot read the HttpOnly session cookie to send its token
                # (I-25): an editor signed in by the handshake's cookie may type.
                valid = self.__editor_by_cookie()
            if not valid or is_disable_pipeline_edit_access():
                return self.send_json_message(
                    ['stdout', f'{command[1]}\nUnauthorized access to the terminal.'])

        await self.__attach()
        if getattr(self, 'terminal', None) is None:
            return
        return terminado.TermSocket.on_message(self, json.dumps(command))

    async def __initialize_terminal(self, term_name: str, cwd: str = None):
        self.terminal = await self.term_manager.get_terminal_async(term_name, cwd=cwd)
        self.terminal.clients.append(self)

        self.send_json_message(["setup", {}])
        self._logger.info("TermSocket.open: Opened %s", term_name)
        # Now drain the preopen buffer, if reconnect.
        buffered = ""
        preopen_buffer = self.terminal.read_buffer.copy()
        while True:
            if not preopen_buffer:
                break
            s = preopen_buffer.popleft()
            buffered += s
        if buffered:
            self.on_pty_read(buffered)

        # Turn enable-bracketed-paste off since it can mess up the output.
        if self.term_command == 'bash':
            try:
                self.terminal.ptyproc.write(
                    "bind 'set enable-bracketed-paste off' # Mage terminal settings command\r")
            except (OSError, ValueError) as error:
                # The shell exited as it started; the connection used to drop with no word.
                self._logger.error('Terminal %s: its shell exited at start: %s', term_name, error)
                self.send_json_message(['stdout', (
                    f'The terminal\'s shell ({self.term_command}) exited as it started. '
                    'Reload the page to start another; the server log has the details.\r\n'
                )])
