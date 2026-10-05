#!/usr/bin/env python3
"""Send commands to the spec server, keep track of queued commands, and handle responses.

Adapted from ``app/spec_controller.py`` in
https://github.com/CHESSComputing/ornl-watchdog (commit 6ef6188), with
the ID1A3/ORNL-specific data-collection methods removed.
"""

import asyncio
import queue
import threading
import time
import traceback

from id4b_pyspec import get_logger


logger = get_logger("spec_controller")


class SpecController:
    """Thread-safe client for sending commands to a running SPEC server.

    Commands are queued via :meth:`enqueue` and dispatched one at a time
    by a background worker thread.  Each command sequence may include an
    optional callback that is invoked after the last command in the
    sequence completes successfully.

    :ivar spec_host: Hostname or IP address of the SPEC server.
    :vartype spec_host: str
    :ivar spec_port: TCP port the SPEC server listens on.
    :vartype spec_port: int
    :ivar spec_timeout: Per-command timeout in seconds.
    :vartype spec_timeout: int
    """

    def __init__(self, spec_host, spec_port, spec_timeout):
        """Initialise the controller, connect to SPEC, and start the
        worker thread.

        :param spec_host: Hostname or IP address of the SPEC server.
        :type spec_host: str
        :param spec_port: TCP port the SPEC server listens on.
        :type spec_port: int
        :param spec_timeout: Per-command timeout in seconds.
        :type spec_timeout: int
        """
        self.spec_host = spec_host
        self.spec_port = spec_port
        self.spec_timeout = spec_timeout

        self.client = None
        self.async_event_loop = asyncio.new_event_loop()
        self._loop_thread = threading.Thread(
            target=self.async_event_loop.run_forever,
            daemon=True
        )
        self._loop_thread.start()
        self._connect()

        self.queue = queue.Queue()
        self.worker = threading.Thread(
            target=self._worker_loop,
            daemon=True
        )
        self._scan_n = self.client.var("SCAN_N")
        self._outfiles = self.client.var("OUTFILES")
        self._datafile = self.client.var("DATAFILE")
        self._status_ready = self.client.status().ready

        self.worker.start()

    def run_with_timeout(self, coroutine, *coroutine_args, **coroutine_kwargs):
        """Run an async coroutine from a synchronous context with a timeout.

        Schedules *coroutine* on the controller's dedicated event loop via
        :func:`asyncio.run_coroutine_threadsafe` and waits up to
        :attr:`spec_timeout` seconds for a result.

        :param coroutine: Async callable to execute.
        :param coroutine_args: Positional arguments forwarded to *coroutine*.
        :param coroutine_kwargs: Keyword arguments forwarded to *coroutine*.
        :returns: The coroutine's return value, or the raised
            :exc:`Exception` instance if execution failed.
        """
        future = asyncio.run_coroutine_threadsafe(
            coroutine(*coroutine_args, **coroutine_kwargs),
            self.async_event_loop,
        )
        try:
            return future.result(timeout=self.spec_timeout)
        except Exception as e:
            future.cancel()
            return e

    def _connect(self, max_retries=-1, retry_delay=5):
        """Create and connect the ``pyspec`` client, retrying on failure.

        Imports :class:`pyspec.client.Client`, creates an instance pointed
        at :attr:`spec_host`/:attr:`spec_port`, and attempts to enter its
        async context manager (which opens the TCP connection and performs
        the ``HELLO`` handshake).  Success is verified by checking
        ``client._connection.is_connected`` after each attempt.

        If a partial connection is established (TCP open but handshake
        failed), the connection is cleanly exited before the next attempt.

        :param max_retries: Maximum number of connection attempts. If
            less than 0, infinite retries.
        :type max_retries: int
        :param retry_delay: Seconds to wait between attempts.
        :type retry_delay: int or float
        :raises ConnectionError: If all *max_retries* attempts fail.
        """
        from pyspec.client import Client

        logger.info("Initializing pyspec.client.Client")
        self.client = Client(host=self.spec_host, port=self.spec_port)

        attempt = 1
        while attempt <= max_retries or max_retries < 0:
            logger.info(
                f"Connecting to SPEC at {self.spec_host}:{self.spec_port} "
                f"(attempt {attempt}/{max_retries})"
            )
            result = self.run_with_timeout(self.client.__aenter__)

            if not isinstance(result, Exception) \
               and self.client._connection.is_connected:
                logger.info("Connected to SPEC server")
                return

            if isinstance(result, Exception):
                logger.warning(
                    f"Connection attempt {attempt} raised: {result!r}"
                )
                traceback.print_exc()
            else:
                logger.warning(
                    f"Connection attempt {attempt} failed: "
                    f"is_connected={self.client._connection.is_connected}"
                )

            # Clean up any partial connection state before retrying
            if self.client._connection.is_connected:
                self.run_with_timeout(self.client.__aexit__, None, None, None)

            attempt += 1
            if attempt <= max_retries or max_retries < 0:
                logger.info(f"Retrying in {retry_delay}s...")
                time.sleep(retry_delay)

        raise ConnectionError(
            f"Failed to connect to SPEC at {self.spec_host}:{self.spec_port} "
            f"after {max_retries} attempt(s)"
        )

    def enqueue(self, command_sequence, callback=None):
        """Add a SPEC command sequence to the processing queue.

        The sequence is consumed by the background worker thread in FIFO
        order.  The optional *callback* is called once after the last
        command in *command_sequence* completes without error.

        :param command_sequence: Ordered list of SPEC command strings.
        :type command_sequence: list[str]
        :param callback: Optional zero-argument callable invoked after the
            sequence completes.
        :type callback: callable or None
        """
        self.queue.put((command_sequence, callback))

    async def client_exec(self, command):
        """Execute a command on the SPEC server without sending an
        abort on timeout.

        Unlike ``self.client.exec``, if this coroutine raises an exception
        (e.g. due to a timeout), no abort message is sent to the server to
        stop execution of the remote function.

        :param command: SPEC command string to execute.
        :type command: str
        """
        async def remote_cmd(command):
            from pyspec._connection.protocol import Command, Header
            return await self.client._connection._send_with_reply(
                Header(Command.CMD_WITH_RETURN), data=command
            )
        return await remote_cmd(command)

    def _send(self, command):
        """Send a single SPEC command, reconnecting first if the
        connection is lost.

        Checks :attr:`pyspec._connection.connection.Connection.is_connected`
        before each send.  If the connection is down, :meth:`_connect` is
        called to re-establish it before proceeding.

        :param command: SPEC command string to execute.
        :type command: str
        """
        if not self.client._connection.is_connected:
            logger.warning(
                f"SPEC connection lost before sending '{command}', "
                "attempting to reconnect"
            )
            self._connect()

        logger.info(f"Sending SPEC command: {command}")
        result = self.run_with_timeout(self.client_exec, command)
        log_msg = f"Response from {command}: {result} (type: {type(result)})"
        if isinstance(result, Exception):
            # if isinstance(result, concurrent.futures._base.TimeoutError):
            #     # Timeout error
            #     pass
            # elif isinstance(
            #         result,
            #         pyspec._connection.client_connection.RemoteException):
            #     # SPEC error
            #     pass
            log_method = logger.error
        else:
            log_method = logger.info
        log_method(log_msg)
        return result

    def _worker_loop(self):
        """Continuously drain the command queue in a background thread.

        Blocks on :attr:`queue`, dequeues ``(commands, callback)`` tuples,
        sends each command via :meth:`_send`, and then calls *callback* if
        one was supplied.  Exceptions are caught and logged so the worker
        never exits unintentionally.
        """
        while True:
            commands, callback = self.queue.get()
            try:
                for cmd in commands:
                    while not self.status_ready:
                        logger.warning("SPEC not ready; waiting for ready")
                        time.sleep(5)
                    self._send(cmd)
                if callback:
                    logger.info(
                        "Running callback after "
                        +f"{', '.join([cmd for cmd in commands])}"
                    )
                    callback()
            except Exception as e:
                logger.error(f"{e!r}")
                traceback.print_exc()
            finally:
                self.queue.task_done()

    @property
    def scan_n(self):
        """Current SPEC scan number (``SCAN_N`` variable).

        Fetches the value from SPEC at call time via
        :meth:`run_with_timeout`.  Logs and returns ``None`` on error.

        :returns: Current scan number, or ``None`` if the fetch failed.
        :rtype: int or None
        """
        logger.info("Getting SCAN_N from SPEC")
        result = self.run_with_timeout(self._scan_n.get)
        if isinstance(result, Exception):
            logger.error(result)
        logger.debug(f"Got SCAN_N: {result}")
        return result

    @property
    def outfiles(self):
        """Current SPEC ``OUTFILES`` variable.

        Fetches the value from SPEC at call time via
        :meth:`run_with_timeout`.  Logs and returns the raw value, or
        the :exc:`Exception` on failure.
        """
        logger.info(f"Getting OUTFILES from SPEC")
        result = self.run_with_timeout(self._outfiles.get)
        if isinstance(result, Exception):
            logger.error(result)
        logger.debug(f"Got OUTFILES: {result}")
        return result

    @property
    def datafile(self):
        """Current SPEC ``DATAFILE`` variable.

        Fetches the value from SPEC at call time via
        :meth:`run_with_timeout`.  Logs and returns the raw value, or
        the :exc:`Exception` on failure.
        """
        logger.info(f"Getting DATAFILE from SPEC")
        result = self.run_with_timeout(self._datafile.get)
        if isinstance(result, Exception):
            logger.error(result)
        logger.debug(f"Got DATAFILE: {result}")
        return result

    @property
    def status_ready(self):
        """Whether SPEC is ready to accept a new command
        (``status/ready`` variable).

        Fetches the value from SPEC at call time via
        :meth:`run_with_timeout`.  Logs and returns the boolean
        result, or the :exc:`Exception` on failure.

        :rtype: bool or Exception
        """
        logger.info("Getting status/ready")
        result = self.run_with_timeout(self._status_ready.get)
        if isinstance(result, Exception):
            logger.error(result)
        logger.debug(f"Got status/ready: {result}")
        return result


class TestSpecController:
    """Drop-in replacement for :class:`SpecController` that never connects to
    or sends commands through a real ``pyspec`` server.

    All commands are logged at INFO level and discarded.  Properties return
    plausible dummy values so calling code can exercise its logic without
    a live SPEC server.

    :ivar spec_host: Hostname or IP address passed in at construction.
    :vartype spec_host: str
    :ivar spec_port: Port number passed in at construction.
    :vartype spec_port: int
    :ivar spec_timeout: Timeout value passed in at construction (unused).
    :vartype spec_timeout: int
    """

    def __init__(self, spec_host, spec_port, spec_timeout):
        """Initialise the test controller and start the worker thread.

        No network connection is made.  Parameters are stored for
        informational purposes only.

        :param spec_host: Hostname or IP address (stored, not connected to).
        :type spec_host: str
        :param spec_port: Port number (stored, not connected to).
        :type spec_port: int
        :param spec_timeout: Per-command timeout in seconds (stored, unused).
        :type spec_timeout: int
        """
        self.spec_host = spec_host
        self.spec_port = spec_port
        self.spec_timeout = spec_timeout

        self._scan_n = 0
        self.queue = queue.Queue()
        self.worker = threading.Thread(target=self._worker_loop, daemon=True)
        self.worker.start()
        logger.info(
            f"TestSpecController initialised (no connection to "
            f"{spec_host}:{spec_port})"
        )

    def enqueue(self, command_sequence, callback=None):
        """Add a SPEC command sequence to the processing queue.

        The sequence is consumed by the background worker thread in FIFO
        order.  Commands are logged rather than transmitted.  The optional
        *callback* is called once after the last command in
        *command_sequence* is processed.

        :param command_sequence: Ordered list of SPEC command strings.
        :type command_sequence: list[str]
        :param callback: Optional zero-argument callable invoked after the
            sequence completes.
        :type callback: callable or None
        """
        self.queue.put((command_sequence, callback))

    def _worker_loop(self):
        """Continuously drain the command queue in a background thread.

        Blocks on :attr:`queue`, dequeues ``(commands, callback)`` tuples,
        logs each command at INFO level, increments :attr:`_scan_n` once
        per sequence, and then calls *callback* if one was supplied.
        Exceptions are caught and logged so the worker never exits
        unintentionally.
        """
        while True:
            commands, callback = self.queue.get()
            try:
                for cmd in commands:
                    logger.info(f"[TEST-SPEC] command: {cmd}")
                self._scan_n += 1
                if callback:
                    callback()
            except Exception as e:
                logger.error(f"{e!r}")
                traceback.print_exc()
            finally:
                self.queue.task_done()

    @property
    def scan_n(self):
        """Monotonically increasing scan counter.

        Incremented once per completed command sequence by the worker
        thread.  Logs and returns the current value.

        :returns: Current fake scan number.
        :rtype: int
        """
        logger.info(f"[TEST-SPEC] SCAN_N = {self._scan_n}")
        return self._scan_n

    @property
    def status_ready(self):
        """Always returns ``True`` — the test controller is always ready.

        :rtype: bool
        """
        return True
