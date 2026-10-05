#!/usr/bin/env python3
"""Interactive prompt that sends typed commands to a SPEC server.

Each line typed at the prompt is sent to SPEC with
:meth:`~id4b_pyspec.spec_controller.SpecController.send`, and the
command's return value or error is printed.  If
:data:`ECHO_SPEC_OUTPUT` is set, everything SPEC prints to its screen
is echoed locally as well.
"""

import argparse
import concurrent.futures
import logging
import readline  # noqa: F401 (enables line editing and history in input())

from id4b_pyspec.spec_controller import SpecController


EXIT_COMMANDS = ("exit", "quit")

# Set to True to echo everything SPEC prints to its screen
ECHO_SPEC_OUTPUT = False


def describe_result(result, spec_timeout):
    """Return the text to show for a result of
    :meth:`~id4b_pyspec.spec_controller.SpecController.send`.

    :param result: Value returned by ``send``: SPEC's reply, or the
        exception raised while sending.
    :param spec_timeout: The controller's per-command timeout, used in
        the timeout message.
    :type spec_timeout: float or None
    :returns: Text to show, or ``None`` if there is nothing to show.
    :rtype: str or None
    """
    if isinstance(result, concurrent.futures.CancelledError):
        return "Aborted."
    if isinstance(result, concurrent.futures.TimeoutError):
        return (
            f"Timed out after {spec_timeout} s; the command may still be "
            "running in SPEC."
        )
    if isinstance(result, Exception):
        return f"Error: {result!r}"
    if result is not None:
        return str(result)
    return None


def run_prompt(controller, prompt):
    """Read commands from stdin and send them to SPEC until the user
    exits.

    Empty lines are ignored.  Typing one of :data:`EXIT_COMMANDS` or
    Ctrl-D leaves the prompt; these are handled locally and never sent
    to SPEC.  Ctrl-C while waiting for a command aborts it on the server,
    as ^C does at the SPEC console.

    :param controller: Connected SPEC controller.
    :type controller: SpecController
    :param prompt: Prompt string shown before each command.
    :type prompt: str
    """
    while True:
        try:
            command = input(prompt).strip()
        except EOFError:
            print()
            return
        except KeyboardInterrupt:
            print()
            continue
        if not command:
            continue
        if command in EXIT_COMMANDS:
            return

        try:
            result = controller.send(command)
        except KeyboardInterrupt:
            result = controller.abort()
            if isinstance(result, Exception):
                print(f"\nFailed to abort the SPEC command: {result!r}")
            else:
                print("\nAborted.")
            continue
        text = describe_result(result, controller.spec_timeout)
        if text is not None:
            print(text)


def main():
    """Parse command-line arguments, connect to SPEC, and run the
    prompt."""
    parser = argparse.ArgumentParser(
        description="Send SPEC commands typed at a prompt to a SPEC server."
    )
    parser.add_argument("host", help="Hostname of the SPEC server.")
    parser.add_argument("port", type=int, help="Port of the SPEC server.")
    parser.add_argument(
        "--timeout", type=float, default=None,
        help="Seconds to wait for each command (default: wait "
             "indefinitely)."
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Show the controller's INFO and DEBUG log messages."
    )
    args = parser.parse_args()

    if not args.verbose:
        logging.getLogger("spec_controller").setLevel(logging.WARNING)

    controller = SpecController(args.host, args.port, args.timeout)

    tty_subscription = None
    if ECHO_SPEC_OUTPUT:
        # Echo everything SPEC prints to its screen
        tty = controller.client.output("tty")
        tty.on("update", lambda text: print(text, end="", flush=True))
        tty_subscription = tty.subscribed()
        result = controller.run_with_timeout(tty_subscription.__aenter__)
        if isinstance(result, Exception):
            print(f"Warning: not echoing SPEC output ({result!r})")
            tty_subscription = None

    print(
        f"Connected to SPEC at {args.host}:{args.port}. Type "
        f"{' or '.join(EXIT_COMMANDS)} (or Ctrl-D) to leave."
    )
    try:
        run_prompt(controller, f"{args.host}> ")
    finally:
        if tty_subscription is not None:
            controller.run_with_timeout(
                tty_subscription.__aexit__, None, None, None
            )


if __name__ == "__main__":
    main()
