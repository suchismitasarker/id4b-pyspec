#!/usr/bin/env python3
"""Small Tk window for sending commands to a SPEC server.

Type a command and press Enter or click Send to run it in SPEC.  The
command and its return value or error appear in the output area.  The
STOP button aborts the running command, as ^C does at the SPEC console.
"""

import argparse
import logging
import queue
import threading
import tkinter as tk
from tkinter import scrolledtext

from id4b_pyspec.prompt import describe_result
from id4b_pyspec.spec_controller import SpecController


class SpecGui:
    """Tk window with a command entry, a Send button and a STOP button.

    SPEC calls block, so they run in background threads and the window
    stays responsive.  Tk may only be used from the thread running the
    Tk main loop, so background threads hand their updates back through
    a queue that the Tk thread polls every :attr:`POLL_MS` milliseconds.

    :param root: Tk root window to build the interface in.
    :type root: tkinter.Tk
    :param controller: Connected SPEC controller.
    :type controller: SpecController
    """

    POLL_MS = 100

    def __init__(self, root, controller):
        self.root = root
        self.controller = controller
        self._tk_calls = queue.Queue()
        self._busy = False

        root.title(f"SPEC {controller.spec_host}:{controller.spec_port}")

        self.output = scrolledtext.ScrolledText(
            root, height=15, width=80, state="disabled", font="TkFixedFont"
        )
        self.output.pack(fill="both", expand=True, padx=8, pady=(8, 4))

        entry_row = tk.Frame(root)
        entry_row.pack(fill="x", padx=8, pady=4)
        self.entry = tk.Entry(entry_row, font="TkFixedFont")
        self.entry.pack(side="left", fill="x", expand=True)
        self.entry.bind("<Return>", lambda event: self.send())
        self.send_button = tk.Button(
            entry_row, text="Send", command=self.send
        )
        self.send_button.pack(side="left", padx=(4, 0))

        self.stop_button = tk.Button(
            root, text="STOP", command=self.stop, height=2,
            font=("Helvetica", 24, "bold"),
            bg="red", fg="white",
            activebackground="darkred", activeforeground="white",
        )
        self.stop_button.pack(fill="x", padx=8, pady=(4, 8))

        self.entry.focus_set()
        self.root.after(self.POLL_MS, self._run_tk_calls)

    def send(self):
        """Send the command in the entry box to SPEC.

        Does nothing if the box is empty or a command is still running.
        """
        command = self.entry.get().strip()
        if not command or self._busy:
            return
        self.entry.delete(0, "end")
        self._write(f"> {command}")
        self._set_busy(True)
        threading.Thread(
            target=self._send_in_background, args=(command,), daemon=True
        ).start()

    def stop(self):
        """Abort the command SPEC is running.

        The abort is sent from a background thread so the window never
        freezes waiting for it and STOP can always be pressed again.
        """
        self._write("STOP: sending abort to SPEC")
        threading.Thread(
            target=self._abort_in_background, daemon=True
        ).start()

    def _send_in_background(self, command):
        """Send *command* and hand the result back to the Tk thread."""
        try:
            result = self.controller.send(command)
        except Exception as e:
            result = e
        self._call_in_tk(self._finish_send, result)

    def _abort_in_background(self):
        """Send the abort and report a failure to the Tk thread."""
        result = self.controller.abort()
        if isinstance(result, Exception):
            self._call_in_tk(
                self._write,
                f"Failed to abort the SPEC command: {result!r}"
            )

    def _finish_send(self, result):
        """Show the result of a command and re-enable Send."""
        text = describe_result(result, self.controller.spec_timeout)
        if text is not None:
            self._write(text)
        self._set_busy(False)

    def _set_busy(self, busy):
        """Record whether a command is running and update Send."""
        self._busy = busy
        self.send_button.config(state="disabled" if busy else "normal")

    def _write(self, line):
        """Append *line* to the output area and scroll to it."""
        self.output.config(state="normal")
        self.output.insert("end", line + "\n")
        self.output.see("end")
        self.output.config(state="disabled")

    def _call_in_tk(self, func, *args):
        """Queue *func* to be called with *args* on the Tk thread."""
        self._tk_calls.put((func, args))

    def _run_tk_calls(self):
        """Run the queued calls on the Tk thread, then reschedule."""
        while True:
            try:
                func, args = self._tk_calls.get_nowait()
            except queue.Empty:
                break
            func(*args)
        self.root.after(self.POLL_MS, self._run_tk_calls)


def main():
    """Parse command-line arguments, connect to SPEC, and open the
    window."""
    parser = argparse.ArgumentParser(
        description="Small window for sending commands to a SPEC server."
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

    # Create the window first so a missing display fails before the
    # (possibly long) connection attempts
    root = tk.Tk()
    root.withdraw()
    controller = SpecController(args.host, args.port, args.timeout)
    SpecGui(root, controller)
    root.deiconify()
    root.mainloop()


if __name__ == "__main__":
    main()
