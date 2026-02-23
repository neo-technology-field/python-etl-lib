import io
import logging
import os
import signal
import sys


def merge_summery(summery_1: dict, summery_2: dict) -> dict:
    """
    Helper function to merge dicts. Assuming that values are numbers.
    If a key exists in both dicts, then the result will contain a key with the added values.
    """
    return {i: summery_1.get(i, 0) + summery_2.get(i, 0)
            for i in set(summery_1).union(summery_2)}


def setup_logging(log_file=None):
    """
    Set up the logging. INFO is used for the root logger.
    Via ETL_LIB_LOG_LEVEL environment variable, the log level of the library itself can be set to another level.
    It also defaults to INFO.
    """
    fmt = '%(asctime)s - %(levelname)s - %(name)s - [%(threadName)s] - %(message)s'
    formatter = logging.Formatter(fmt)

    def _safe_console_stream(stream):
        # handling streams that are not capable of UTF8 (and it looks like some Windows systems can't)
        # most likely looks ugly, but does not crash
        try:
            enc = (getattr(stream, "encoding", None) or "").lower()
            if enc.startswith("utf"):
                return stream
            buf = getattr(stream, "buffer", None)
            if buf is not None:
                return io.TextIOWrapper(buf, encoding="utf-8", errors="backslashreplace")
        except (ValueError, OSError, AttributeError, TypeError):
            pass
        return stream

    root_handlers = [logging.StreamHandler(stream=_safe_console_stream(sys.stderr))]
    if log_file:
        root_handlers.append(logging.FileHandler(log_file, encoding="utf-8", errors="backslashreplace"))

    for h in root_handlers:
        h.setLevel(logging.INFO)
        h.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO, handlers=root_handlers, force=True)

    raw = os.getenv("ETL_LIB_LOG_LEVEL", "INFO")
    try:
        etl_level = int(raw) if str(raw).isdigit() else getattr(logging, str(raw).upper())
    except Exception:
        etl_level = logging.DEBUG

    etl_logger = logging.getLogger('etl_lib')
    etl_logger.setLevel(etl_level)
    etl_logger.propagate = False
    etl_logger.handlers.clear()

    dbg_console = logging.StreamHandler(stream=_safe_console_stream(sys.stderr))
    dbg_console.setLevel(logging.NOTSET)
    dbg_console.setFormatter(formatter)
    etl_logger.addHandler(dbg_console)

    if log_file:
        dbg_file = logging.FileHandler(log_file, encoding="utf-8", errors="backslashreplace")
        dbg_file.setLevel(logging.NOTSET)
        dbg_file.setFormatter(formatter)
        etl_logger.addHandler(dbg_file)


def add_sigint_handler(handler_to_add):
    """
    Register handler_to_add(signum, frame) to run on Ctrl-C,
    chaining any previously registered handler afterward.
    """
    old_handler = signal.getsignal(signal.SIGINT)

    def chained_handler(signum, frame):
        handler_to_add(signum, frame)
        if callable(old_handler):
            old_handler(signum, frame)

    signal.signal(signal.SIGINT, chained_handler)
