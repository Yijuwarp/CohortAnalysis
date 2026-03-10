"""
Short summary: lightweight performance timing helper.
"""
import logging
import time

logger = logging.getLogger("performance")


def time_block(label: str):
    start = time.perf_counter()

    def end(**metadata):
        duration_ms = (time.perf_counter() - start) * 1000
        logger.info(f"[PERF] {label} | {duration_ms:.2f} ms | {metadata}")

    return end
