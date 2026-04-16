import logging
import time
import sys

logger = logging.getLogger("performance")
logger.setLevel(logging.INFO)

# Ensure console output if not already configured
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s [PERF] %(message)s', datefmt='%H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def time_block(label: str):
    start = time.perf_counter()

    def end(**metadata):
        duration_ms = (time.perf_counter() - start) * 1000
        meta_str = f" | {metadata}" if metadata else ""
        logger.info(f"{label:<25} | {duration_ms:>9.2f} ms{meta_str}")

    return end
