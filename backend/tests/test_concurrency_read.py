import time
import threading
import requests

BASE_URL = "http://127.0.0.1:8000"
USER_ID = "a87d16d1" # Valid 8-char hex ID

def make_read_request():
    start = time.time()
    # Use /events or /retention (small results)
    response = requests.get(f"{BASE_URL}/events?user_id={USER_ID}")
    end = time.time()
    return end - start

def test_concurrency():
    threads = []
    results = []

    def worker():
        results.append(make_read_request())

    start_all = time.time()
    for _ in range(5):
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    for t in threads:
        t.join()
    end_all = time.time()

    total_time = end_all - start_all
    avg_request_time = sum(results) / (len(results) if results else 1)

    print(f"\nTotal time for 5 parallel requests: {total_time:.4f}s")
    print(f"Sum of individual request times: {sum(results):.4f}s")
    
    if total_time < sum(results) * 0.8:
        print("✅ CONCURRENCY SUCCESS: Requests ran in parallel.")
    else:
        print("⚠️ SERIALIZATION DETECTED: Total time is close to sum of individual times.")

if __name__ == "__main__":
    test_concurrency()
