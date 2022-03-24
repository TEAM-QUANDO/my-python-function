from functools import wraps
import time


def timefn(fn):
    @wraps(fn)
    def measure_time(*args, **kwargs):
        t1 = time.time()
        result = fn(*args, **kwargs)
        t2 = time.time()
        print(f"@timefn: {fn.__name__} took {t2 - t1} seconds")
        return result
    return measure_time


@timefn
def run_loop():
    val = 0
    for i in range(20_000_000):
        val = val + 1


run_loop()
