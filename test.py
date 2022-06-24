import signal
import time


def foo(s, _other):
    print(s)
    exit()


catchable_sigs = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP}
for sig in catchable_sigs:
    signal.signal(sig, foo)

time.sleep(3600)
