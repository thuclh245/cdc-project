import random
import time
from decimal import Decimal


def random_money(min_value=50_000, max_value=5_000_000):
    return Decimal(random.randint(min_value, max_value))


def random_discount():
    return Decimal(random.choice([0, 0, 0, 5000, 10000, 20000, 50000]))


def random_shipping_fee():
    return Decimal(random.choice([0, 15000, 20000, 30000]))


def sleep_random(min_seconds=0.2, max_seconds=1.5):
    time.sleep(random.uniform(min_seconds, max_seconds))


def sleep_until_next_cycle(cycle_started_at, interval_seconds):
    """Keep a stable cadence even when a cycle itself takes a few seconds."""
    elapsed = time.monotonic() - cycle_started_at
    time.sleep(max(0, interval_seconds - elapsed))


def log_event(event_name, message):
    print(f"[{event_name}] {message}")
