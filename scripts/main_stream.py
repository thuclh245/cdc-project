import threading
import time

from scripts.common.constants import STREAM_INTERVAL_SECONDS
from scripts.streams.customer_stream import run_customer_stream
from scripts.streams.product_stream import run_product_stream
from scripts.streams.order_stream import run_order_stream
from scripts.streams.payment_stream import run_payment_stream


def start_thread(name, target):
    thread = threading.Thread(
        name=name,
        target=target,
        daemon=True,
    )

    thread.start()
    print(f"Started thread: {name}")

    return thread


def main():
    print("Starting all CDC fake streams...")

    threads = [
        start_thread("customer-stream", run_customer_stream),
        start_thread("product-stream", run_product_stream),
        start_thread("order-stream", run_order_stream),
        start_thread("payment-stream", run_payment_stream),
    ]

    try:
        while True:
            alive_threads = [thread.name for thread in threads if thread.is_alive()]
            print(f"Running streams: {alive_threads}")
            time.sleep(STREAM_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        print("Stopping streams...")


if __name__ == "__main__":
    main()
