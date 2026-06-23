from scripts.database.connection import get_conn
from scripts.common.utils import sleep_random
from scripts.services.payment_service import update_random_payment


def run_payment_stream():
    conn = get_conn()

    try:
        print("Payment stream started.")

        while True:
            try:
                update_random_payment(conn)

            except Exception as e:
                conn.rollback()
                print(f"[ERROR PAYMENT STREAM] {e}")

            sleep_random(0.8, 2.5)

    finally:
        conn.close()


if __name__ == "__main__":
    run_payment_stream()