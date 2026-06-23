CATEGORIES = [
    "Electronics",
    "Fashion",
    "Books",
    "Home",
    "Beauty",
    "Sports",
    "Toys",
    "Food",
]

BRANDS = [
    "Samsung",
    "Apple",
    "Sony",
    "Nike",
    "Adidas",
    "Uniqlo",
    "Logitech",
    "Xiaomi",
    "Local Brand",
]

GENDERS = [
    "MALE",
    "FEMALE",
    "OTHER",
]

CUSTOMER_STATUSES = [
    "ACTIVE",
    "INACTIVE",
    "BLOCKED",
]

PRODUCT_STATUSES = [
    "ACTIVE",
    "OUT_OF_STOCK",
    "DISCONTINUED",
]

ORDER_STATUSES = [
    "PENDING",
    "CONFIRMED",
    "SHIPPING",
    "COMPLETED",
    "CANCELLED",
]

PAYMENT_METHODS = [
    "COD",
    "CREDIT_CARD",
    "BANK_TRANSFER",
    "MOMO",
    "ZALOPAY",
    "VNPAY",
]

PAYMENT_STATUSES = [
    "PENDING",
    "SUCCESS",
    "FAILED",
    "REFUNDED",
]

SHIPMENT_CARRIERS = [
    "GHN",
    "GHTK",
    "J&T Express",
    "Viettel Post",
    "VNPost",
]

SEED_CUSTOMERS = 1000
SEED_PRODUCTS = 300
SEED_ORDERS = 1000

# Fake realtime workload cadence. Keep these values small enough for a demo that
# resembles normal traffic instead of a benchmark/load test.
STREAM_INTERVAL_SECONDS = 60
CUSTOMERS_PER_CYCLE = 4
ORDERS_PER_CYCLE = 2
PAYMENT_UPDATES_PER_CYCLE = 2
SHIPMENTS_CREATED_PER_CYCLE = 2
SHIPMENT_UPDATES_PER_CYCLE = 2

PRODUCT_INTERVAL_SECONDS = 300
PRODUCT_EVENTS_PER_CYCLE = 1
INVENTORY_INTERVAL_SECONDS = 300
INVENTORY_IMPORTS_PER_CYCLE = 1
