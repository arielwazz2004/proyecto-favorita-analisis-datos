import os

DATA_DIR = os.path.expanduser("~/proyecto-favorita/data")
STAGING_DIR = os.path.expanduser("~/proyecto-favorita/staging")
os.makedirs(STAGING_DIR, exist_ok=True)
POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "favorita",
    "user": "postgres",
    "password": "favorita2026",
}

POSTGRES_URI = (
    f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}"
    f"@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['dbname']}"
)