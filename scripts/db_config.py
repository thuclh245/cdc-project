import os


def get_postgres_config():
    """Return the shared PostgreSQL connection settings for host-side scripts."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5433")),
        "database": os.getenv("POSTGRES_DB", "cdc_demo"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def get_postgres_target():
    """Return a password-free connection description for logs."""
    config = get_postgres_config()
    return (
        f"{config['user']}@{config['host']}:{config['port']}/"
        f"{config['database']}"
    )
