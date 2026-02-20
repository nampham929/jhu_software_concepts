"""Shared helpers for database connection setup."""

from __future__ import annotations


def build_db_config(db_name, db_user, db_password, db_host, db_port):
    """Build a standard db-config mapping used by connection helpers."""
    return {
        "db_name": db_name,
        "db_user": db_user,
        "db_password": db_password,
        "db_host": db_host,
        "db_port": db_port,
    }


def create_connection_with_driver(
    connect_callable,
    operational_error_cls,
    db_config,
):
    """Create and return a PostgreSQL connection via an injected driver."""
    connection = None
    try:
        connection = connect_callable(
            dbname=db_config["db_name"],
            user=db_config["db_user"],
            password=db_config["db_password"],
            host=db_config["db_host"],
            port=db_config["db_port"],
        )
        print("Connection to PostgreSQL DB successful")
    except operational_error_cls as error:
        print(f"The error '{error}' occurred")
        raise
    return connection


def create_connection_from_env(connect_url_callable, create_connection_callable, getenv):
    """Create a DB connection from DATABASE_URL or discrete DB_* env vars."""
    database_url = getenv("DATABASE_URL")
    if database_url:
        return connect_url_callable(database_url)

    config = {
        "DB_NAME": getenv("DB_NAME"),
        "DB_USER": getenv("DB_USER"),
        "DB_PASSWORD": getenv("DB_PASSWORD"),
        "DB_HOST": getenv("DB_HOST"),
        "DB_PORT": getenv("DB_PORT"),
    }
    missing = [name for name, value in config.items() if not value]
    if missing:
        raise RuntimeError(
            "Database configuration missing. Set DATABASE_URL or "
            "DB_NAME/DB_USER/DB_PASSWORD/DB_HOST/DB_PORT. "
            f"Missing: {', '.join(missing)}"
        )
    return create_connection_callable(
        config["DB_NAME"],
        config["DB_USER"],
        config["DB_PASSWORD"],
        config["DB_HOST"],
        config["DB_PORT"],
    )
