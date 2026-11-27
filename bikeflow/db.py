"""B2: database connection."""

import os

import psycopg


def get_conn() -> psycopg.Connection:
    """B2: open a Postgres connection.

    Input: env vars POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB,
    POSTGRES_USER, POSTGRES_PASSWORD (spec 4.3).
    Output: an open psycopg connection.
    """
    return psycopg.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )
