"""List Airflow connection routing fields without exposing credentials."""

from airflow.models import Connection
from airflow.settings import Session


def main() -> None:
    """Print the CineFlux connections used by the orchestration DAGs."""
    with Session() as session:
        connections = (
            session.query(Connection)
            .filter(Connection.conn_id.like("cineflux_%"))
            .order_by(Connection.conn_id)
            .all()
        )

    print("conn_id\tconn_type\thost\tport\textra_keys")
    for connection in connections:
        extra_keys = ",".join(sorted(connection.extra_dejson))
        print(
            f"{connection.conn_id}\t{connection.conn_type}\t"
            f"{connection.host or ''}\t{connection.port or ''}\t{extra_keys}"
        )


if __name__ == "__main__":
    main()
