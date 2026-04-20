"""
Pipeline Database — DuckDB-based pipeline simulation and management.

Manages simulated data pipelines with DuckDB tables, supports failure
injection (schema drift, null violations, type mismatches, duplicate keys),
and tracks pipeline metadata.
"""

import duckdb

from app.models import PipelineInfo, PipelineStatus


def get_db_connection(db_path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    """Create and return a DuckDB connection."""
    return duckdb.connect(database=db_path)


def initialize_pipelines(con: duckdb.DuckDBPyConnection):
    """
    Initialize the pipeline simulation database with sample tables and data.

    Creates realistic pipeline tables with intentional data quality issues
    for demonstration purposes.
    """
    # Orders pipeline table
    con.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            customer_name VARCHAR NOT NULL,
            amount DOUBLE,
            order_date DATE,
            status VARCHAR DEFAULT 'pending'
        )
    """)

    # Revenue pipeline table
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_revenue (
            date DATE,
            region VARCHAR,
            revenue DOUBLE,
            transactions INTEGER
        )
    """)

    # User events pipeline table
    con.execute("""
        CREATE TABLE IF NOT EXISTS user_events (
            event_id INTEGER,
            user_id INTEGER,
            event_type VARCHAR,
            event_data VARCHAR,
            created_at TIMESTAMP
        )
    """)

    # Inventory pipeline table
    con.execute("""
        CREATE TABLE IF NOT EXISTS inventory (
            sku VARCHAR,
            product_name VARCHAR,
            quantity INTEGER,
            warehouse VARCHAR,
            last_updated TIMESTAMP
        )
    """)

    # Pipeline metadata table
    con.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_metadata (
            name VARCHAR PRIMARY KEY,
            status VARCHAR DEFAULT 'healthy',
            last_run VARCHAR,
            schedule VARCHAR,
            owner VARCHAR,
            description VARCHAR
        )
    """)

    # Insert sample pipeline metadata
    pipelines = [
        ("daily_order_ingestion", "healthy", "2026-04-20T08:00:00",
         "0 8 * * *", "Data Engineering", "Ingests daily orders from source systems"),
        ("revenue_aggregation", "healthy", "2026-04-20T09:00:00",
         "0 9 * * *", "Analytics Team", "Aggregates daily revenue by region"),
        ("user_event_processing", "healthy", "2026-04-20T07:00:00",
         "*/30 * * * *", "Platform Team", "Processes user interaction events"),
        ("inventory_sync", "healthy", "2026-04-20T06:00:00",
         "0 6 * * *", "Supply Chain", "Syncs inventory from warehouse systems"),
        ("data_quality_checks", "healthy", "2026-04-20T10:00:00",
         "0 10 * * *", "Data Quality", "Runs data quality validation suite"),
    ]

    for p in pipelines:
        con.execute(
            "INSERT OR REPLACE INTO pipeline_metadata VALUES (?, ?, ?, ?, ?, ?)",
            p,
        )

    # Insert sample data with intentional quality issues
    _insert_sample_orders(con)
    _insert_sample_revenue(con)
    _insert_sample_events(con)
    _insert_sample_inventory(con)


def get_pipeline_list(con: duckdb.DuckDBPyConnection) -> list[PipelineInfo]:
    """Get a list of all pipeline metadata."""
    try:
        rows = con.execute(
            "SELECT name, status, last_run, schedule, owner, description "
            "FROM pipeline_metadata ORDER BY name"
        ).fetchall()
    except Exception:
        return _get_default_pipelines()

    pipelines = []
    table_map = {
        "daily_order_ingestion": ["orders"],
        "revenue_aggregation": ["daily_revenue"],
        "user_event_processing": ["user_events"],
        "inventory_sync": ["inventory"],
        "data_quality_checks": ["orders", "daily_revenue", "user_events", "inventory"],
    }

    for row in rows:
        pipelines.append(
            PipelineInfo(
                name=row[0],
                status=PipelineStatus(row[1]) if row[1] in PipelineStatus.__members__.values() else PipelineStatus.HEALTHY,
                last_run=row[2] or "",
                schedule=row[3] or "",
                owner=row[4] or "",
                tables=table_map.get(row[0], []),
                description=row[5] or "",
            )
        )

    return pipelines or _get_default_pipelines()


def update_pipeline_status(
    con: duckdb.DuckDBPyConnection,
    pipeline_name: str,
    status: PipelineStatus,
):
    """Update the status of a pipeline."""
    con.execute(
        "UPDATE pipeline_metadata SET status = ? WHERE name = ?",
        [status.value, pipeline_name],
    )


def _get_default_pipelines() -> list[PipelineInfo]:
    """Return default pipeline list when DB is not initialized."""
    return [
        PipelineInfo(
            name="daily_order_ingestion",
            status=PipelineStatus.HEALTHY,
            schedule="0 8 * * *",
            owner="Data Engineering",
            tables=["orders"],
            description="Ingests daily orders from source systems",
        ),
        PipelineInfo(
            name="revenue_aggregation",
            status=PipelineStatus.HEALTHY,
            schedule="0 9 * * *",
            owner="Analytics Team",
            tables=["daily_revenue"],
            description="Aggregates daily revenue by region",
        ),
        PipelineInfo(
            name="user_event_processing",
            status=PipelineStatus.HEALTHY,
            schedule="*/30 * * * *",
            owner="Platform Team",
            tables=["user_events"],
            description="Processes user interaction events",
        ),
        PipelineInfo(
            name="inventory_sync",
            status=PipelineStatus.HEALTHY,
            schedule="0 6 * * *",
            owner="Supply Chain",
            tables=["inventory"],
            description="Syncs inventory from warehouse systems",
        ),
        PipelineInfo(
            name="data_quality_checks",
            status=PipelineStatus.HEALTHY,
            schedule="0 10 * * *",
            owner="Data Quality",
            tables=["orders", "daily_revenue", "user_events", "inventory"],
            description="Runs data quality validation suite",
        ),
    ]


def _insert_sample_orders(con: duckdb.DuckDBPyConnection):
    """Insert sample orders with data quality issues."""
    orders = [
        (1, "Alice Johnson", 250.00, "2026-04-01", "completed"),
        (2, "Bob Smith", 175.50, "2026-04-02", "completed"),
        (3, "Charlie Brown", 320.00, "2026-04-03", "pending"),
        (4, "Diana Prince", 89.99, "2026-04-04", "completed"),
        (5, "Eve Wilson", 445.00, "2026-04-05", "shipped"),
        (6, "Frank Castle", 150.00, "2026-04-06", "completed"),
        (7, "Grace Hopper", 999.99, "2026-04-07", "pending"),
        (8, "Heidi Klum", 67.50, "2026-04-08", "completed"),
        (9, "Ivan Drago", 210.00, "2026-04-09", "shipped"),
        (10, "Judy Garland", 380.00, "2026-04-10", "completed"),
    ]
    for o in orders:
        con.execute("INSERT OR IGNORE INTO orders VALUES (?, ?, ?, ?, ?)", o)


def _insert_sample_revenue(con: duckdb.DuckDBPyConnection):
    """Insert sample revenue data."""
    revenues = [
        ("2026-04-01", "US-East", 15420.50, 142),
        ("2026-04-01", "US-West", 12300.00, 98),
        ("2026-04-01", "EU", 8750.25, 67),
        ("2026-04-02", "US-East", 16100.00, 155),
        ("2026-04-02", "US-West", 11800.75, 91),
        ("2026-04-02", "EU", 9200.00, 72),
    ]
    for r in revenues:
        con.execute("INSERT INTO daily_revenue VALUES (?, ?, ?, ?)", r)


def _insert_sample_events(con: duckdb.DuckDBPyConnection):
    """Insert sample user events."""
    events = [
        (1, 101, "page_view", '{"page": "/home"}', "2026-04-01 10:00:00"),
        (2, 102, "click", '{"button": "signup"}', "2026-04-01 10:05:00"),
        (3, 101, "purchase", '{"amount": 49.99}', "2026-04-01 10:10:00"),
        (4, 103, "page_view", '{"page": "/products"}', "2026-04-01 10:15:00"),
        (5, 104, "click", '{"button": "add_cart"}', "2026-04-01 10:20:00"),
    ]
    for e in events:
        con.execute("INSERT INTO user_events VALUES (?, ?, ?, ?, ?)", e)


def _insert_sample_inventory(con: duckdb.DuckDBPyConnection):
    """Insert sample inventory data."""
    items = [
        ("SKU-001", "Widget A", 150, "Warehouse-1", "2026-04-01 08:00:00"),
        ("SKU-002", "Widget B", 75, "Warehouse-1", "2026-04-01 08:00:00"),
        ("SKU-003", "Gadget X", 200, "Warehouse-2", "2026-04-01 08:00:00"),
        ("SKU-004", "Gadget Y", 30, "Warehouse-2", "2026-04-01 08:00:00"),
        ("SKU-005", "Tool Z", 500, "Warehouse-1", "2026-04-01 08:00:00"),
    ]
    for i in items:
        con.execute("INSERT INTO inventory VALUES (?, ?, ?, ?, ?)", i)
