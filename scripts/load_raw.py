import io
import sys
import pathlib
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import psycopg

month = sys.argv[1]
path = pathlib.Path("data/raw") / f"yellow_tripdata_{month}.parquet"
CONN = "host=localhost port=5433 dbname=nyc_taxi user=admin password=admin"


def pg_type(t):
    if pa.types.is_integer(t):
        return "BIGINT"
    if pa.types.is_floating(t):
        return "DOUBLE PRECISION"
    if pa.types.is_timestamp(t):
        return "TIMESTAMP"
    return "TEXT"


pf = pq.ParquetFile(path)
cols = [(f.name.lower(), pg_type(f.type)) for f in pf.schema_arrow]

col_defs = ", ".join(f'"{name}" {typ}' for name, typ in cols)
create_sql = f"""
CREATE TABLE IF NOT EXISTS raw.yellow_trips (
    {col_defs},
    source_month TEXT NOT NULL,
    loaded_at TIMESTAMP DEFAULT now()
)"""

col_names = ", ".join(f'"{name}"' for name, _ in cols) + ", source_month"
copy_sql = f"COPY raw.yellow_trips ({col_names}) FROM STDIN WITH (FORMAT csv)"

total = 0
with psycopg.connect(CONN) as conn:
    conn.execute(create_sql)
    conn.execute("DELETE FROM raw.yellow_trips WHERE source_month = %s", (month,))

    with conn.cursor() as cur:
        for batch in pf.iter_batches(batch_size=200_000):
            tbl = pa.Table.from_batches([batch])
            tbl = tbl.rename_columns([c.lower() for c in tbl.column_names])
            tbl = tbl.append_column("source_month", pa.array([month] * tbl.num_rows))

            buf = io.BytesIO()
            pacsv.write_csv(tbl, buf, pacsv.WriteOptions(include_header=False))

            with cur.copy(copy_sql) as cp:
                cp.write(buf.getvalue())

            total += tbl.num_rows
            print(f"  da nap {total:,} dong")

print(f"Xong: nap {total:,} dong, file co {pf.metadata.num_rows:,} dong")