import sys
import pathlib
import requests

month = sys.argv[1]
url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
out = pathlib.Path("data/raw") / f"yellow_tripdata_{month}.parquet"

if out.exists():
    print(f"Da co {out}, bo qua")
    sys.exit(0)

print(f"Dang tai {url}")
with requests.get(url, stream=True, timeout=60) as r:
    r.raise_for_status()
    with open(out, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)

print(f"Xong: {out} ({out.stat().st_size / 1e6:.1f} MB)")