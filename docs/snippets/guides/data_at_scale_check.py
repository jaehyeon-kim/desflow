"""The check of the Data at Scale page: count the rows and the files of each day.

Run it from the folder of the files with `python data_at_scale_check.py [folder]
[customers] [days]`. It reads every Parquet file under the folder, prints the rows
and the files of each day, and stops with an error when a day has no file or the
total is far from the volume the model should give.
"""

# --8<-- [start:check]
import sys
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq


def check(folder: str, customers: int, days: int) -> int:
    """Prints the files and rows of each day and returns the total rows."""
    rows: Counter = Counter()
    files: Counter = Counter()
    for path in sorted(Path(folder).glob("*.parquet")):
        day = path.name[:10]  # the day in the file name, such as 2026-01-01
        files[day] += 1
        rows[day] += pq.ParquetFile(path).metadata.num_rows
    for day in sorted(rows):
        print(f"{day}  {files[day]} file  {rows[day]:>7} rows")
    # Each customer orders once a day on average. Its first order comes 12 to 36 hours
    # in, so the total is about half a day short of customers * days.
    expected = customers * (days - 0.46)
    total = sum(rows.values())
    print(f"total {total} rows, expected about {expected:.0f}")
    assert len(rows) == days, f"{len(rows)} days have a file, expected {days}"
    assert abs(total - expected) < 0.1 * expected, (
        "the total is far from the expected volume"
    )
    return total


if __name__ == "__main__":
    folder = sys.argv[1] if len(sys.argv) > 1 else "scale_output"
    customers = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    days = int(sys.argv[3]) if len(sys.argv) > 3 else 30
    check(folder, customers, days)
# --8<-- [end:check]
