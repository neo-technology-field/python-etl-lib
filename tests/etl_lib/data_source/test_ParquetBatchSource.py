import pytest
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from etl_lib.data_source.ParquetBatchSource import ParquetBatchSource
from etl_lib.test_utils.utils import DummyContext


def _write_parquet(path: Path, data: dict):
    table = pa.Table.from_pydict(data)
    pq.write_table(table, path)


def test_parquet_batch_source_read(tmp_path):
    parquet_file = tmp_path / "test.parquet"
    data = {
        "col1": list(range(10)),
        "col2": [f"val_{i}" for i in range(10)]
    }
    _write_parquet(parquet_file, data)

    # Test reading with small batch size
    source = ParquetBatchSource(parquet_file, DummyContext())
    batches = list(source.get_batch(max_batch_size=3))

    # Expected: 10 rows / 3 = 3 batches of 3, plus 1 batch of 1 = 4 batches
    assert len(batches) == 4
    
    all_rows = []
    for batch in batches:
        all_rows.extend(batch.chunk)
        assert batch.statistics["parquet_rows_read"] == len(batch.chunk)

    assert len(all_rows) == 10
    
    # Check content and _row index
    for i, row in enumerate(all_rows):
        assert row["col1"] == i
        assert row["col2"] == f"val_{i}"
        assert row["_row"] == i


def test_parquet_get_total_rows(tmp_path):
    parquet_file = tmp_path / "test_count.parquet"
    data = {"col1": list(range(100))}
    _write_parquet(parquet_file, data)

    assert ParquetBatchSource.get_total_rows(parquet_file) == 100


def test_parquet_missing_file():
    with pytest.raises(FileNotFoundError):
        ParquetBatchSource.get_total_rows(Path("non_existent.parquet"))
