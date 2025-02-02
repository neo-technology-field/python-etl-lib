import csv
from pathlib import Path

import pytest

from etl_lib.core.BatchProcessor import BatchResults
from etl_lib.data_source.CSVBatchProcessor import CSVBatchProcessor
from test_utils.utils import DummyContext
from test_utils.utils import get_test_file

TEST_FILES = [
    (get_test_file("coma-double-quotes.csv"), {"delimiter": ",", "quotechar": '"', "escapechar": "\\"}),
    (get_test_file("semi-single-quotes.csv"), {"delimiter": ";", "quotechar": "'", "escapechar": "\\"}),
    (get_test_file("tab-no-quotes.csv"), {"delimiter": "\t", "quotechar": '"', "escapechar": "\\"})
]

EXPECTED_DATA = [
    {"string": "Hello, World!", "integer": "42", "float": "3.14"},
    {"string": "Ümläuts & Accénts", "integer": "-1", "float": "-0.01"},
    {"string": "Escape \\tTab", "integer": "999999", "float": "1000000.0"},
]


@pytest.mark.parametrize("csv_file, csv_reader_options", TEST_FILES)
def test_csv_batch_processor(csv_file, csv_reader_options):
    """Test CSVBatchProcessor passing and using specific csv reader options."""

    processor = CSVBatchProcessor(csv_file=csv_file, context=DummyContext(), **csv_reader_options)

    batch_size = len(EXPECTED_DATA)
    batches = list(processor.get_batch(batch_size))

    assert len(batches) == 1, "Expected a single batch"

    batch: BatchResults = batches[0]

    expected_with_row = [
        {**row, "_row": idx} for idx, row in enumerate(EXPECTED_DATA)
    ]

    assert batch.chunk == expected_with_row, f"Mismatch in batch chunk: {batch.chunk}"

    for row in batch.chunk:
        assert isinstance(row["_row"], int), f"_row should be int, got {type(row['_row'])} in {row}"

    # Validate statistics field (should be a dictionary)
    assert batch.statistics == {'csv_lines_read': 3}, "Expected statistics to be a dictionary"

    assert batch.batch_size == batch_size, f"Expected batch_size to be {batch_size}, got {batch.batch_size}"


@pytest.mark.parametrize("batch_size", [2, 5])
def test_csv_batch_processor_batching(tmp_path: Path, batch_size):
    """Verify reading in batches."""
    csv_reader_options = {"delimiter": ",", "quotechar": '"', "escapechar": "\\"}
    total_rows = 10
    csv_path = tmp_path / "test_large.csv"

    fieldnames = ["string", "integer", "float"]
    with csv_path.open(mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, **csv_reader_options)
        writer.writeheader()
        for i in range(total_rows):
            writer.writerow({
                "string": f"Row {i}",
                "integer": str(i),
                "float": str(float(i) * 1.1)
            })

    processor = CSVBatchProcessor(csv_file=csv_path, context=DummyContext(), **csv_reader_options)

    yield_count = 0
    all_batches = []
    for batch in processor.get_batch(batch_size):
        all_batches.append(batch)
        yield_count += 1

    expected_batches = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)
    assert yield_count == expected_batches, f"Expected {expected_batches} yields, got {yield_count}"

    all_rows = [row for batch in all_batches for row in batch.chunk]
    assert len(all_rows) == total_rows, f"Expected {total_rows} rows processed, got {len(all_rows)}"
    assert [row["_row"] for row in all_rows] == list(range(total_rows)), "Row indices mismatch"
