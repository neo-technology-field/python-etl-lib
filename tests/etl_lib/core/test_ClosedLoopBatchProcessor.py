import pytest

from etl_lib.core.BatchProcessor import BatchResults
from etl_lib.core.ClosedLoopBatchProcessor import ClosedLoopBatchProcessor
from etl_lib.test_utils.utils import DummyContext, DummyPredecessor


def _make_processor(batches, expected_rows=None, expected_batches=None):
    predecessor = DummyPredecessor(batches)
    return ClosedLoopBatchProcessor(
        DummyContext(),
        task=None,
        predecessor=predecessor,
        expected_rows=expected_rows,
        expected_batches=expected_batches,
    )


class TestSafeCalculateCount:
    """_safe_calculate_count must return expected batch count, not row count."""

    def test_exact_division(self):
        proc = _make_processor([], expected_rows=1000)
        assert proc._safe_calculate_count(100) == 10

    def test_ceiling_division(self):
        # 1001 rows / 100 batch_size → ceiling = 11 batches
        proc = _make_processor([], expected_rows=1001)
        assert proc._safe_calculate_count(100) == 11

    def test_single_row(self):
        proc = _make_processor([], expected_rows=1)
        assert proc._safe_calculate_count(500) == 1

    def test_expected_batches_takes_priority(self):
        proc = _make_processor([], expected_rows=9999, expected_batches=7)
        assert proc._safe_calculate_count(100) == 7

    def test_zero_expected_rows_returns_zero(self):
        proc = _make_processor([])
        assert proc._safe_calculate_count(100) == 0

    def test_zero_batch_size_returns_zero(self):
        proc = _make_processor([], expected_rows=1000)
        assert proc._safe_calculate_count(0) == 0


class TestGetBatch:

    def test_requires_predecessor(self):
        proc = ClosedLoopBatchProcessor(DummyContext(), task=None, predecessor=None)
        with pytest.raises(ValueError):
            next(proc.get_batch(100))

    def test_merges_statistics_across_batches(self):
        batches = [
            BatchResults(chunk=[1, 2], statistics={"rows": 2}, batch_size=2),
            BatchResults(chunk=[3], statistics={"rows": 1}, batch_size=1),
        ]
        proc = _make_processor(batches)
        result = next(proc.get_batch(100))
        assert result.statistics == {"rows": 3}
