import random
from typing import Counter, List, Tuple

import pytest
from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.SplittingBatchProcessor import SplittingBatchProcessor, dict_id_extractor, tuple_id_extractor


class SingleBatchPredecessor(BatchProcessor):
    def __init__(self, chunk, statistics=None):
        super().__init__(context=None, task=None, predecessor=None)
        self._chunk = chunk
        self._statistics = statistics or {}
        self._done = False

    def get_batch(self, max_batch_size: int):
        if not self._done:
            self._done = True
            yield BatchResults(
                chunk=self._chunk,
                statistics=self._statistics,
                batch_size=len(self._chunk),
            )


class MultiBatchPredecessor(BatchProcessor):
    def __init__(self, batches):
        super().__init__(context=None, task=None, predecessor=None)
        self._batches = batches

    def get_batch(self, max_batch_size: int):
        for chunk, statistics in self._batches:
            yield BatchResults(
                chunk=chunk,
                statistics=statistics or {},
                batch_size=len(chunk),
            )


def test_tuple_id_extractor_with_strings_and_ints():
    extractor = tuple_id_extractor()
    # string inputs
    assert extractor(('123', '456')) == (3, 6)
    assert extractor(('1001', '1002')) == (1, 2)
    assert extractor(('019', '020')) == (9, 0)
    # integer inputs
    assert extractor((123, 456)) == (3, 6)
    assert extractor((1001, 1002)) == (1, 2)
    assert extractor((19, 20)) == (9, 0)


def test_tuple_id_extractor_modulo_behavior():
    extractor = tuple_id_extractor()
    assert extractor(('999', '1000')) == (9, 0)
    assert extractor((908, 1009)) == (8, 9)


def test_dict_id_extractor_with_strings_and_ints():
    extractor = dict_id_extractor()
    # string values
    assert extractor({'start': '123', 'end': '456'}) == (3, 6)
    assert extractor({'start': '1001', 'end': '1002'}) == (1, 2)
    # integer values
    assert extractor({'start': 123, 'end': 456}) == (3, 6)
    assert extractor({'start': 1001, 'end': 1002}) == (1, 2)


def test_dict_id_extractor_missing_keys():
    extractor = dict_id_extractor()
    with pytest.raises(KeyError):
        extractor({'start': '123'})
    with pytest.raises(KeyError):
        extractor({'end': '456'})


class DummyProcessor(SplittingBatchProcessor):
    def __init__(self, predecessor, table_size=3):
        super().__init__(context=None, task=None, predecessor=predecessor,
                         table_size=table_size, id_extractor=tuple_id_extractor(table_size))

    def extract_ids(self, item: Tuple[str, str]) -> Tuple[int, int]:
        return int(item[0][-1]) % self.table_size, int(item[1][-1]) % self.table_size


class DummyPredecessor:
    def __init__(self, items: List[Tuple[str, str]]):
        self._items = items

    def get_batch(self, batch_size: int):
        for i in range(0, len(self._items), batch_size):
            chunk = self._items[i:i + batch_size]
            yield BatchResults(
                chunk=chunk,
                statistics={},
                batch_size=len(chunk)
            )


def test_generate_batch_schedule():
    proc = DummyProcessor(predecessor=DummyPredecessor([]), table_size=3)
    schedule = proc._generate_batch_schedule()
    assert schedule == [
        [(0, 0), (1, 1), (2, 2)],
        [(0, 1), (1, 2), (2, 0)],
        [(0, 2), (1, 0), (2, 1)]
    ]


def test_get_batch_with_batch_size_two_and_shuffled_input():
    batch_size = 2
    table_size = 3
    # group 0 is filled with 2 batches, making it full.
    # in addition, (0,0) has one more entry that must be sent out in the end.
    items_group0 = [
        ('120', '210'), ('130', '230'),  # cell (0,0)
        ('130', '410'), ('230', '330'),  # 2 full batches
        ('ex10', 'ex20'),
        ('431', '871'), ('511', '981'),  # cell (1,1)
        ('441', '971'), ('111', '381'),  # 2 full batches
        ('542', '592'), ('342', '122'),  # cell (2,2)
        ('642', '692'), ('442', '322')  # 2 full batches
    ]
    # group one is complete to send.
    items_group1 = [
        ('110', '111'), ('030', '031'),  # cell (0,1) 1 full batch
        ('111', '112'), ('411', '412'),  # cell (1,2) 1 full batch
        ('222', '220'), ('522', '520')  # cell (2,0) 1 full batch
    ]
    items_group2_partial = [
        ('222', '221')  # cell (2,1) leftover
    ]

    # Combine all items
    input_items = items_group0 + items_group1 + items_group2_partial

    # Shuffle
    random.seed(42)
    random.shuffle(input_items)

    predecessor = DummyPredecessor(input_items)
    proc = DummyProcessor(predecessor=predecessor, table_size=table_size)

    batches = list(proc.get_batch(batch_size))
    # each entry is a [[], [], ..]
    # with the outer one group of the schedules
    # we expect group0 2x, group1 1x and the rest
    # Expect 4 outer batches:
    #   - 2 for group0 full,
    #   - 1 for group1 full,
    #   - 2 for leftovers (group0 leftover + group2_partial)
    assert len(batches) == 5

    # Flatten all items emitted across all batches
    all_emitted = []
    for br in batches:
        assert isinstance(br, BatchResults)
        # each chunk is a list of per-cell-lists
        for cell_items in br.chunk:
            all_emitted.extend(cell_items)

    # Check multiset equality with original input
    assert Counter(all_emitted) == Counter(input_items), (
        "Some items were lost or duplicated in the batching process"
    )

    # total count matches
    assert len(all_emitted) == len(input_items), (
        f"Expected {len(input_items)} items in total, got {len(all_emitted)}"
    )
    # Each batch must consist of items from a single diagonal group
    for b in batches:
        shifts = set()
        # compute shift per item based on last digit
        for cell in b.chunk:
            for item in cell:
                row_digit = int(item[0][-1])
                col_digit = int(item[1][-1])
                shift = (col_digit - row_digit) % table_size
                shifts.add(shift)
        assert len(shifts) == 1, f"Batch contains mixed schedule groups: {shifts}"


def test_id_extractor_table_size_mismatch():
    """If id_extractor's implied table size doesn't match the provided table_size, constructor should raise."""

    class DummyMismatchProcessor(SplittingBatchProcessor):
        def __init__(self, predecessor):
            # tuple_id_extractor() defaults to table_size=10, but we pass table_size=5 -> mismatch
            super().__init__(context=None, table_size=5, id_extractor=tuple_id_extractor(), predecessor=predecessor)

    # Predecessor can be empty; we expect init to fail fast
    with pytest.raises(ValueError):
        DummyMismatchProcessor(predecessor=DummyPredecessor([]))


def test_out_of_range_id_raises():
    """If an item produces an out-of-range (row, col), SplittingBatchProcessor should raise an error."""

    class DummyProcessorTableTest(SplittingBatchProcessor):
        def __init__(self, predecessor, table_size=3):
            super().__init__(context=None, task=None, predecessor=predecessor,
                             table_size=table_size, id_extractor=tuple_id_extractor(table_size))

    # Use DummyProcessorTableTest (with table_size=3) but feed it an item ending in a digit outside 0-2 range.
    items = [('123', '987')]  # '123' -> last digit 3, '987' -> last digit 7, expecting (3,7)
    predecessor = DummyPredecessor(items)
    # DummyProcessorTableTest is configured with tuple_id_extractor(table_size=3) internally in its __init__
    proc = DummyProcessorTableTest(predecessor=predecessor, table_size=3)
    with pytest.raises(ValueError) as excinfo:
        next(proc.get_batch(1))
    # The error message should mention out-of-range partition ID
    assert "out of range" in str(excinfo.value)


def test_splitting_last_batch_carries_accumulated_stats():
    incoming_stats = {"processed": 6, "valid_rows": 5, "nodes_created": 2}
    # Six tuples in range for table_size=3
    chunk = [(0, 0), (1, 1), (2, 2), (0, 1), (1, 2), (2, 0)]
    predecessor = SingleBatchPredecessor(chunk=chunk, statistics=incoming_stats)

    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=3,
        id_extractor=lambda rc: rc
    )

    outs = list(splitter.get_batch(2))
    assert len(outs) >= 1

    total_emitted = sum(sum(len(cell) for cell in br.chunk) for br in outs)
    assert total_emitted == len(chunk)

    non_empty_stats_indices = [i for i, br in enumerate(outs) if br.statistics]
    assert len(non_empty_stats_indices) == 1
    assert non_empty_stats_indices[0] == len(outs) - 1

    last = outs[-1]
    assert last.statistics == incoming_stats


def test_splitting_no_upstream_stats_means_no_stats_emitted():
    chunk = [(0, 0), (0, 1), (1, 0), (1, 1)]
    predecessor = SingleBatchPredecessor(chunk=chunk)

    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=2,
        id_extractor=lambda rc: rc
    )

    outs = list(splitter.get_batch(2))
    assert len(outs) >= 1

    # No upstream stats -> no stats in any emitted batch
    assert all(not br.statistics for br in outs)


def test_splitting_merges_numeric_stats_additively_and_nested_dicts():
    batch1 = (
        [(0, 0), (1, 1)],
        {"valid_rows": 2, "nodes_created": 1},
    )
    batch2 = (
        [(2, 2), (0, 1), (1, 2)],
        {"valid_rows": 3, "nodes_created": 2},
    )

    predecessor = MultiBatchPredecessor([batch1, batch2])

    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=3,
        id_extractor=lambda rc: rc
    )

    outs = list(splitter.get_batch(2))
    assert len(outs) >= 2

    non_empty_stats_indices = [i for i, br in enumerate(outs) if br.statistics]
    assert len(non_empty_stats_indices) == 1
    assert non_empty_stats_indices[0] == len(outs) - 1

    last = outs[-1]
    # Accumulated totals from both upstream batches (no filtering)
    assert last.statistics.get("valid_rows") == 5
    assert last.statistics.get("nodes_created") == 3
