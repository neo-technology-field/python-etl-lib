import random
from collections import Counter
from typing import Any, Dict, List, Tuple

import pytest

from etl_lib.core.BatchProcessor import BatchProcessor, BatchResults
from etl_lib.core.SplittingBatchProcessor import (
    SplittingBatchProcessor,
    canonical_integer_id_extractor,
    dict_id_extractor,
    tuple_id_extractor,
)


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


class DummyPredecessor:
    def __init__(self, items: List[Any]):
        self._items = items

    def get_batch(self, batch_size: int):
        for i in range(0, len(self._items), batch_size):
            chunk = self._items[i:i + batch_size]
            yield BatchResults(
                chunk=chunk,
                statistics={},
                batch_size=len(chunk),
            )


def _assert_wave_non_overlap_bipartite(br: BatchResults, id_extractor) -> None:
    bucket_ids: List[Tuple[int, int]] = []
    for bucket_batch in br.chunk:
        assert bucket_batch, "Empty bucket batch emitted"
        r0, c0 = id_extractor(bucket_batch[0])
        for item in bucket_batch[1:]:
            assert id_extractor(item) == (r0, c0), "Bucket batch contains mixed bucket ids"
        bucket_ids.append((r0, c0))

    rows = [r for r, _ in bucket_ids]
    cols = [c for _, c in bucket_ids]
    assert len(rows) == len(set(rows)), f"Row overlap in wave: {bucket_ids}"
    assert len(cols) == len(set(cols)), f"Col overlap in wave: {bucket_ids}"


def _assert_wave_non_overlap_monopartite(br: BatchResults, id_extractor) -> None:
    bucket_ids: List[Tuple[int, int]] = []
    for bucket_batch in br.chunk:
        assert bucket_batch, "Empty bucket batch emitted"
        r0, c0 = id_extractor(bucket_batch[0])
        for item in bucket_batch[1:]:
            assert id_extractor(item) == (r0, c0), "Bucket batch contains mixed bucket ids"
        bucket_ids.append((r0, c0))

    claims: List[int] = []
    for r, c in bucket_ids:
        if r == c:
            claims.append(r)
        else:
            claims.extend([r, c])

    assert len(claims) == len(set(claims)), f"Node-index overlap in mono-partite wave: {bucket_ids}"


def test_tuple_id_extractor_with_strings_and_ints():
    extractor = tuple_id_extractor()
    assert extractor(("123", "456")) == (3, 6)
    assert extractor(("1001", "1002")) == (1, 2)
    assert extractor(("019", "020")) == (9, 0)
    assert extractor((123, 456)) == (3, 6)
    assert extractor((1001, 1002)) == (1, 2)
    assert extractor((19, 20)) == (9, 0)


def test_tuple_id_extractor_modulo_behavior():
    extractor = tuple_id_extractor()
    assert extractor(("999", "1000")) == (9, 0)
    assert extractor((908, 1009)) == (8, 9)


def test_dict_id_extractor_with_strings_and_ints():
    extractor = dict_id_extractor()
    assert extractor({"start": "123", "end": "456"}) == (3, 6)
    assert extractor({"start": "1001", "end": "1002"}) == (1, 2)
    assert extractor({"start": 123, "end": 456}) == (3, 6)
    assert extractor({"start": 1001, "end": 1002}) == (1, 2)


def test_dict_id_extractor_missing_keys():
    extractor = dict_id_extractor()
    with pytest.raises(KeyError):
        extractor({"start": "123"})
    with pytest.raises(KeyError):
        extractor({"end": "456"})


def test_id_extractor_table_size_mismatch():
    class DummyMismatchProcessor(SplittingBatchProcessor):
        def __init__(self, predecessor):
            super().__init__(context=None, table_size=5, id_extractor=tuple_id_extractor(), predecessor=predecessor)

    with pytest.raises(ValueError):
        DummyMismatchProcessor(predecessor=DummyPredecessor([]))


def test_out_of_range_id_raises():
    class DummyProcessorTableTest(SplittingBatchProcessor):
        def __init__(self, predecessor, table_size=3):
            super().__init__(
                context=None,
                task=None,
                predecessor=predecessor,
                table_size=table_size,
                id_extractor=tuple_id_extractor(table_size),
            )

    items = [("123", "987")]
    predecessor = DummyPredecessor(items)
    proc = DummyProcessorTableTest(predecessor=predecessor, table_size=3)

    with pytest.raises(ValueError) as excinfo:
        next(proc.get_batch(1))

    assert "out of range" in str(excinfo.value)


def test_select_wave_picks_non_overlapping_buckets_and_is_deterministic_for_equal_sizes():
    class Proc(SplittingBatchProcessor):
        def __init__(self):
            super().__init__(context=None, task=None, predecessor=DummyPredecessor([]), table_size=3, id_extractor=lambda x: x)

    proc = Proc()

    for r in range(3):
        for c in range(3):
            proc.buffer[r][c] = [(r, c)]

    wave = proc._select_wave(min_bucket_len=1)
    assert wave == [(0, 0), (1, 1), (2, 2)]


def test_get_batch_with_batch_size_2_and_shuffled_input_non_overlap_per_wave():
    batch_size = 2
    table_size = 3

    items_group0 = [
        ("120", "210"), ("130", "230"),
        ("130", "410"), ("230", "330"),
        ("ex10", "ex20"),
        ("431", "871"), ("511", "981"),
        ("441", "971"), ("111", "381"),
        ("542", "592"), ("342", "122"),
        ("642", "692"), ("442", "322"),
    ]
    items_group1 = [
        ("110", "111"), ("030", "031"),
        ("111", "112"), ("411", "412"),
        ("222", "220"), ("522", "520"),
    ]
    items_group2_partial = [
        ("222", "221"),
    ]

    input_items = items_group0 + items_group1 + items_group2_partial

    random.seed(42)
    random.shuffle(input_items)

    predecessor = DummyPredecessor(input_items)
    proc = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=table_size,
        id_extractor=tuple_id_extractor(table_size),
    )

    batches = list(proc.get_batch(batch_size))
    assert batches, "No batches emitted"

    all_emitted: List[Tuple[str, str]] = []
    for br in batches:
        assert isinstance(br, BatchResults)
        for bucket_batch in br.chunk:
            all_emitted.extend(bucket_batch)

    assert Counter(all_emitted) == Counter(input_items)
    assert len(all_emitted) == len(input_items)

    id_ex = tuple_id_extractor(table_size)
    for br in batches:
        _assert_wave_non_overlap_bipartite(br, id_ex)
        for bucket_batch in br.chunk:
            assert len(bucket_batch) <= batch_size


def test_each_bucket_batch_is_pure_single_bucket():
    items = [
        ("120", "210"),
        ("121", "211"),
        ("130", "230"),
        ("131", "231"),
    ]
    predecessor = DummyPredecessor(items)

    table_size = 3
    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=table_size,
        id_extractor=tuple_id_extractor(table_size),
    )

    outs = list(splitter.get_batch(2))
    assert outs

    id_ex = tuple_id_extractor(table_size)
    for br in outs:
        for bucket_batch in br.chunk:
            assert bucket_batch
            r0, c0 = id_ex(bucket_batch[0])
            assert all(id_ex(it) == (r0, c0) for it in bucket_batch)


def test_monopartite_extractor_enforces_single_node_index_claims_per_wave():
    table_size = 5
    extractor = canonical_integer_id_extractor(table_size=table_size, start_key="start", end_key="end")

    items: List[Dict[str, int]] = []
    for a in range(200):
        items.append({"start": a, "end": a})
        items.append({"start": a, "end": a + 1})

    predecessor = DummyPredecessor(items)
    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=table_size,
        id_extractor=extractor,
    )

    outs = list(splitter.get_batch(20))
    assert outs

    for br in outs:
        _assert_wave_non_overlap_monopartite(br, extractor)


def test_splitting_last_batch_carries_accumulated_stats():
    incoming_stats = {"processed": 6, "valid_rows": 5, "nodes_created": 2}
    chunk = [(0, 0), (1, 1), (2, 2), (0, 1), (1, 2), (2, 0)]
    predecessor = SingleBatchPredecessor(chunk=chunk, statistics=incoming_stats)

    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=3,
        id_extractor=lambda rc: rc,
    )

    outs = list(splitter.get_batch(2))
    assert outs

    total_emitted = sum(sum(len(bucket_batch) for bucket_batch in br.chunk) for br in outs)
    assert total_emitted == len(chunk)

    non_empty_stats_indices = [i for i, br in enumerate(outs) if br.statistics]
    assert non_empty_stats_indices == [len(outs) - 1]

    assert outs[-1].statistics == incoming_stats


def test_splitting_no_upstream_stats_means_no_stats_emitted():
    chunk = [(0, 0), (0, 1), (1, 0), (1, 1)]
    predecessor = SingleBatchPredecessor(chunk=chunk)

    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=2,
        id_extractor=lambda rc: rc,
    )

    outs = list(splitter.get_batch(2))
    assert outs
    assert all(not br.statistics for br in outs)


def test_splitting_merges_numeric_stats_additively_and_nested_dicts():
    batch1 = ([(0, 0), (1, 1)], {"valid_rows": 2, "nodes_created": 1})
    batch2 = ([(2, 2), (0, 1), (1, 2)], {"valid_rows": 3, "nodes_created": 2})

    predecessor = MultiBatchPredecessor([batch1, batch2])

    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=3,
        id_extractor=lambda rc: rc,
    )

    outs = list(splitter.get_batch(2))
    assert len(outs) >= 2

    non_empty_stats_indices = [i for i, br in enumerate(outs) if br.statistics]
    assert non_empty_stats_indices == [len(outs) - 1]

    last = outs[-1]
    assert last.statistics.get("valid_rows") == 5
    assert last.statistics.get("nodes_created") == 3


def test_skewed_hotspot_single_bucket_emits_multiple_batches_and_never_includes_other_buckets():
    table_size = 4
    max_batch_size = 3
    target_bucket = (1, 2)

    items = [(f"row{i}1", f"col{i}2") for i in range(10)]
    predecessor = DummyPredecessor(items)

    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=table_size,
        id_extractor=tuple_id_extractor(table_size),
    )

    outs = list(splitter.get_batch(max_batch_size))
    assert len(outs) == 4

    id_ex = tuple_id_extractor(table_size)

    all_emitted: List[Tuple[str, str]] = []
    for br in outs:
        assert len(br.chunk) == 1
        bucket_batch = br.chunk[0]
        assert 1 <= len(bucket_batch) <= max_batch_size
        for it in bucket_batch:
            assert id_ex(it) == target_bucket
        all_emitted.extend(bucket_batch)

    assert Counter(all_emitted) == Counter(items)


def test_many_full_buckets_more_than_table_size_emits_multiple_waves_and_maintains_non_overlap():
    table_size = 3
    max_batch_size = 2

    buckets = [(0, 0), (0, 1), (1, 2), (2, 0), (2, 2)]
    items: List[Tuple[str, str]] = []
    for r, c in buckets:
        for i in range(max_batch_size):
            items.append((f"r{r}_{i}{r}", f"c{c}_{i}{c}"))

    random.seed(7)
    random.shuffle(items)

    predecessor = DummyPredecessor(items)

    splitter = SplittingBatchProcessor(
        context=None,
        task=None,
        predecessor=predecessor,
        table_size=table_size,
        id_extractor=tuple_id_extractor(table_size),
        near_full_ratio=1.0,
        burst_multiplier=10_000,
    )

    outs = list(splitter.get_batch(max_batch_size))
    assert len(outs) >= 2

    id_ex = tuple_id_extractor(table_size)

    all_emitted: List[Tuple[str, str]] = []
    for br in outs:
        _assert_wave_non_overlap_bipartite(br, id_ex)
        for bucket_batch in br.chunk:
            assert len(bucket_batch) <= max_batch_size
            all_emitted.extend(bucket_batch)

    assert Counter(all_emitted) == Counter(items)


def test_monopartite_folding_swapped_pairs_land_in_same_bucket_and_row_leq_col():
    table_size = 17
    extractor = canonical_integer_id_extractor(table_size=table_size, start_key="start", end_key="end")

    pairs = [
        (0, 1),
        (1, 0),
        (2, 9),
        (9, 2),
        (123, 456),
        (456, 123),
        (10_001, 10_002),
        (10_002, 10_001),
    ]

    for a, b in pairs:
        r1, c1 = extractor({"start": a, "end": b})
        r2, c2 = extractor({"start": b, "end": a})
        assert (r1, c1) == (r2, c2)
        assert r1 <= c1
        assert 0 <= r1 < table_size
        assert 0 <= c1 < table_size


def test_order_agnostic_correctness_randomized_inputs_multiset_preserved_and_non_overlap_per_wave():
    table_size = 5
    max_batch_size = 10
    seeds = [0, 1, 2, 42, 99]

    id_ex = tuple_id_extractor(table_size)

    for seed in seeds:
        rng = random.Random(seed)

        items: List[Tuple[str, str]] = []
        for i in range(200):
            r = rng.randrange(table_size)
            c = rng.randrange(table_size)
            items.append((f"a{seed}_{i}{r}", f"b{seed}_{i}{c}"))

        rng.shuffle(items)

        predecessor = DummyPredecessor(items)
        splitter = SplittingBatchProcessor(
            context=None,
            task=None,
            predecessor=predecessor,
            table_size=table_size,
            id_extractor=id_ex,
        )

        outs = list(splitter.get_batch(max_batch_size))
        assert outs

        all_emitted: List[Tuple[str, str]] = []
        for br in outs:
            _assert_wave_non_overlap_bipartite(br, id_ex)
            for bucket_batch in br.chunk:
                assert 1 <= len(bucket_batch) <= max_batch_size
                r0, c0 = id_ex(bucket_batch[0])
                assert all(id_ex(it) == (r0, c0) for it in bucket_batch)
                all_emitted.extend(bucket_batch)

        assert Counter(all_emitted) == Counter(items)
