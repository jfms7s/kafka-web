import pytest

from kafka_web.services.offsets import end_offsets, is_done, plan_start_offsets


def test_earliest_starts_at_low_and_omits_empty_partitions() -> None:
    marks = {0: (5, 20), 1: (0, 0), 2: (3, 4)}

    assert plan_start_offsets("earliest", marks, 100) == {0: 5, 2: 3}


def test_latest_splits_count_across_non_empty_partitions() -> None:
    marks = {0: (0, 100), 1: (0, 3), 2: (5, 5)}

    # P = 2 (partition 2 is empty), per = ceil(10 / 2) = 5
    assert plan_start_offsets("latest", marks, 10) == {0: 95, 1: 0}


def test_latest_all_partitions_empty() -> None:
    assert plan_start_offsets("latest", {0: (0, 0), 1: (7, 7)}, 10) == {}


def test_latest_no_partitions() -> None:
    assert plan_start_offsets("latest", {}, 10) == {}


def test_latest_rounds_per_partition_up() -> None:
    marks = {0: (0, 50), 1: (0, 50), 2: (0, 50)}

    # ceil(10 / 3) = 4
    assert plan_start_offsets("latest", marks, 10) == {0: 46, 1: 46, 2: 46}


def test_latest_count_larger_than_total_starts_at_low() -> None:
    marks = {0: (2, 6), 1: (0, 3)}

    assert plan_start_offsets("latest", marks, 1000) == {0: 2, 1: 0}


def test_offset_within_range() -> None:
    assert plan_start_offsets("offset", {3: (0, 10)}, 5, offset=4) == {3: 4}


def test_offset_below_low_is_clamped_to_low() -> None:
    assert plan_start_offsets("offset", {0: (5, 10)}, 5, offset=1) == {0: 5}


def test_offset_above_high_is_omitted() -> None:
    assert plan_start_offsets("offset", {0: (5, 10)}, 5, offset=99) == {}


def test_offset_equal_to_high_is_omitted() -> None:
    assert plan_start_offsets("offset", {0: (5, 10)}, 5, offset=10) == {}


def test_offset_requires_exactly_one_partition() -> None:
    with pytest.raises(ValueError):
        plan_start_offsets("offset", {0: (0, 10), 1: (0, 10)}, 5, offset=1)


def test_offset_requires_an_offset() -> None:
    with pytest.raises(ValueError):
        plan_start_offsets("offset", {0: (0, 10)}, 5)


def test_timestamp_uses_looked_up_offsets() -> None:
    marks = {0: (0, 10), 1: (0, 10)}

    assert plan_start_offsets("timestamp", marks, 5, timestamp_offsets={0: 4, 1: 9}) == {0: 4, 1: 9}


def test_timestamp_minus_one_is_omitted() -> None:
    marks = {0: (0, 10), 1: (0, 10)}

    assert plan_start_offsets("timestamp", marks, 5, timestamp_offsets={0: -1, 1: 2}) == {1: 2}


def test_timestamp_offset_at_or_after_high_is_omitted() -> None:
    assert plan_start_offsets("timestamp", {0: (0, 10)}, 5, timestamp_offsets={0: 10}) == {}


def test_timestamp_requires_lookup() -> None:
    with pytest.raises(ValueError):
        plan_start_offsets("timestamp", {0: (0, 10)}, 5)


def test_end_offsets_are_the_high_watermarks() -> None:
    assert end_offsets({0: (0, 10), 1: (3, 3)}) == {0: 10, 1: 3}


def test_is_done_when_every_partition_reached_its_end() -> None:
    assert is_done({0: 10, 1: 7}, {0: 10, 1: 7}) is True


def test_is_done_false_while_any_partition_is_behind() -> None:
    assert is_done({0: 10, 1: 6}, {0: 10, 1: 7}) is False


def test_is_done_with_nothing_planned() -> None:
    assert is_done({}, {}) is True
