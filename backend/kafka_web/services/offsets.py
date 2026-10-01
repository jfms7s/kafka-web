"""Pure start-offset planning for snapshot consumption (spec §3)."""

import math
from typing import Literal

StartMode = Literal["earliest", "latest", "offset", "timestamp"]
Watermarks = dict[int, tuple[int, int]]  # partition -> (low, high)


def _has_data(marks: Watermarks) -> Watermarks:
    return {p: (low, high) for p, (low, high) in marks.items() if high > low}


def plan_start_offsets(
    mode: StartMode,
    watermarks: Watermarks,
    count: int,
    *,
    offset: int | None = None,
    timestamp_offsets: dict[int, int] | None = None,
) -> dict[int, int]:
    """Partition -> start offset, omitting partitions with nothing to read (start >= high)."""
    match mode:
        case "earliest":
            starts = {p: low for p, (low, _) in watermarks.items()}
        case "latest":
            non_empty = _has_data(watermarks)
            if not non_empty:
                return {}
            per_partition = math.ceil(count / len(non_empty))
            starts = {p: max(low, high - per_partition) for p, (low, high) in non_empty.items()}
        case "offset":
            if offset is None:
                raise ValueError("start=offset needs an offset")
            if len(watermarks) != 1:
                raise ValueError("start=offset needs exactly one partition")
            ((p, (low, high)),) = watermarks.items()
            starts = {p: min(max(offset, low), high)}
        case "timestamp":
            if timestamp_offsets is None:
                raise ValueError("start=timestamp needs the offsets-for-times lookup")
            starts = {p: o for p, o in timestamp_offsets.items() if o >= 0 and p in watermarks}
    return {p: start for p, start in starts.items() if start < watermarks[p][1]}


def end_offsets(watermarks: Watermarks) -> dict[int, int]:
    """Partition -> high watermark: where a snapshot stops reading."""
    return {p: high for p, (_, high) in watermarks.items()}


def is_done(positions: dict[int, int], ends: dict[int, int]) -> bool:
    """True once every planned partition's next offset has reached its end."""
    return all(positions.get(p, 0) >= end for p, end in ends.items())
