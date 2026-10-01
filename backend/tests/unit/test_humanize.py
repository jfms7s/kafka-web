import pytest

from kafka_web.services.humanize import humanize_bytes, humanize_config_value, humanize_ms


@pytest.mark.parametrize(
    ("ms", "expected"),
    [
        (-1, "unlimited"),
        (0, "0ms"),
        (1, "1ms"),
        (1500, "1s 500ms"),
        (60_000, "1m"),
        (3_600_000, "1h"),
        (604_800_000, "7d"),
        (90_061_000, "1d 1h 1m 1s"),
    ],
)
def test_humanize_ms(ms: int, expected: str) -> None:
    assert humanize_ms(ms) == expected


@pytest.mark.parametrize(
    ("n", "expected"),
    [
        (-1, "unlimited"),
        (0, "0 B"),
        (512, "512 B"),
        (1024, "1 KiB"),
        (1500, "1.46 KiB"),
        (1_048_588, "1.00 MiB"),
        (1_073_741_824, "1 GiB"),
        (1024**4, "1 TiB"),
        (5 * 1024**5, "5120 TiB"),
    ],
)
def test_humanize_bytes(n: int, expected: str) -> None:
    assert humanize_bytes(n) == expected


@pytest.mark.parametrize(
    ("name", "value", "expected"),
    [
        ("retention.ms", "604800000", "7d"),
        ("retention.ms", "-1", "unlimited"),
        ("retention.bytes", "-1", "unlimited"),
        ("segment.bytes", "1073741824", "1 GiB"),
        ("max.message.bytes", "1048588", "1.00 MiB"),
        ("cleanup.policy", "delete", "delete"),
        ("min.insync.replicas", "2", "2"),
        ("retention.ms", "not-a-number", "not-a-number"),
        ("local.retention.ms", "-2", "-2"),
        ("retention.ms", None, None),
        ("retention.ms", "", ""),
    ],
)
def test_humanize_config_value(name: str, value: str | None, expected: str | None) -> None:
    assert humanize_config_value(name, value) == expected
