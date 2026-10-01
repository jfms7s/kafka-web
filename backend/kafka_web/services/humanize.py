"""Human-readable renderings of Kafka config values (spec §2); raw values are kept elsewhere."""

UNLIMITED = "unlimited"

_MS_UNITS = (("d", 86_400_000), ("h", 3_600_000), ("m", 60_000), ("s", 1_000), ("ms", 1))
_BYTE_UNITS = ("B", "KiB", "MiB", "GiB", "TiB")


def humanize_ms(ms: int) -> str:
    """`604800000` -> `7d`, `90061000` -> `1d 1h 1m 1s`, `-1` -> `unlimited`."""
    if ms == -1:
        return UNLIMITED
    if ms <= 0:
        return f"{ms}ms"
    parts = []
    for unit, size in _MS_UNITS:
        count, ms = divmod(ms, size)
        if count:
            parts.append(f"{count}{unit}")
    return " ".join(parts)


def humanize_bytes(n: int) -> str:
    """`1073741824` -> `1 GiB`; inexact values get two decimals; `-1` -> `unlimited`."""
    if n == -1:
        return UNLIMITED
    if n < 0:
        return f"{n} B"
    exponent = 0
    while exponent < len(_BYTE_UNITS) - 1 and n >= 1024 ** (exponent + 1):
        exponent += 1
    size = 1024**exponent
    unit = _BYTE_UNITS[exponent]
    if n % size == 0:
        return f"{n // size} {unit}"
    return f"{n / size:.2f} {unit}"


def humanize_config_value(name: str, value: str | None) -> str | None:
    """Humanize `*.ms` and `*.bytes` integer values; anything else is returned unchanged."""
    if value is None:
        return None
    is_ms = name.endswith(".ms")
    if not (is_ms or name.endswith(".bytes")):
        return value
    try:
        number = int(value)
    except ValueError:
        return value
    if number < -1:  # e.g. -2 means "inherit" for some tiered-storage settings
        return value
    return humanize_ms(number) if is_ms else humanize_bytes(number)
