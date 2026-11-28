"""A3: trip cleaning helpers."""

from __future__ import annotations

import re

_CODE_RE = re.compile(r"^(\d+)\.(\d{1,2})(_.*)?$")


def normalize_station_code(raw: str | None) -> str | None:
    """A3: normalize a station code to the form 1234.56 (spec section 2).

    Input: raw code from trips (start/end_station_id) or GBFS short_name.
    Output: normalized code, or None when the input does not match.
    Examples: "5980.1" -> "5980.10", "5348.06_old" -> "5348.06", "SYS016" -> None.
    """
    if raw is None:
        return None
    m = _CODE_RE.match(raw.strip())
    if not m:
        return None
    return f"{m.group(1)}.{m.group(2).ljust(2, '0')}"
