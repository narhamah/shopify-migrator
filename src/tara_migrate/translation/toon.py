"""TOON (Token-Oriented Object Notation) codec.

TOON uses | as field separator and newlines as record separator.
Format:  id|value
Escaping: newlines → \\n, pipes → \\p, backslashes → \\\\
"""


def to_toon(entries):
    """Convert a list of {id, value} dicts to TOON format."""
    lines = []
    for entry in entries:
        eid = _toon_escape(str(entry["id"]))
        val = _toon_escape(str(entry["value"]))
        lines.append(f"{eid}|{val}")
    return "\n".join(lines)


def from_toon(toon_text):
    """Parse TOON format back to list of {id, value} dicts."""
    entries = []
    for line in toon_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = line.split("|", 1)
        if len(parts) == 2:
            entries.append({
                "id": _toon_unescape(parts[0]),
                "value": _toon_unescape(parts[1]),
            })
    return entries


def _toon_escape(text):
    """Escape special characters for TOON."""
    text = text.replace("\\", "\\\\")
    text = text.replace("|", "\\p")
    text = text.replace("\n", "\\n")
    return text


def _toon_unescape(text):
    """Unescape TOON special characters."""
    text = text.replace("\\n", "\n")
    text = text.replace("\\p", "|")
    text = text.replace("\\\\", "\\")
    return text
