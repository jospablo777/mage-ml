"""
A local run log read as one text over its rotated segments, a window of lines at a time
(column_atlas_native, rust/column_atlas/src/logs.rs).

A block run's log rotates at 20 MB into block.log.1, block.log.2, ... (the higher, the
older). Readers took only block.log: whatever was printed before the last rotation was
hidden. A log item now carries the last WINDOW_LINES lines of all its segments, with
first_line and total_lines; the lines before first_line are read with window(path,
before_line=first_line), so no line is out of reach and a large log is not sent whole.
"""
import glob
import json
import os
import re
from typing import Dict, List, Optional

# Lines of a log a page shows before asking for earlier ones.
WINDOW_LINES = int(os.getenv('MAGE_LOG_WINDOW_LINES') or 5_000)
# Matching lines a search returns per log, the newest.
SEARCH_LINES = int(os.getenv('MAGE_LOG_SEARCH_LINES') or 20_000)


def segment_paths(path: str) -> List[str]:
    """The log's segments, oldest first: path.N ... path.1, then path."""
    numbered = []
    for candidate in glob.glob(f'{glob.escape(path)}.*'):
        suffix = candidate[len(path) + 1:]
        if re.fullmatch(r'[0-9]+', suffix):
            numbered.append((int(suffix), candidate))
    return [candidate for _, candidate in sorted(numbered, reverse=True)] + [path]


def _base(path: str) -> Dict:
    segments = [p for p in segment_paths(path) if os.path.isfile(p)]
    return dict(
        name=os.path.basename(path),
        path=path,
        size=sum(os.path.getsize(p) for p in segments) if segments else None,
        modified_timestamp=round(os.path.getmtime(path)) if os.path.isfile(path) else None,
        segments=len(segments),
    )


def window(path: str, before_line: Optional[int] = None, lines: int = WINDOW_LINES) -> Dict:
    """
    The log item for path: the `lines` lines that end before line `before_line` (the last
    ones when None), over all its segments.
    """
    import column_atlas_native

    item = _base(path)
    result = json.loads(column_atlas_native.log_window(
        segment_paths(path), before_line, max(int(lines), 0),
    ))
    item.update(
        content=result['text'],
        first_line=result['first_line'],
        total_lines=result['total_lines'],
    )
    return item


def search(path: str, text: str, limit: int = SEARCH_LINES) -> Optional[Dict]:
    """The log item with only the lines that contain text, ignoring case, over all segments;
    None when no line does."""
    import column_atlas_native

    found = json.loads(column_atlas_native.log_search(segment_paths(path), text, limit))
    if not found:
        return None
    item = _base(path)
    item.update(
        content='\n'.join(match['text'] for match in found),
        matched_lines=[match['line'] for match in found],
    )
    return item
