import json
import os
from pathlib import Path
from typing import Dict, List, Tuple


CACHE_DIRS: List[Tuple[str, str]] = [
    ("Covalent buckets", "data/raw_cache/covalent"),
    ("BirdEye pages", "data/raw_cache/birdeye"),
    ("Cache TX windows", "data/alpha_profiler/cache_tx"),
    ("Legacy covalent cache", "data/cache/covalent"),
    ("Warehouse", "data/alpha_profiler/warehouse"),
]


def dir_stats(path: Path) -> Tuple[int, int]:
    size = 0
    count = 0
    if not path.exists():
        return size, count
    for p in path.rglob("*"):
        if p.is_file():
            count += 1
            size += p.stat().st_size
    return size, count


def main() -> None:
    report: Dict[str, Dict[str, int]] = {}
    for name, rel in CACHE_DIRS:
        path = Path(rel)
        size, count = dir_stats(path)
        report[name] = {"files": count, "bytes": size, "path": str(path)}
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
