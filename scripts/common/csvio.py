#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import csv
import os
from typing import List, Dict, Sequence

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def write_csv(path: str, rows: List[Dict], field_order: Sequence[str]) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(field_order), extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)

def write_rows(path: str, rows: List[Dict]) -> None:
    """
    Convenience writer that infers field order from the rows (in encounter order).
    """
    ensure_dir(os.path.dirname(path))
    if not rows:
        # create empty file with no header (consistent with zero results)
        with open(path, "w", encoding="utf-8"):
            pass
        return

    field_order: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in field_order:
                field_order.append(key)

    write_csv(path, rows, field_order)
