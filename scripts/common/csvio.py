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
