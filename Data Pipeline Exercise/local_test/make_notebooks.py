"""Generate .ipynb versions of the Databricks .py sources.

The .py files under databricks/ are the source of truth (Databricks
"notebook source" format — also plain importable Python, which is what the
local harness relies on). This script converts them into standard Jupyter
.ipynb files for drag-and-drop import into the Databricks workspace, so
the two formats can never drift: regenerate instead of editing the .ipynb.

Usage:  python make_notebooks.py
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
DBX = os.path.normpath(os.path.join(HERE, "..", "databricks"))
SOURCES = ["olist_transforms.py", "transform_olist.py"]

CELL_SEP = "# COMMAND ----------"
MAGIC = "# MAGIC "


def to_cells(py_text):
    lines = py_text.splitlines()
    if lines and lines[0].startswith("# Databricks notebook source"):
        lines = lines[1:]
    blocks, current = [], []
    for line in lines:
        if line.strip() == CELL_SEP:
            blocks.append(current)
            current = []
        else:
            current.append(line)
    blocks.append(current)

    cells = []
    for block in blocks:
        # trim leading/trailing blank lines
        while block and not block[0].strip():
            block.pop(0)
        while block and not block[-1].strip():
            block.pop()
        if not block:
            continue
        if all(l.startswith("# MAGIC") for l in block if l.strip()):
            stripped = [l[len(MAGIC):] if l.startswith(MAGIC) else "" for l in block]
            body = "\n".join(stripped)
            if body.startswith("%md"):
                cells.append(("markdown", body[len("%md"):].lstrip("\n ")))
            else:
                cells.append(("code", body))   # e.g. a %run cell
        else:
            cells.append(("code", "\n".join(block)))
    return cells


def to_ipynb(cells):
    nb_cells = []
    for kind, src in cells:
        cell = {
            "cell_type": kind,
            "metadata": {},
            "source": src.splitlines(keepends=True),
        }
        if kind == "code":
            cell.update({"execution_count": None, "outputs": []})
        nb_cells.append(cell)
    return {
        "cells": nb_cells,
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {"name": "python3", "display_name": "Python 3",
                           "language": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


if __name__ == "__main__":
    for name in SOURCES:
        src_path = os.path.join(DBX, name)
        out_path = src_path[:-3] + ".ipynb"
        with open(src_path, encoding="utf-8") as f:
            cells = to_cells(f.read())
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(to_ipynb(cells), f, indent=1, ensure_ascii=False)
        n_md = sum(1 for k, _ in cells if k == "markdown")
        print(f"{os.path.basename(out_path)}: {len(cells)} cells "
              f"({n_md} markdown, {len(cells) - n_md} code)")
