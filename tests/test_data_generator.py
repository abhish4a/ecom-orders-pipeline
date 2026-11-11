"""Generator: reproducible, self-consistent, labels kept out of the input."""
from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from ecom_pipeline import data_generator as gen


def _generate(out: Path, rows: int = 300, seed: int = 7) -> dict:
    gen.main(["--rows", str(rows), "--seed", str(seed), "--out", str(out)])
    return json.loads((out / "expected" / "expected_summary.json").read_text())


def _snapshot(root: Path) -> dict[str, bytes]:
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def test_same_seed_is_byte_identical(tmp_path: Path) -> None:
    _generate(tmp_path / "a")
    _generate(tmp_path / "b")
    assert _snapshot(tmp_path / "a") == _snapshot(tmp_path / "b")


def test_different_seed_changes_data(tmp_path: Path) -> None:
    _generate(tmp_path / "a", seed=1)
    _generate(tmp_path / "b", seed=2)
    assert _snapshot(tmp_path / "a") != _snapshot(tmp_path / "b")


def test_counts_reconcile_and_match_files(tmp_path: Path) -> None:
    s = _generate(tmp_path)
    assert s["input_rows"] == s["expected_output_rows"] + s["expected_rejected"] + s["expected_deduped"]
    on_disk = 0
    for f in (tmp_path / "input" / "orders").glob("*.csv"):
        with f.open() as fh:
            on_disk += sum(1 for _ in csv.DictReader(fh))
    assert on_disk == s["input_rows"] == 300


def test_every_edge_case_is_present(tmp_path: Path) -> None:
    s = _generate(tmp_path)
    assert set(gen.DEFECTS.values()) | {gen.SCHEMA_MISMATCH_REASON} == set(s["reject_reason_counts"])
    assert set(s["dedup_reason_counts"]) == {gen.SUPERSEDED_REASON, gen.EXACT_DUP_REASON}
    v2 = list((tmp_path / "input" / "orders").glob("*_schema_v2.csv"))
    assert len(v2) == 1
    header = v2[0].read_text().splitlines()[0].split(",")
    assert "total_amount" in header and "order_amount" not in header


def test_labels_are_never_inside_input_files(tmp_path: Path) -> None:
    _generate(tmp_path)
    for f in (tmp_path / "input").rglob("*.csv"):
        header = f.read_text().splitlines()[0]
        assert "expected" not in header and "label" not in header
    assert (tmp_path / "expected" / "expected_outcomes.csv").exists()


def test_committed_sample_is_current() -> None:
    """data/sample must be exactly what `make sample` produces (seed 42, 150 rows)."""
    summary = json.loads((Path(__file__).parents[1] / "data/sample/expected/expected_summary.json").read_text())
    assert summary["seed"] == 42 and summary["input_rows"] == 150


def test_too_few_rows_is_rejected() -> None:
    with pytest.raises(ValueError):
        gen.build_dataset(50, 1, gen.date(2026, 1, 1), 5, 100, 0.02, 0.03, 0.01, 0.02)
