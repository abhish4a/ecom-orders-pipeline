# Architecture and data flow

```mermaid
flowchart LR
    subgraph SRC[Source: data/.../input]
        O[orders/*.csv<br/>order events]
        C[customers.csv]
        P[products.csv]
    end

    subgraph ING[ingest.py]
        H{header check<br/>per file}
        R[read as strings<br/>+ source_file<br/>+ _corrupt_record]
    end

    subgraph VAL[validate.py]
        T[parse types<br/>trim/blank→null]
        J[join dims<br/>broadcast in optimized]
        RU[rules → reject_reasons]
    end

    subgraph TR[transform.py]
        PR[prune columns]
        D[dedup per order_id<br/>latest event_ts wins]
        B[order_sk, order_date,<br/>is_refund, run_id]
    end

    subgraph PUB[publish.py]
        M[merge with existing<br/>touched partitions only]
        S[(staging/run_id)]
        TG[(target orders/<br/>partitioned by order_date)]
        Q[(quarantine/<br/>partitioned by source_file)]
    end

    MET[(run_metrics/<br/>run_id.json + jsonl)]
    REC[reconcile.py<br/>report]

    O --> H
    H -- required columns present --> R
    H -- missing/renamed columns --> Q
    C --> J
    P --> J
    R --> T --> J --> RU
    RU -- invalid --> Q
    RU -- valid --> PR --> D --> B --> M
    TG -. existing rows .-> M
    M --> S -- dynamic partition overwrite --> TG
    PUB -.counts.-> MET
    TG --> REC
    Q --> REC
    O --> REC
```

ASCII version:

```
 orders/*.csv ─► [ingest] header check ──missing cols──────────────────────┐
                    │ read as strings (+source_file, +_corrupt_record)      │
 customers.csv ─┐   ▼                                                       ▼
 products.csv ──┴► [validate] parse → join dims → rules ──invalid──► quarantine/ (by source_file)
                    │ valid
                    ▼
                 [transform] prune → dedup(order_id, latest event_ts) → derive cols
                    │
                    ▼
                 [publish] read existing rows of touched order_date partitions
                           merge by order_id ─► staging/<run_id> ─► target orders/ (by order_date)
                    │
                    ▼
                 run_metrics/<run_id>.json, run_metrics.jsonl   ─►  [reconcile] report
```

## Module responsibilities

| Module | Responsibility |
|---|---|
| `config.py` | `PipelineConfig` dataclass, JSON config + CLI overrides |
| `spark_session.py` | SparkSession (UTC, strict date parser, dynamic overwrite), baseline/optimized confs |
| `schemas.py` | all column contracts (raw, dims, target, quarantine) |
| `ingest.py` | list files, per-file schema check, string read, lineage columns |
| `validate.py` | parsing, rule evaluation, referential integrity, reject projection |
| `transform.py` | column pruning, dedup (window vs group-by), target shaping |
| `publish.py` | merge-by-key, staging + swap, quarantine write, failure hook |
| `metrics.py` | `RunMetrics`, atomic JSON write, JSONL history |
| `pipeline.py` | orchestration + CLI, exit codes |
| `benchmark.py` | baseline vs optimized timing and shuffle metrics → `performance_report.md` |
| `reconcile.py` | independent source vs target checks → `reconciliation_report.md` |
| `data_generator.py` | reproducible test data + expected-outcome labels |
