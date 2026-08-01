"""EDGAR lakehouse medallion pipelines (repo 4 of 5).

Layering (see AGENTS.md section 4). Each layer may only call *down*:

    L0  config, session
    L1  framework/       -- generic, knows nothing about EDGAR
    L2  bronze/          -- landing -> bronze, append only
    L3  silver/          -- typed, deduped, MERGEd, DQ'd
    L4  gold/            -- marts, including restatement detection
    L5  export/          -- gold -> Parquet -> S3 + manifest
    L6  entrypoints/     -- thin job task wrappers, no logic
"""

__version__ = "0.1.0"
