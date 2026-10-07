# NovaCart Order Analytics: submission pack

Team **Bug Byts**: [@sayantansikdar](https://github.com/sayantansikdar) · [@Harshita-Basera](https://github.com/Harshita-Basera) · [@kritika240624](https://github.com/kritika240624) · [@Pari219](https://github.com/Pari219) · [@RohanS00007](https://github.com/RohanS00007)

Platform: **Microsoft Fabric** (OneLake lakehouses + Delta Lake + Fabric Spark + Data Pipelines), workspace `NovaCart_HCL`,
capacity `hackafabric` (F2, Central India). Raw inputs: `LH_NovaCart_Bronze/Files/NovaCart_SourceData/NovaCart_SourceData/`
(8 files, uploaded once, SHA-256 identical to the originals).

## What to submit (lab section 6) → where it is

| # | Requirement | Where |
|---|---|---|
| 1 | Code for bronze, silver and gold that runs top to bottom without manual edits | `notebooks/` (`00_config`, `00_setup`, `01_bronze`, `03_silver_reference`, `02_silver_orders`, `04_silver_items`, `05_gold`, `lib_silver`, `lib_gold`, `99_notify_failure`); run by the pipeline |
| 2 | Screenshots or an export of the pipeline definition and its two monitored runs | `deliverables/NovaCart_Deliverables.pdf` §1–2; `deliverables/raw/pipeline_definition.json`, `pipeline_runs.json`; `evidence/run_logs/`; plus screenshots from Fabric → Monitor (below) |
| 3 | DQ report and the Part 5 evidence | `deliverables/NovaCart_Deliverables.pdf` §3–4; `deliverables/raw/dq_batch_1.json`, `dq_batch_2.json`; `evidence/EVIDENCE.md` |
| 4 | Design note (max 1.5 pages) | `docs/NovaCart_Design_Note.pdf` (1 page) |
| 5 | Viva on your own code | `docs/NovaCart_Pipeline_Cheatsheet.pdf` (2 pages) + this file |

### Screenshots to capture (Fabric portal, signed in to the workspace account)
1. **Monitor** → filter *Item type = Data pipeline*: the four `novacart_medallion` runs (abc failed on purpose; batch 1, batch 2, batch 2 again completed).
2. `novacart_medallion` → batch 1 run → **View run details** (all six steps green).
3. Same for the **batch 2** run.
4. The **abc** run details: setup failed three times (retries), later steps skipped, notify_failure, fail_run.
5. `06_evidence` → *Recent runs* → latest run: the history table, CDF tables and printed evidence.

## How to run it again

```bash
az login                                                   # account with access to NovaCart_HCL
python3 -u setup/pipeline_tool.py deploy                   # (re)create the pipeline from pipeline/novacart_pipeline.json
python3 -u setup/pipeline_tool.py run 1                    # batch 1; then: run 2
python3 tests/local/reference_totals.py <input folder> 1   # independent expected numbers (plain Python)
```
Resume the F2 capacity before running and pause it afterwards (`setup/teardown.sh pause`).

## Test results (all from real runs)

| Module | Tests | Result |
|---|---|---|
| 1 Foundation | inputs readable, counts match, SHA-256 identical | pass |
| 2 Config + setup | O1, S1, S2, S3–S5, S11, D1, R4, idempotent setup | all pass |
| 3 Bronze | B1–B6, D1 | all pass (one test bug fixed, re-verified) |
| 4 Silver orders | S3–S12, O4, real-data reconciliation | all pass (R1 expectation corrected, re-verified) |
| 5 Items, SCD2, reference | I1–I7, C1–C4, D1, re-run | 38/38 |
| 6 Gold | G1–G9, C5, C6, I5, gold = plain-Python reference | 29/29 |
| 7 Pipeline | failure path (O1/O2), official runs, time-travel verification | 25/25 |
| 8 Evidence | R1–R5, VACUUM, CDF, incremental gold | 11/11 |

Not exercised by the real data, so covered with generated test data: malformed JSON, bad status, unmappable country,
`UK`, corrected item lines, same-`updated_at` ties, orders before the first FX rate, the 0.50 payment boundary.

## AI-assistance disclosure (lab ground rule)

I ([@sayantansikdar](https://github.com/sayantansikdar)) built this with an AI coding assistant. I defined the
working protocol and the 52-case test matrix, made the platform and design decisions, and reviewed and verified
every module through real test runs before moving on. The assistant drafted code, tests and documents to that plan.
Per module:

| Module | Drafted with the AI assistant | How I validated it | My decisions |
|---|---|---|---|
| 0 Discovery | Data-profiling scripts, discovery questions, platform comparison | Ran the profiling on the real files; reused its numbers as test expectations | Platform choice (Fabric), names, public repo, capacity use |
| 1 Foundation | `setup/01_create_lakehouses.sh`, notebook runner, input check | Idempotent re-run; SHA-256 comparison; row counts in Spark | Reuse of the team workspace and existing uploads |
| 2 Config + setup | `00_config`, `00_setup`, tests | Test notebook (timestamps, currency, constraints, idempotency): 45/45 | — |
| 3 Bronze | `01_bronze`, raw reader, tests | B1–B6 with independent counts | — |
| 4 Silver orders | `lib_silver` (orders), `02_silver_orders`, tests | Generated edge cases; real status mix equal to the profile | Tie rule (later batch wins only if different) |
| 5 Items / SCD2 / reference | `lib_silver` (rest), `03`, `04`, tests | 38 tests incl. SCD2 integrity and re-run | Keep unknown-order payments in silver |
| 6 Gold | `lib_gold`, `05_gold`, plain-Python reference, tests | Gold totals equal the independent Python calculation to the cent | Payment-mismatch expected amount (0 unless paid/shipped/delivered) |
| 7 Pipeline | Pipeline definition, deploy/run tool, alert notebook, reset + verification | Deliberate failure run; official runs; time-travel verification | Reset before official runs; recorded alert instead of e-mail |
| 8 Evidence | `06_evidence`, evidence write-up | 11 automatic checks on the official tables | — |
| 9 Submission | Design note, cheatsheet, deliverables PDF generators, this file | Every number in them comes from the saved run outputs | Final wording of the design note |

I reviewed every line and can explain it. Where a first draft was wrong (the Fabric child-notebook lakehouse rule,
the `fs.head` 100 KB limit, `try_to_date` missing in Spark 3.5, `count()` answered from Delta statistics), a failing
test caught it and the fix is in the git history.

## Teardown

`setup/teardown.sh pause` stops compute billing (everything kept). `setup/teardown.sh delete-tests` removes the test notebooks.
Deleting lakehouses, the workspace or the capacity is left to the team in the portal after grading (irreversible; the capacity
belongs to the team's Azure subscription).
