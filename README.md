# NovaCart Order Analytics: Medallion Pipeline

A bronze → silver → gold pipeline for NovaCart's September 2026 orders on **Microsoft Fabric**
(OneLake lakehouses, Delta Lake, Fabric Spark, Data Pipelines), built for the
"NovaCart Order Analytics: Data Engineering Lab". It gives the right answer after batch 1, after
batch 2, and when either batch is run twice. Team: Bug Byts.

- **Submission pack, test results and AI-use disclosure:** [SUBMISSION.md](SUBMISSION.md)
- **Design note:** [docs/NovaCart_Design_Note.pdf](docs/NovaCart_Design_Note.pdf)
- **Deliverables (pipeline export, monitored runs, DQ report, evidence):** [deliverables/NovaCart_Deliverables.pdf](deliverables/NovaCart_Deliverables.pdf)
- **Contributions (incl. the master build prompt and test matrix):** [CONTRIBUTIONS.md](CONTRIBUTIONS.md)
- **Evidence write-up:** [evidence/EVIDENCE.md](evidence/EVIDENCE.md) · **Cheatsheet:** [docs/NovaCart_Pipeline_Cheatsheet.pdf](docs/NovaCart_Pipeline_Cheatsheet.pdf)

## Layout

| Folder | Contents |
|---|---|
| `notebooks/` | Pipeline notebooks (`00_config` … `05_gold`, `lib_silver`, `lib_gold`, `99_notify_failure`), `06_evidence`, and `tests/` |
| `pipeline/` | Pipeline definition source (steps, retries, failure path) |
| `setup/` | Deploy/run tools (lakehouses, notebooks, pipeline, exports, teardown) |
| `tests/local/` | Independent plain-Python calculation of the expected gold numbers |
| `evidence/`, `deliverables/` | Run logs, exports, evidence and the generated PDFs |

## Repository rules

- The input files are never edited or committed; they are read from OneLake at runtime.
- No secret, token or key is ever committed (all access uses Entra ID sign-in).
