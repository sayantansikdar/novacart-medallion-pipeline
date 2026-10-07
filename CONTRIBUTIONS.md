# Contributions

## Team Bug Byts

| GitHub | Name |
|---|---|
| [@sayantansikdar](https://github.com/sayantansikdar) | Sayantan Sikdar |
| [@Harshita-Basera](https://github.com/Harshita-Basera) | |
| [@kritika240624](https://github.com/kritika240624) | |
| [@Pari219](https://github.com/Pari219) | |
| [@RohanS00007](https://github.com/RohanS00007) | Rohan Singh |

The team wrote the original design note ("NovaCart – Medallion Pipeline Design Note", Azure Databricks + Delta Lake),
whose layer design, business rules and assumptions this implementation follows; the Fabric version is
[`docs/NovaCart_Design_Note.pdf`](docs/NovaCart_Design_Note.pdf).

## [@sayantansikdar](https://github.com/sayantansikdar)

Led the implementation in this repository: directed the build, made the decisions below, and reviewed
each module's test results before the next one started.

**Prompt engineering and working protocol** — [`docs/prompts/master_build_prompt.md`](docs/prompts/master_build_prompt.md)
- Wrote the master build prompt: a 10-module plan (discovery → teardown), house rules (credential protocol, a single
  config notebook, idempotency for every module, a "contract" between modules, diagnose-before-patch, honesty about
  free tiers), security and cost constraints, and a per-module AI-disclosure requirement.
- Defined the **52-case test matrix** (B1–B6, S1–S13, I1–I7, C1–C6, G1–G9, O1–O4, R1–R5, D1–D2) that every module
  was built and verified against, and the five cases that matter most (idempotency, late data, the UTC date boundary,
  the 0.50 threshold, the point-in-time join).

**Platform and design decisions**
- Worked through the platform options when the original Azure Databricks plan was blocked (student subscription
  disabled; trial account limited to Contributor): Azure free trial, Databricks Free Edition, AWS/GCP, and chose
  **Microsoft Fabric** on the team's existing `NovaCart_HCL` workspace and F2 capacity.
- Cost discipline: capacity resumed only while running and paused right after (about $2–3 of credit in total).
- Approved the key design choices recorded in the design note: reuse of the team workspace and existing uploads;
  the same-`updated_at` tie rule (later batch wins only if content differs); keeping payments for unknown orders out
  of the mismatch check; the payment-mismatch expected amount; a full reset before the official runs; a recorded
  alert instead of an e-mail; a public repository.

**Validation and delivery**
- Reviewed every module's results (224 automated tests, see [TEST_RESULTS.md](TEST_RESULTS.md)) and the deliverables:
  pipeline export and monitored runs, DQ report, Part 5/6 evidence, design note, cheatsheet.
- Fabric portal monitoring and run screenshots; viva preparation.

## Other members

Contribution details for [@Harshita-Basera](https://github.com/Harshita-Basera),
[@kritika240624](https://github.com/kritika240624), [@Pari219](https://github.com/Pari219) and
[@RohanS00007](https://github.com/RohanS00007) are to be added by each member.
