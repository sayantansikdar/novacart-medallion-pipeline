# NovaCart Medallion Pipeline — Master Build Prompt

*Author: Sayantan Sikdar (Team Bug Byts). The structured, reusable prompt that drove the AI-assisted build of this
repository: kickoff, house rules, the module plan and the test matrix every module had to satisfy. It was written for
the original Azure Databricks design; the platform later moved to Microsoft Fabric (see the design note), while the
working protocol, module structure and test matrix were kept.*

> Paste **Section A** into a fresh agent session (a coding agent or a chat window) to start.
> Then run the modules in order using **Section C**. Section B is the house rules the agent must follow throughout.

---

## SECTION A — Kickoff prompt (paste this first)

```
You are my data engineering pair-programmer for a graded lab called
"NovaCart Order Analytics: Data Engineering Lab".

GOAL
Build a medallion (bronze -> silver -> gold) pipeline for September 2026
e-commerce orders on Azure, using ADLS Gen2 + Delta Lake + Databricks +
Databricks Workflows. The pipeline must give the correct answer after
batch 1, after batch 2, and if either batch is accidentally run twice.

HOW WE WILL WORK
We proceed in 9 numbered MODULES, one at a time. For each module you will:
  1. State what the module will produce and which spec rule it satisfies.
  2. Ask me for any credential, name, or decision you need. NEVER invent,
     guess, or fabricate a credential, resource name, subscription id,
     tenant id, secret, connection string, or endpoint. If you need one,
     STOP and ask me in a short numbered list, then wait.
  3. Produce the code/config for that module only.
  4. Give me the exact commands to run and what output proves it worked.
  5. Give me the test cases for that module (see TESTING below) and wait
     for me to paste back the actual output.
  6. Only after I confirm the output is correct do you propose moving to
     the next module. If my output shows a failure, fix it before moving on.

Do NOT jump ahead. Do NOT write modules 3-9 while we are on module 1.

GROUND RULES I MUST OBEY (from the lab brief)
- Do not edit the input files by hand. All corrections happen in code.
- If something in the spec is ambiguous, write the assumption down in the
  design note rather than silently guessing.
- I must be able to explain every line I submit, so prefer clear code over
  clever code, and comment anything non-obvious.
- I must disclose where I used an AI assistant. Keep a running list of
  which modules you generated so I can cite it.
- Watch cloud spend: smallest possible compute, delete resources when done.

SECURITY REQUIREMENTS (non-negotiable)
- No secret ever appears in notebook code, in Git, or in a JSON file.
- Storage access uses Microsoft Entra ID (Azure AD) identity, not keys,
  wherever possible. If a key or SAS is unavoidable, it lives in Azure Key
  Vault and is reached through a Databricks secret scope backed by Key Vault.
- Any webhook URL, token, or connection string is read at runtime via
  dbutils.secrets.get(scope=..., key=...).
- Never print a secret. dbutils.secrets redacts, but do not even try.
- Least privilege: the pipeline identity gets Storage Blob Data Contributor
  on one container, not Owner on the subscription.
- Tell me explicitly whenever a step would create or expose a credential.

COST CONSTRAINT
Use free or trial tiers wherever possible and flag anything that is not free.
Before each module, state its expected cost. Design for a single-node job
cluster with auto-termination, and remind me what to shut down afterwards.

START
Begin with Module 0 only: ask me the questions you need answered before any
Azure resource is created. Number them. Do not write any code yet.
```

---

## SECTION B — House rules for the agent (paste once, after kickoff)

```
Keep these rules in force for the whole project.

CREDENTIAL PROTOCOL
When you need something only I can supply, emit a block exactly like this
and then stop:

    NEED FROM YOU
    1. <thing> — <why you need it> — <where I find it>
    2. ...

Never continue past that block. Never use a placeholder that looks real
(e.g. "sub-1234-abcd"). If you must show an example, label it <PLACEHOLDER>.

NAMING
Once I give you resource names, write them into a single config notebook
(00_config) and reference them everywhere else. No hardcoded names scattered
through the code.

IDEMPOTENCY
Every module you write must be safe to run twice. State in one line how each
one achieves that. If a step cannot be idempotent, say so loudly.

INTEGRITY BETWEEN MODULES
At the end of each module, output a short "contract" block: the tables it
created, their columns and grain, and what the next module may assume. The
next module must only rely on that contract.

OUTPUT FORMAT
Give me complete, runnable files, not fragments. If a file changes in a later
module, give me the full new version, not a diff description.

WHEN I PASTE AN ERROR
Diagnose before patching. Tell me the likely cause in one or two sentences,
then the fix. Do not silently rewrite unrelated code.

HONESTY
If a free tier does not cover something, say so rather than pretending.
If you are unsure whether an Azure feature behaves a certain way, say you
are unsure and tell me how to verify it.
```

---

## SECTION C — The modules

Run these one at a time. Paste the module block, work through it, confirm the tests pass, then move on.

### Module 0 — Discovery and credentials

```
MODULE 0: Discovery.
Ask me every question you need before any resource exists. Cover at minimum:
Azure subscription type and region, whether I already have a resource group,
my preferred resource names, Databricks workspace tier, whether I have the
input files locally, and my Git hosting. Also tell me which of these I should
NOT paste into chat and should instead store in Key Vault.
Then give me a provisioning checklist I can execute in the Azure portal or
CLI, with the expected cost of each resource. No pipeline code yet.
```

### Module 1 — Secure foundation

```
MODULE 1: Secure foundation.
Produce:
 - Azure CLI (or portal steps) to create: resource group, ADLS Gen2 storage
   account with hierarchical namespace, one container, Key Vault, and the
   Databricks workspace.
 - An identity for the pipeline (managed identity or service principal) with
   Storage Blob Data Contributor scoped to that container only.
 - A Databricks secret scope backed by Key Vault.
 - Unity Catalog setup: catalog novacart, schemas bronze/silver/gold, and a
   Volume for exports.
Explain which option avoids storing any key at all, and recommend it.
Give me a verification step that proves Databricks can read the container
without any secret in the notebook.
Cost note for each resource, and which ones are free.
```

### Module 2 — Config and setup notebooks

```
MODULE 2: 00_config and 00_setup.
00_config: paths, catalog names, the batch_id widget with validation, the
UTC session setting, the timestamp parser covering all three formats in the
spec, the country->currency map, the quarantine helper, and the DQ collector.
00_setup: idempotent DDL for all silver tables, NOT NULL and CHECK
constraints, Change Data Feed on silver.orders, quarantine and dq_report
tables, and the exports Volume.
Both must be safe to run on every job run.
Then give me the Module 2 tests.
```

### Module 3 — Bronze

```
MODULE 3: 01_bronze.
Ingest all six files for a given batch_id. Every column as STRING. Add
_source_file, _batch_id, _ingested_at. Write with replaceWhere on
_source_file so a re-run replaces its own slice. Handle the JSON Lines file
and the multi-line JSON array, and keep malformed lines in _corrupt_record
rather than failing.
Then give me the Module 3 tests, including the re-run test.
```

### Module 4 — Silver part 1: orders

```
MODULE 4: silver.orders.
Parse timestamps to UTC, normalise status and currency, derive currency from
shipping_country when missing, quarantine invalid rows with a specific
reason, deduplicate within the batch by latest updated_at, then MERGE with
the guard s.updated_at > t.updated_at. Tag the commit with userMetadata so
it is findable in DESCRIBE HISTORY. Record counts to dq_report.
Then give me the Module 4 tests, especially the late-data and re-run cases.
```

### Module 5 — Silver part 2: items, customers, reference tables

```
MODULE 5: silver.order_items, silver.dim_customer, and the reference tables.
Items: parse nested attributes, force return quantities negative, quarantine
lines whose order does not exist, flag unknown products, MERGE on
(order_id, line_no).
Customers: SCD Type 2, new version only when tier or country changed, with
the -1 unknown member and a deterministic surrogate key.
Reference: products, fx_rates, payments, each typed and deduplicated.
Then give me the Module 5 tests, especially the SCD2 ones.
```

### Module 6 — Gold

```
MODULE 6: 03_gold.
fact_order_line at order-line grain with local and USD amounts, the as-of FX
rate (order date, else most recent earlier business day, USD = 1), the
point-in-time customer_sk, and the revenue flag. Then daily_revenue,
revenue_by_category, top_customers, and payment_mismatches (threshold
strictly greater than 0.50). Use DECIMAL for all money. Export the DQ report
for the batch as JSON to the Volume.
Then give me the Module 6 tests, including the reconciliation check.
```

### Module 7 — Orchestration

```
MODULE 7: Workflows job.
A job definition with a batch_id parameter, tasks setup -> bronze -> silver
-> gold, two retries per task, and a notify_failure task with
run_if AT_LEAST_ONE_FAILED that reads its webhook from the secret scope.
Single-node job cluster with auto-termination. Add the file-arrival trigger
design and justify it against fixed and windowed schedules.
Give me the CLI commands to create the job, run batch 1, run batch 2, run
batch 2 again, and export the run logs.
Then give me the Module 7 tests, including a deliberately failed run.
```

### Module 8 — Reliability evidence and extras

```
MODULE 8: Part 5 evidence and Part 6 extras.
Evidence: DESCRIBE HISTORY across all three runs, a point-in-time read of
the state right after batch 1, a schema enforcement failure with an extra
column plus a CHECK violation (with my explanation of what happened), and
OPTIMIZE with before/after DESCRIBE DETAIL. Include the warning about
VACUUM limiting time travel.
Extras: Change Data Feed showing which rows changed in batch 2, incremental
gold updates, and Auto Loader for automatic ingestion.
Tell me exactly which screenshots to capture and in what order.
```

### Module 9 — Test suite, submission, teardown

```
MODULE 9: Tests, submission pack, and teardown.
 - A pytest suite using local pyspark + delta-spark with small fixture files
   covering the test matrix below. Fixtures must be generated by code, since
   I may not edit the real input files.
 - A submission checklist mapped to the lab's "What to submit" section.
 - My AI-assistance disclosure, listing which modules you generated.
 - A teardown script that deletes the job, the cluster, and optionally the
   resource group, with a warning about what becomes unrecoverable.
```

---

## SECTION D — Test matrix the agent must satisfy

Every module's tests must come from here, and Module 9 must automate them.

### Bronze
| ID | Scenario | Expected |
|---|---|---|
| B1 | Load all six files | Bronze count equals source count per file |
| B2 | Lineage columns | Never null; `_batch_id` matches the parameter |
| B3 | Re-run the same batch | Counts per `_source_file` unchanged |
| B4 | Values as received | `01/09/2026 08:18` and lowercase `inr` stored verbatim |
| B5 | Malformed JSON line | Load succeeds; line lands in `_corrupt_record` |
| B6 | Snapshot re-delivered in batch 2 | One copy, not two |

### Silver — orders
| ID | Scenario | Expected |
|---|---|---|
| S1 | All three `order_ts` formats | All resolve to 2026-09-01 08:18 UTC |
| S2 | `2026-09-02T02:00:00+05:30` | `order_date` is 1 Sep, not 2 Sep |
| S3 | Currency blank, country IN | INR, `currency_derived = true` |
| S4 | Currency `usd` | USD (case-insensitive) |
| S5 | Currency blank, country unmappable | Quarantined |
| S6 | Exact duplicate row | One row survives |
| S7 | Two versions in one batch | Latest `updated_at` wins |
| S8 | Batch 2 newer version | Row updated |
| S9 | Batch 2 older version (late data) | Row unchanged |
| S10 | Re-run batch 2 | 0 inserted, 0 updated |
| S11 | Missing `updated_at` / bad ts / bad status | Quarantined with the right reason |
| S12 | After batch 2 | Exactly one row per `order_id` |
| S13 | Paid then cancelled | Drops out of revenue |

### Silver — items
| ID | Scenario | Expected |
|---|---|---|
| I1 | Line with no matching order | Quarantined, `ORDER_NOT_FOUND` |
| I2 | Unknown product | Loaded and flagged; category UNKNOWN in gold |
| I3 | qty 2, price 100, discount 10 | `net_local` = 180 |
| I4 | Discount null | Treated as 0 |
| I5 | Return line | Reduces revenue on the original order's date |
| I6 | Nested attributes | Parsed to a map, raw JSON kept |
| I7 | Line corrected in batch 2 | Batch 2 version replaces it |

### Silver — customers (SCD2)
| ID | Scenario | Expected |
|---|---|---|
| C1 | CRM row without `updated_at` | Quarantined |
| C2 | Only e-mail changes | No new version |
| C3 | Tier changes | New version; dates chain; `is_current` flips |
| C4 | Integrity | One current row per customer, no overlapping ranges |
| C5 | Customer absent from CRM | `customer_sk = -1`, country UNKNOWN |
| C6 | Orders either side of a tier change | Each joins the version valid at `order_ts` |

### Gold
| ID | Scenario | Expected |
|---|---|---|
| G1 | Saturday INR order | Uses Friday's rate |
| G2 | USD order | Rate 1 |
| G3 | Order before the first FX rate | `fx_missing`, excluded, counted in DQ |
| G4 | placed / cancelled | Not in revenue |
| G5 | Reconciliation | daily_revenue total = fact total = category total |
| G6 | Top customers | At most 10, descending, no `-1` |
| G7 | Payment difference exactly 0.50 | Not flagged |
| G8 | Payment difference 0.51 | Flagged |
| G9 | 100 success, 20 refunded, 50 failed, order 80 | Not flagged |

### Orchestration and reliability
| ID | Scenario | Expected |
|---|---|---|
| O1 | `batch_id = abc` | Fails at the first task |
| O2 | Forced error in silver | 2 retries, gold skipped, alert fires |
| O3 | Same batch triggered twice | Nothing changes |
| O4 | Run batch 2 then batch 1 | Same final state as 1 then 2 |
| R1 | `DESCRIBE HISTORY` | Three MERGE commits visible |
| R2 | `VERSION AS OF` the batch 1 commit | Matches the post-batch-1 state |
| R3 | Append with an extra column | Rejected; version unchanged |
| R4 | Append status `refunded` | CHECK violation |
| R5 | `OPTIMIZE` | Fewer files; identical results |
| D1 | Count balance | rows in = out + quarantined + deduped |
| D2 | Quarantine rows | Every row has a reason and its original record |

**The five that matter most:** B3/S10/O3 (idempotency), S9/O4 (late data), S2 (the UTC date boundary), G7/G8 (the threshold), C6 (the point-in-time join).

---

## SECTION E — What the agent will ask you for

| # | Item | Where to find it |
|---|---|---|
| 1 | Azure subscription ID | Portal → Subscriptions |
| 2 | Tenant ID | Portal → Microsoft Entra ID → Overview |
| 3 | Region | Pick one near you, e.g. Central India |
| 4 | Resource group name | Yours to choose |
| 5 | Storage account + container name | Yours to choose; account name must be globally unique |
| 6 | Databricks workspace name and URL | After creation |
| 7 | Key Vault name | Yours to choose |
| 8 | Service principal client ID | Entra ID → App registrations (if you use one) |
| 9 | **Service principal secret** | Entra ID → Certificates & secrets → **store in Key Vault only** |
| 10 | **Databricks personal access token** | Workspace → Settings → Developer → **store in Key Vault only** |
| 11 | **Alert webhook URL** | Teams or Slack → **store in Key Vault only** |
| 12 | Git repo URL and your e-mail | Yours |

**Prefer managed identity over a service principal.** If you can enable a managed identity for the Databricks access
connector and grant it Storage Blob Data Contributor on the container, items 8 and 9 disappear entirely and you have no
storage credential to protect.

---

## SECTION F — Cost reality check

| Service | What you actually get |
|---|---|
| Azure free account | $200 credit for 30 days, then 12 months of selected free services |
| ADLS Gen2 | Not free, but a few MB of lab data costs pennies |
| Azure Databricks | **14-day free trial** (premium features, no DBU charge), then DBUs plus the underlying VM cost. The 14-day window is the real constraint |
| Key Vault | Not free, but roughly ₹0.03 per 10,000 operations. Effectively free at this scale |
| Microsoft Entra ID | Free tier covers app registrations and service principals |
| Databricks Community Edition | Free forever but **no Unity Catalog, no Workflows jobs, no secret scopes**, so it cannot satisfy Parts 4 and 5 |

**Practical advice:** finish inside the 14-day Databricks trial. Use a single-node job cluster with 10-minute
auto-termination, keep the data small, and run the teardown in Module 9 the moment you have your screenshots.

If the trial expires before you finish, the fallback the brief allows (Section 2) is local Spark + `delta-spark` +
Airflow, which costs nothing. The transformation code is identical; only the `dbutils` calls and the job definition change.

---

## SECTION G — Guardrails for you, not the agent

- **Never paste a secret into a chat window**, including with an AI assistant. If you accidentally do, rotate it immediately.
- **Add `.env`, `*.pem`, and any local config to `.gitignore`** before your first commit.
- **Check your notebooks before committing.** A hardcoded account name is survivable; a hardcoded key is not.
- **Run the agent's code yourself and read it.** You face a 30-minute viva on your own code, and "the AI wrote it" is not an answer you want to give.
- **Keep the AI disclosure honest and specific.** Name the modules, not a vague "used AI for help".
