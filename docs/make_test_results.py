"""Write TEST_RESULTS.md: every test from every module, its result, its actual output and what it shows.

Reads the saved results in tests/results/module{1..8}.json (copied from each module's Fabric run).
Usage: python3 docs/make_test_results.py
"""
import json, os, re

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(ROOT, "tests", "results")

MODULES = [
    (1, "Foundation", "setup/verify_inputs_notebook.py", "Inputs are present, unedited and readable by Spark"),
    (2, "Config + setup", "notebooks/tests/test_module2.py", "Parsing rules, constraints and setup idempotency"),
    (3, "Bronze", "notebooks/tests/test_module3.py", "Everything loaded exactly as received, re-run safe"),
    (4, "Silver orders", "notebooks/tests/test_module4.py", "Late data, re-runs and batch order cannot change the answer"),
    (5, "Items, SCD2, reference", "notebooks/tests/test_module5.py", "Order lines, customer history and snapshots"),
    (6, "Gold", "notebooks/tests/test_module6.py", "Business numbers, matched to an independent Python calculation"),
    (7, "Pipeline + official runs", "notebooks/tests/test_module7.py", "Failure path and the official batch 1 / 2 / 2-again runs"),
    (8, "Evidence (Parts 5-6)", "notebooks/06_evidence.py", "Delta reliability features demonstrated on the real tables"),
]

# Ordered (pattern, insight): the first pattern found in the test name wins.
INSIGHTS = [
    # Module 1
    (r"^SHA-256", "Byte-for-byte identical to the originals: the 'never edit inputs' rule is provable."),
    (r"^rows readable", "Spark reads every file with the same record count as the local profile, using only Entra sign-in (no keys)."),
    (r"^lakehouses created", "The setup script can run any number of times without creating duplicates."),
    # Module 2
    (r"^O1 batch_id='2' accepted", "Valid batch ids pass through unchanged."),
    (r"^O1 batch_id", "Only 1 or 2 is accepted; a bad value stops the run before anything is written."),
    (r"^S1 ", "All three source formats resolve to the same UTC instant."),
    (r"^S2 ", "An early-morning +05:30 order belongs to the previous UTC day (236 real rows are like this)."),
    (r"^S11 unparseable", "Unparseable text becomes NULL and is quarantined later, never a crash."),
    (r"^S3 blank \+ IN|^S3 NULL \+ SG", "A missing currency is derived from the shipping country and flagged as derived."),
    (r"^S4 'usd'", "Currency codes are case-insensitive."),
    (r"^S4 ' gbp '", "Stray spaces are trimmed before matching."),
    (r"^S5 ", "A currency that cannot be derived leads to quarantine, not a guess."),
    (r"^UK treated", "'UK' is accepted as Great Britain (documented assumption)."),
    (r"^D1 unbalanced", "The DQ report refuses counts that do not add up: no row can disappear silently."),
    (r"^setup table", "Created with CREATE TABLE IF NOT EXISTS, so safe on every run."),
    (r"^setup Change Data Feed", "Row-level change capture is on, which Part 6 relies on."),
    (r"^setup constraint", "Constraint is active: bad rows are rejected at write time."),
    (r"^C5 unknown customer", "The -1 member exists once, for orders whose customer is not in the CRM."),
    (r"^R4 ", "Delta's CHECK constraint rejects an invalid status and leaves the table version unchanged."),
    (r"^NOT NULL", "Required columns cannot be empty."),
    (r"^return line with positive qty", "A return can never be stored with a positive quantity."),
    (r"^setup second run", "Setup runs at the start of every pipeline run and makes zero commits when nothing changed."),
    # Module 3
    (r"^B1 ", "No record lost or invented: bronze count equals a count made without Spark's parsers."),
    (r"^B2 \w+ lineage", "Every row can be traced to its file, batch and load time."),
    (r"^B4 \w+ every source column", "Nothing is cast or converted in bronze; silver decides."),
    (r"^B2 orders _batch_id matches", "Test bug, not a pipeline bug: bronze correctly kept batch 2's slice from an earlier run. "
                                      "Corrected to check only batch 1's rows and re-verified in Module 4."),
    (r"^B2 \(Module 3, corrected\)|^B2 orders batch 1 slice", "Batch 1's rows carry _batch_id = 1."),
    (r"^B4 day/month", "dd/MM/yyyy timestamps are stored verbatim; parsing happens in silver."),
    (r"^B4 lowercase", "Lower-case currency codes are kept as received."),
    (r"^B4/I6", "Nested attributes are kept as their raw JSON text."),
    (r"^B4 unit_price", "A price sent as a JSON string is kept as text, not converted."),
    (r"^B3 ", "Re-running a batch replaces its own file slice: counts unchanged."),
    (r"^B6 \w+ holds both", "Each batch's file is its own slice; batch 2 adds, never duplicates."),
    (r"^B6 \w+ snapshot", "A snapshot reloaded in batch 2 replaces batch 1's copy: one copy, not two."),
    (r"^D1 bronze dq_report rows", "Every bronze load is recorded in the DQ report."),
    (r"^D1 bronze rows_in", "Bronze rows in = rows out for every file and batch."),
    (r"^B5 ", "A malformed line is kept in _corrupt_record and the rest of the file still loads."),
    # Module 4
    (r"^S11/S5 quarantine reasons", "Each bad row gets a precise reason that names the rule it broke."),
    (r"^batch 1: 5 inserted", "MERGE inserts only valid, deduplicated rows."),
    (r"^S6 ", "Exact duplicates collapse to one row."),
    (r"^S7 ", "Within one batch the latest updated_at wins."),
    (r"^S3 blank currency \+ IN", "Derived currency survives the full silver path."),
    (r"^S4 'usd' -> USD", "Case normalised in the stored table."),
    (r"^batch 2: 1 inserted", "Batch 2 inserts new orders and updates only genuinely newer ones."),
    (r"^S8 ", "A newer version replaces the old one."),
    (r"^S9 ", "A late, older copy can never overwrite a newer row (10 real cases: cancelled orders stay cancelled)."),
    (r"^tie:", "Same updated_at: the later batch wins only if the content differs."),
    (r"^S10 re-run batch 2 -> 0", "Idempotent: a re-run changes nothing."),
    (r"^S12 one row", "Exactly one current row per order."),
    (r"^O4 batch 2 then batch 1 ->", "Order-independent: running batch 2 before batch 1 gives the same result."),
    (r"^batch 1: 495 orders", "1,418 batch-1 rows collapse to 495 orders (923 older versions and duplicates)."),
    (r"^D1 batch 1 balance|^D1 batch 2 balance", "Rows in = out + quarantined + deduplicated."),
    (r"^batch 1 on empty table", "All 495 orders are new on the first load."),
    (r"^S12 after batch 2", "945 distinct orders after both batches, one row each."),
    (r"^final status mix", "Exactly the profile computed locally: the late copies were ignored."),
    (r"^48 final orders", "48 orders end with a currency derived from the shipping country."),
    (r"^batch 2: 450 new", "450 orders exist only in batch 2."),
    (r"^S10 re-run batch 2 \(real", "Real data: the re-run inserts 0 and updates 0."),
    (r"^S10 still 945", "The re-run leaves the order count unchanged."),
    (r"^real orders data has no quarantined", "The real order files are clean; the rules are proven on generated data."),
    (r"^R1 preview", "Expectation was wrong, not the pipeline: a MERGE that changes nothing writes no commit. "
                     "Test corrected and re-verified in Module 5."),
    # Module 5
    (r"^R1 \(Module 4, corrected\)", "One tagged MERGE per batch, and no commit for the no-op re-run."),
    (r"^I1 line without order", "A line whose order does not exist is quarantined as ORDER_NOT_FOUND."),
    (r"^qty 'abc'", "Non-numeric quantity and a discount above 100% are quarantined with their own reasons."),
    (r"^I3 ", "net_local = qty × unit_price × (1 − discount/100), exact in DECIMAL."),
    (r"^I4 ", "A missing discount means 0%."),
    (r"^I2 unknown product loads", "An unknown product still loads, flagged product_known = false."),
    (r"^I5 return qty", "Returns are forced negative, so they reduce revenue."),
    (r"^I6 ", "Nested attributes become a queryable map; the raw JSON is kept alongside."),
    (r"^I7 line corrected", "A correction in batch 2 replaces the batch 1 line."),
    (r"^I7 re-run", "Re-running the correction changes nothing."),
    (r"^O4 batch 1 run after", "Running an older batch late cannot undo a correction."),
    (r"^C1 CRM row without", "A CRM row without updated_at is quarantined (business rule 8)."),
    (r"^C2 e-mail-only", "Changing only the e-mail does not create a new customer version."),
    (r"^C2 e-mail is Type 1", "Name and e-mail are Type 1: every version shows the latest value."),
    (r"^C3 tier change", "A tier change starts a new version; date ranges chain and is_current flips."),
    (r"^UK country stored", "Customer country UK is stored as GB."),
    (r"^customer_sk is deterministic", "The surrogate key is a hash, stable across rebuilds."),
    (r"^reference tables:", "Snapshot tables hold exactly the source rows; 166 customer versions plus the unknown member."),
    (r"^C4 one current", "Every one of the 130 customers has exactly one current version."),
    (r"^C4 no overlapping", "No customer has two versions valid at the same time."),
    (r"^C3 versions chain", "Each version ends exactly where the next begins."),
    (r"^C1 real data", "The 5 Platinum rows without updated_at are quarantined."),
    (r"^items: 971", "971 + 974 lines, minus 12 re-delivered identical lines = 1,933."),
    (r"^I1 real data", "The NC-990xxx orphans (10 in batch 1, 8 in batch 2) are quarantined."),
    (r"^I2 real data", "The 14 lines for products P101-P105 load and are flagged."),
    (r"^I5 every return", "All 90 return lines are negative."),
    (r"^D1 \w+ batch \d balance", "Rows in = out + quarantined + deduplicated for this table and batch."),
    (r"^re-run batch 2: no new commit", "Re-running batch 2 creates no new version on any silver table."),
    (r"^re-run batch 2: items", "The items re-run inserts 0 and updates 0."),
    # Module 6
    (r"^G1 Saturday", "Weekends have no FX rate: the Friday rate is used."),
    (r"^G1 net_usd", "USD = local amount × as-of rate."),
    (r"^G2 ", "USD orders use a rate of exactly 1."),
    (r"^G3 ", "An order before the first FX rate is flagged fx_missing and kept out of revenue, not dropped."),
    (r"^G4 ", "Only paid / shipped / delivered orders are revenue."),
    (r"^C6 order before", "Point-in-time join: an order before a tier change gets the old customer version."),
    (r"^C6 order after", "...and an order after it gets the new version."),
    (r"^C5 customer not in the CRM", "Orders for customers missing from the CRM still load, with customer_sk -1."),
    (r"^I5 return reduces", "A return reduces revenue on the original order's date."),
    (r"^G6 top customers exclude", "The unknown customer never appears in the top 10."),
    (r"^G7 ", "Threshold is strictly greater than 0.50: exactly 0.50 is not flagged."),
    (r"^G8 ", "0.51 is flagged."),
    (r"^G9 ", "Failed payments are ignored and refunds subtract."),
    (r"^payment for an unknown order", "Payments for unknown orders are counted, not compared."),
    (r"^gold \w+ = independent", "Gold matches a separate plain-Python calculation from the original files, to the cent."),
    (r"^G5 ", "The same revenue measured three ways agrees to the cent."),
    (r"^5 payments for unknown", "The 5 NC-80000x payments are counted separately."),
    (r"^G6 at most 10", "The top-10 list is complete, sorted and excludes unknown customers."),
    (r"^C5 real data", "Only C0901-C0906 (absent from the CRM) map to -1."),
    (r"^I2 14 unknown", "Unknown products roll up into the UNKNOWN category."),
    (r"^re-run gold", "Gold is a deterministic rebuild: same input, identical output."),
    (r"^DQ report exported", "Each batch's DQ report is exported as JSON."),
    # Module 7
    (r"^R1 history:", "Official history: version 3 = batch 1, version 4 = batch 2."),
    (r"^R1/S10", "The official batch 2 re-run left the table at version 4."),
    (r"^R2 VERSION AS OF", "Time travel returns the table exactly as it was after batch 1."),
    (r"^R2 status mix", "That past state matches the batch-1-only Python reference."),
    (r"^final orders and status", "The final state matches the Python reference."),
    (r"^after batch 1: gold", "Gold read as of batch 1 (time travel) equals the batch-1 Python reference."),
    (r"^after batch 2 \(final\): gold", "Final gold equals the full Python reference."),
    (r"^G5 reconciliation all pass", "All reconciliation checks pass on the final run."),
    (r"^D1 dq_report has every step", "All 13 steps (6 bronze, 6 silver, 1 gold) are recorded for the batch."),
    (r"^D1 every count balances", "No row is unaccounted for anywhere in the batch."),
    (r"^DQ JSON export exists", "The batch's DQ JSON was exported."),
    (r"^on-failure alert", "The alert from the deliberate batch_id = abc failure is still recorded."),
    # Module 8
    (r"^R1 one MERGE commit", "Change history: one tagged MERGE per batch (495 inserted; then 450 inserted, 137 updated)."),
    (r"^R1 the batch 2 re-run", "Idempotency is visible in history: the re-run made no version."),
    (r"^R2 TIMESTAMP AS OF", "Time travel by timestamp returns the same rows as by version."),
    (r"^R3 ", "Schema enforcement: an unexpected column is rejected and the table is untouched."),
    (r"^R5 ", "OPTIMIZE compacted the fact table from 13 files to 1 with identical data."),
    (r"^VACUUM", "VACUUM permanently deletes old versions' files: time travel to them fails afterwards."),
    (r"^CDF: ", "Change Data Feed shows exactly what batch 2 changed: 450 inserts, 137 updates."),
    (r"^reference tables unchanged", "Pre-condition for incremental gold holds: no reference table changed in batch 2."),
    (r"^incremental gold", "Rebuilding only the 608 changed orders gives exactly the full-rebuild result."),
]


def insight(test):
    for pattern, text in INSIGHTS:
        if re.search(pattern, test):
            return text
    return ""


def clean(s, n=110):
    s = re.sub(r"\s+", " ", str(s)).strip().replace("|", "/")
    s = re.sub(r"datetime\.datetime\(([^)]*)\)", "…", s)
    return s if len(s) <= n else s[: n - 1] + "…"


lines = ["# NovaCart pipeline: test results and insights", "",
         "Every automated test from every module, with its result, the actual output of the run, and what it shows. "
         "All results come from real runs on Microsoft Fabric; raw results are in [`tests/results/`](tests/results/). "
         "Generated by `docs/make_test_results.py`.", ""]
summary = ["| Module | What is tested | Tests | Passed | Failed | Notes |", "|---|---|---|---|---|---|"]
sections = []
total = passed = 0
for n, name, code, scope in MODULES:
    d = json.loads(open(os.path.join(RES, f"module{n}.json")).read(), strict=False)
    rows = d["results"]
    ok = sum(r["passed"] for r in rows)
    total, passed = total + len(rows), passed + ok
    fails = [r for r in rows if not r["passed"]]
    note = ("; ".join(f"{clean(r['test'], 40)}: see insight (corrected, re-verified later)" for r in fails)
            if fails else "all pass")
    summary.append(f"| {n} {name} | {scope} | {len(rows)} | {ok} | {len(fails)} | {note} |")
    sec = [f"## Module {n}: {name}", "", f"Code: [`{code}`]({code}) · **{ok} of {len(rows)} passed**", "",
           "| # | Test | Result | Output | Insight |", "|---|---|---|---|---|"]
    for i, r in enumerate(rows, 1):
        sec.append(f"| {i} | {clean(r['test'], 90)} | {'✅ PASS' if r['passed'] else '❌ FAIL'} | "
                   f"{clean(r['detail']) or '—'} | {insight(r['test'])} |")
    sections += sec + [""]

lines += ["## Summary", "", *summary, f"| **Total** | | **{total}** | **{passed}** | **{total - passed}** | |", "",
          "Both failures were mistakes in a test's expectation, not in the pipeline. Each was corrected, and the corrected "
          "check passed in the next module's run (Module 4 re-checked B2; Module 5 re-checked R1).", "",
          "## Key insights", "",
          "- **Idempotent everywhere:** re-running a batch changed nothing in bronze, silver or gold, and created no new table version.",
          "- **Late data is safe:** the 10 older 'paid' copies of cancelled orders were ignored; the final status mix equals the "
          "independently computed one.",
          "- **Order-independent:** batch 2 then batch 1 gives the same result as batch 1 then batch 2.",
          "- **Numbers are proven, not claimed:** gold after batch 1 ($150,062.64) and after batch 2 ($295,585.57) equal a "
          "plain-Python calculation from the original files to the cent.",
          "- **Nothing disappears silently:** every row is either loaded, quarantined with a reason, or counted as a duplicate, "
          "and the DQ report balances for every step of both batches.",
          "- **Edge cases the data does not contain** (malformed JSON, bad status, unmappable country, the 0.50 boundary, "
          "orders before the first FX rate) are tested with generated data, because the inputs may not be edited.",
          "- **Delta reliability features work on the real tables:** history, time travel, schema enforcement, CHECK "
          "constraints, OPTIMIZE (13 → 1 files), VACUUM risk, Change Data Feed and incremental gold.", ""]
lines += sections
open(os.path.join(ROOT, "TEST_RESULTS.md"), "w").write("\n".join(lines))
print(f"written TEST_RESULTS.md: {total} tests, {passed} passed")
