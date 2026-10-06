"""Build the 2-page NovaCart project cheatsheet PDF."""
import sys
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak, KeepTogether)

from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.fonts import addMapping

FONTS = "/System/Library/Fonts/Supplemental/"
pdfmetrics.registerFont(TTFont("Uni", FONTS + "Arial Unicode.ttf"))      # covers ✓ → ≤ ≈
pdfmetrics.registerFont(TTFont("UniBold", FONTS + "Arial Bold.ttf"))
pdfmetrics.registerFont(TTFont("UniItalic", FONTS + "Arial Italic.ttf"))
pdfmetrics.registerFont(TTFont("UniBoldItalic", FONTS + "Arial Bold Italic.ttf"))
for bold, italic, face in [(0, 0, "Uni"), (1, 0, "UniBold"), (0, 1, "UniItalic"), (1, 1, "UniBoldItalic")]:
    addMapping("Uni", bold, italic, face)

OUT = sys.argv[1]
M5 = sys.argv[2] if len(sys.argv) > 2 else "Tests running at time of writing"
M7 = sys.argv[3] if len(sys.argv) > 3 else "Official runs in progress"

INK, MUTED, ACCENT, RULE, SOFT = (colors.HexColor(c) for c in ("#1d2433", "#5b6475", "#0f6e6e", "#d5dbe3", "#eef3f6"))
OK, WARN = colors.HexColor("#1f7a3d"), colors.HexColor("#9a5b00")

base = dict(fontName="Uni", textColor=INK, fontSize=7.9, leading=9.9)
S = {
    "title": ParagraphStyle("title", fontName="UniBold", fontSize=15, leading=18, textColor=INK),
    "sub": ParagraphStyle("sub", **{**base, "textColor": MUTED, "fontSize": 7.8}),
    "h": ParagraphStyle("h", fontName="UniBold", fontSize=9.6, leading=11.5, textColor=ACCENT,
                        spaceBefore=5, spaceAfter=2),
    "p": ParagraphStyle("p", **base),
    "cell": ParagraphStyle("cell", **{**base, "fontSize": 7.5, "leading": 9.2}),
    "cellb": ParagraphStyle("cellb", **{**base, "fontName": "UniBold", "fontSize": 7.5, "leading": 9.2}),
    "head": ParagraphStyle("head", **{**base, "fontName": "UniBold", "fontSize": 7.5, "leading": 9.2,
                                      "textColor": colors.white}),
    "bullet": ParagraphStyle("bullet", **{**base, "leftIndent": 7, "bulletIndent": 0}),
}


def P(text, style="p"):
    return Paragraph(text, S[style])


def bullets(items):
    return [Paragraph(t, S["bullet"], bulletText="•") for t in items]


def table(rows, widths, header=True, first_bold=True):
    data = []
    for i, r in enumerate(rows):
        style = "head" if header and i == 0 else None
        data.append([P(c, style or ("cellb" if first_bold and j == 0 else "cell")) for j, c in enumerate(r)])
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    cmds = [("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 1.6), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.6),
            ("LINEBELOW", (0, 0), (-1, -1), 0.4, RULE)]
    if header:
        cmds += [("BACKGROUND", (0, 0), (-1, 0), ACCENT)]
    for i in range(1 if header else 0, len(rows)):
        if i % 2 == 0:
            cmds.append(("BACKGROUND", (0, i), (-1, i), SOFT))
    t.setStyle(TableStyle(cmds))
    return t


def two_col(left, right, gap=5 * mm):
    w = (A4[0] - 24 * mm - gap) / 2
    t = Table([[left, right]], colWidths=[w, w])
    t.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LEFTPADDING", (0, 0), (-1, -1), 0),
                           ("RIGHTPADDING", (0, 0), (0, 0), gap), ("RIGHTPADDING", (1, 0), (1, 0), 0),
                           ("TOPPADDING", (0, 0), (-1, -1), 0), ("BOTTOMPADDING", (0, 0), (-1, -1), 0)]))
    return t, w


FULL = A4[0] - 24 * mm
story = []

# ======================================================================= PAGE 1
story += [
    P("NovaCart Order Analytics: Medallion Pipeline Cheatsheet", "title"),
    P("Team Bug Byts · HCLTech AI-Ready Cloud Data Engineer lab · status as of 6 Oct 2026 · "
      "repo: github.com/sayantansikdar/novacart-medallion-pipeline", "sub"),
    Spacer(1, 4),
    P("<b>The task.</b> Build a bronze → silver → gold pipeline for NovaCart's September 2026 orders that gives the "
      "right answer after batch 1 (1–15 Sep), after batch 2 (new orders + corrected versions of earlier ones), and "
      "when either batch is accidentally run twice. Inputs may never be edited by hand; every assumption goes in the "
      "design note; AI use must be disclosed and every line explainable in a 30-minute viva."),
]

story.append(P("1 · Platform: what we use and why", "h"))
journey = table([
    ["Step", "Outcome"],
    ["Azure for Students", "Subscription disabled (offer ended 19 Sep 2026); cannot be re-enabled from the CLI."],
    ["Databricks Free Edition", "Set up and working, but dropped in favour of the team's Fabric workspace."],
    ["Azure free trial", "Enabled, but only Contributor rights (cannot grant the storage role the Databricks design needs)."],
    ["<b>Microsoft Fabric (chosen)</b>", "Team workspace <b>NovaCart_HCL</b> on the existing <b>F2</b> capacity "
     "<i>hackafabric</i>, Central India. Resumed only while working, paused right after."],
], [34 * mm, FULL / 2 - 34 * mm - 2.5 * mm])
why = table([
    ["Lab requirement", "Fabric feature"],
    ["Raw + table storage", "OneLake lakehouses (Bronze / Silver / Gold)"],
    ["Upserts, history, point-in-time, schema enforcement", "Delta tables: MERGE, DESCRIBE HISTORY, VERSION AS OF, NOT NULL + CHECK"],
    ["Distributed processing", "Fabric Spark notebooks (PySpark + Spark SQL)"],
    ["Parameter, retries, on-failure path", "Fabric Data Pipeline (Module 7)"],
    ["Security", "Entra ID sign-in only: no keys, tokens or secrets anywhere"],
], [33 * mm, FULL / 2 - 33 * mm - 2.5 * mm])
left_w = FULL / 2 - 2.5 * mm
t, _ = two_col([journey], [why])
story.append(t)
story.append(Spacer(1, 2))
story.append(P("<b>Why Fabric:</b> one platform covers every requirement natively; the tables are Delta, so the "
               "Spark/Delta code is the same as the original Databricks design; Power BI sits on top for the JD's "
               "BI expectation; and the F2 costs about <b>$0.36/hour only while resumed</b> (≈ $2 so far)."))

story.append(P("2 · Architecture and run order", "h"))
arch = table([
    ["Layer", "Lakehouse / notebook", "What happens"],
    ["Input", "LH_NovaCart_Bronze / Files/NovaCart_SourceData/…",
     "8 raw files, uploaded once, SHA-256 identical to the originals. 3 file names differ from the brief: mapped in config, never renamed."],
    ["Setup", "00_config, 00_setup",
     "All names/paths/helpers in one place. Creates 8 silver tables with NOT NULL + 10 CHECK constraints, Change Data Feed on orders, the -1 unknown customer."],
    ["Bronze", "01_bronze → 6 tables",
     "Every column as STRING + _source_file, _batch_id, _ingested_at. Malformed lines kept in _corrupt_record. Write replaces only its own _source_file slice."],
    ["Silver", "03_silver_reference → 02_silver_orders → 04_silver_items",
     "Typed, cleaned, UTC. Bad rows → silver.quarantine (reason + original record). Counts → silver.dq_report. MERGE / snapshot sync, never blind overwrite."],
    ["Gold", "05_gold", "fact_order_line + daily_revenue, revenue_by_category, top_customers, payment_mismatches, reconciliation; DQ report exported as JSON."],
    ["Orchestration", "Pipeline novacart_medallion", "batch_id → setup → bronze → silver ×3 → gold; 2 retries per step; on failure: notify_failure (alert table + file) → fail_run."],
], [21 * mm, 50 * mm, FULL - 71 * mm])
story.append(arch)

story.append(P("3 · What profiling the data revealed (and how the design handles it)", "h"))
traps = table([
    ["Finding in the real data", "Handled by"],
    ["10 late batch-2 rows are older <b>paid</b> copies of orders batch 1 already marked <b>cancelled</b>",
     "MERGE guard: only a newer updated_at may replace a row"],
    ["236 +05:30 timestamps fall on the previous day in UTC", "Session time zone UTC; order_date = UTC date"],
    ["435 dd/MM/yyyy rows with day ≤ 12 (a month-first parser would silently be wrong)", "Explicit dd/MM/yyyy pattern"],
    ["49 batch-2 returns belong to batch-1 orders", "Returns dated by the parent order; gold rebuilt each run"],
    ["Orphan items NC-990xxx (10 + 8); unknown products P101–P105 (14 lines)", "ORDER_NOT_FOUND quarantine; product_known = false"],
    ["5 CRM rows (all Platinum) without updated_at; customers C0901–C0906 not in the CRM", "Quarantined; customer_sk = -1"],
    ["FX has no weekends and no Mon 7 Sep; 5 payments for unknown orders NC-80000x", "As-of earlier rate; kept, excluded from mismatch check"],
    ["Duplicates: 18 order rows and 25 item lines per batch; 12 item lines re-delivered", "Deterministic dedup; MERGE skips identical rows"],
], [FULL * 0.60, FULL * 0.40], first_bold=False)
story.append(traps)

story.append(P("4 · Viva one-liners", "h"))
gloss = table([
    ["Term", "Say it like this"],
    ["Medallion", "Bronze = as received, silver = clean and conformed, gold = business-ready; each layer can be rebuilt from the one before."],
    ["Idempotent", "Running the same batch twice leaves the tables exactly as after one run (proved: 0 inserted, 0 updated, no new version)."],
    ["MERGE guard", "Upsert that only accepts a newer updated_at, so late data cannot overwrite current data."],
    ["SCD Type 2", "Keeps every tier/country version with valid_from/valid_to, so an order joins the customer as they were on the order date."],
    ["Time travel", "Delta keeps old versions: VERSION AS OF reads the table as it was after batch 1. VACUUM deletes old files and limits this."],
    ["Quarantine", "Bad rows are never dropped silently: each is stored with a reason and its original record, and counted in dq_report."],
], [21 * mm, FULL - 21 * mm])
story.append(gloss)

# ======================================================================= PAGE 2
story.append(PageBreak())
story.append(P("5 · Modules: what was built and the test results", "h"))
mods = table([
    ["#", "Module", "Key design", "Result"],
    ["0", "Discovery", "Profiled all 8 files locally; platform chosen", "Data traps listed above"],
    ["1", "Foundation", "Silver + Gold lakehouses (idempotent script); inputs verified", "<font color='#1f7a3d'>✓</font> SHA-256 identical; 8/8 files readable, counts match"],
    ["2", "Config + setup", "Timestamp parser (3 formats), currency rule, quarantine + DQ helpers, DDL", "<font color='#1f7a3d'>✓ All pass</font>: S1, S2, S3–S5, O1, R4, D1; second setup run = 0 commits"],
    ["3", "Bronze", "STRING + lineage; replaceWhere on _source_file", "<font color='#1f7a3d'>✓ all pass</font> (one test bug fixed and re-verified): B1–B6, D1; re-run counts unchanged"],
    ["4", "Silver orders", "Parse → quarantine → latest per order → guarded MERGE", "<font color='#1f7a3d'>✓ all pass</font> (R1 expectation corrected and re-verified): S3–S12, <b>O4</b>; 945 orders; status mix = profile exactly"],
    ["5", "Items, SCD2, reference", "Items MERGE on (order_id, line_no); SCD2 on tier/country; snapshot sync", M5],
    ["6", "Gold", "As-of FX, point-in-time customer, revenue flag; full deterministic rebuild; reconciliation + DQ JSON export",
     "<font color='#1f7a3d'>✓ all 29 pass</font>: G1–G9, C5, C6, I5; every gold total equals an independent plain-Python calculation to the cent"],
    ["7", "Pipeline", "Fabric Data Pipeline <i>novacart_medallion</i>: batch_id parameter, 2 retries / 60 s, on-failure notify_failure → fail_run",
     "<font color='#1f7a3d'>✓ failure path</font> (batch_id = abc: setup failed 3×, rest skipped, alert recorded, run Failed). " + M7],
    ["8", "Evidence", "06_evidence: history, time travel, schema + CHECK, OPTIMIZE, VACUUM demo, CDF, incremental gold",
     "<font color='#1f7a3d'>✓ 11/11</font>: v3 = batch 1, v4 = batch 2, no re-run version; OPTIMIZE 13 files → 1; incremental gold = full rebuild (0 differences)"],
], [5 * mm, 25 * mm, 70 * mm, FULL - 100 * mm])
story.append(mods)

story.append(Spacer(1, 3))
left = [P("6 · Key design decisions (the “why”)", "h")] + bullets([
    "<b>Bronze keeps everything as text</b>: nothing is lost or silently converted; silver decides.",
    "<b>Re-run safety everywhere</b>: bronze replaces its own file slice; silver MERGE changes only what differs; "
    "quarantine and dq_report replace their own (entity, batch) rows. A second run = no change.",
    "<b>Late data</b>: update only if s.updated_at &gt; t.updated_at. Same timestamp: later batch wins only if content "
    "differs, so <b>batch 2 then 1 = batch 1 then 2</b> (O4).",
    "<b>SCD Type 2</b> on tier and country; name/e-mail are Type 1; first version from 1900-01-01; "
    "customer_sk = hash(customer_id, valid_from), so keys are stable across rebuilds.",
    "<b>Money is DECIMAL</b>, never float; <b>all time is UTC</b>.",
    "<b>Every rejected row</b> is in silver.quarantine with a reason and its original record; "
    "dq_report enforces rows in = out + quarantined + deduped before writing.",
])
right = [P("7 · Real-data results so far", "h")] + bullets([
    "silver.orders: batch 1 = 1418 rows in → 495 orders; batch 2 = 450 new, 137 updated → <b>945 orders</b>.",
    "Final status: delivered 666 · cancelled 107 · shipped 89 · placed 44 · paid 39 (the late copies were ignored).",
    "48 orders have a currency derived from the shipping country; 0 orders quarantined.",
    "Re-run of batch 2: <b>0 inserted, 0 updated</b>, and <b>no new table version</b>: Delta writes no commit for a "
    "MERGE that changes nothing (official history: v3 = batch 1, v4 = batch 2, latest = v4).",
    "Time travel: silver.orders VERSION AS OF 3 = 495 orders with exactly the batch-1 status mix; gold read “as of batch 1” "
    "= the batch-1 Python reference ($150,062.64, 64 mismatches).",
    "Bronze: 1418 / 1006 / 194 / 40 / 84 / 1164 rows per file, cross-checked by an independent count.",
    "Items: <b>1933 lines</b> (971 + 974 - 12 re-delivered); 10 + 8 orphans quarantined; 14 unknown-product lines flagged.",
    "dim_customer: <b>166 versions</b> for 130 customers (+ the -1 row); 5 CRM rows quarantined; no overlaps or gaps.",
    "Gold (final): revenue <b>$295,585.57</b> over 30 days (Electronics $223,943.51); top customer C0035; "
    "<b>13</b> payment mismatches; 5 payments for unknown orders. After batch 1 only: $150,062.64 over 15 days.",
]) + [P("8 · Fabric lessons learned", "h")] + bullets([
    "A child notebook started from another must share its default lakehouse.",
    "notebookutils.fs.head returns at most ~100 KB: read whole files with Spark instead.",
    "Every failed run reports the same generic error, so tests save PASS/FAIL details to a JSON file.",
])
t, _ = two_col(left, right)
story.append(t)

story.append(P("9 · Remaining stages", "h"))
todo = table([
    ["Module", "Plan"],
    ["7 Screens", "Screenshots: app.fabric.microsoft.com → <b>Monitor</b> → novacart_medallion run → View run details (batch 1, "
     "batch 2, abc failure). Trigger design in the note: file-arrival event vs fixed or windowed schedule."],
    ["Done", "Deliverables pack: <i>deliverables/NovaCart_Deliverables.pdf</i> (pipeline export, 4 monitored runs, DQ report, "
     "Part 5/6 evidence) and <i>evidence/EVIDENCE.md</i>."],
    ["9 Submission", "Test summary, submission checklist, AI-use disclosure per module, design note (≤ 1.5 pages), "
     "teardown (pause or delete the F2, clean the workspace)."],
], [21 * mm, FULL - 21 * mm])
story.append(todo)

story.append(P("10 · Design-note updates needed", "h"))
story += bullets([
    "Tools: <b>Fabric Lakehouse (OneLake + Delta), Fabric Spark, Data Pipelines</b> replace ADLS Gen2, Databricks, Workflows and Unity Catalog.",
    "History diagram: v3 = batch 1, v4 = batch 2, <b>no version for the batch 2 re-run</b> (not “v4: 0 changes”).",
    "Assumptions to add: payments for unknown orders are kept and excluded from mismatches; same-updated_at tie goes to the later batch "
    "only if content differs; if batch 2 runs before batch 1, its lines for batch-1 orders stay quarantined (auto-retry = future work).",
    "Payment mismatch: expected = order net total if paid/shipped/delivered, else 0 (a cancelled order must be fully refunded). "
    "The payments file is a full-month snapshot, so after batch 1 it already holds batch-2 payments (64 mismatches then, 13 at the end).",
])



doc = SimpleDocTemplate(OUT, pagesize=A4, leftMargin=12 * mm, rightMargin=12 * mm, topMargin=10 * mm,
                        bottomMargin=9 * mm, title="NovaCart Pipeline Cheatsheet", author="Bug Byts")


def footer(canvas, doc_):
    canvas.saveState()
    canvas.setFont("Uni", 6.5)
    canvas.setFillColor(MUTED)
    canvas.drawString(12 * mm, 5 * mm, "NovaCart medallion pipeline · Bug Byts · prepared with AI assistance (Claude); every result above comes from a real test run")
    canvas.drawRightString(A4[0] - 12 * mm, 5 * mm, f"page {doc_.page} of 2")
    canvas.restoreState()


doc.build(story, onFirstPage=footer, onLaterPages=footer)
print("written", OUT)
