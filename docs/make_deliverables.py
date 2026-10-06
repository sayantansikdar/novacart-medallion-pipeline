"""Build deliverables/NovaCart_Deliverables.pdf from the exported files.

Inputs (already in the repo):
  deliverables/raw/pipeline_definition.json, pipeline_runs.json, dq_batch_1.json, dq_batch_2.json, alerts/
  evidence/run_logs/*.json            activity-level logs of each monitored run
  deliverables/raw/module8.json       results of the 06_evidence notebook (Part 5 / Part 6)
Usage (needs reportlab): python docs/make_deliverables.py
"""
import glob, json, os
from datetime import datetime
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.fonts import addMapping
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak, KeepTogether

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW = os.path.join(ROOT, "deliverables", "raw")
FONTS = "/System/Library/Fonts/Supplemental/"
pdfmetrics.registerFont(TTFont("Uni", FONTS + "Arial Unicode.ttf"))
pdfmetrics.registerFont(TTFont("UniBold", FONTS + "Arial Bold.ttf"))
pdfmetrics.registerFont(TTFont("UniItalic", FONTS + "Arial Italic.ttf"))
pdfmetrics.registerFont(TTFont("UniBoldItalic", FONTS + "Arial Bold Italic.ttf"))
for b, i, f in [(0, 0, "Uni"), (1, 0, "UniBold"), (0, 1, "UniItalic"), (1, 1, "UniBoldItalic")]:
    addMapping("Uni", b, i, f)

INK, MUTED, ACCENT, RULE, SOFT = (colors.HexColor(c) for c in ("#1d2433", "#5b6475", "#0f6e6e", "#d5dbe3", "#eef3f6"))
GREEN, RED, AMBER = colors.HexColor("#1f7a3d"), colors.HexColor("#b42318"), colors.HexColor("#9a5b00")
base = dict(fontName="Uni", textColor=INK, fontSize=8.2, leading=10.4)
S = {
    "title": ParagraphStyle("t", fontName="UniBold", fontSize=16, leading=20, textColor=INK),
    "sub": ParagraphStyle("s", **{**base, "textColor": MUTED}),
    "h1": ParagraphStyle("h1", fontName="UniBold", fontSize=12, leading=15, textColor=ACCENT, spaceBefore=6, spaceAfter=3),
    "h2": ParagraphStyle("h2", fontName="UniBold", fontSize=9.4, leading=12, textColor=INK, spaceBefore=6, spaceAfter=2),
    "p": ParagraphStyle("p", **base, spaceAfter=3),
    "cell": ParagraphStyle("c", **{**base, "fontSize": 7.3, "leading": 9}),
    "head": ParagraphStyle("hd", **{**base, "fontName": "UniBold", "fontSize": 7.3, "leading": 9, "textColor": colors.white}),
    "mono": ParagraphStyle("m", fontName="Courier", fontSize=7, leading=8.6, textColor=INK),
}
FULL = A4[0] - 24 * mm
P = lambda t, s="p": Paragraph(t, S[s])


def status(s):
    c = {"Succeeded": GREEN, "Completed": GREEN, "PASS": GREEN, "Failed": RED, "FAIL": RED, "Skipped": AMBER}.get(s)
    return f"<font color='{c.hexval().replace('0x', '#')}'>{s}</font>" if c else str(s)


def table(rows, widths=None, header=True):
    data = [[P(str(c), "head" if header and i == 0 else "cell") for c in r] for i, r in enumerate(rows)]
    t = Table(data, colWidths=widths, repeatRows=1 if header else 0)
    cmds = [("VALIGN", (0, 0), (-1, -1), "TOP"), ("LINEBELOW", (0, 0), (-1, -1), 0.4, RULE),
            ("LEFTPADDING", (0, 0), (-1, -1), 3), ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 1.6), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.6)]
    if header:
        cmds.append(("BACKGROUND", (0, 0), (-1, 0), ACCENT))
    cmds += [("BACKGROUND", (0, i), (-1, i), SOFT) for i in range(2, len(rows), 2)]
    t.setStyle(TableStyle(cmds))
    return t


def when(ts):
    return (ts or "")[:19].replace("T", " ")


def secs(a, b):
    try:
        f = lambda s: datetime.fromisoformat(s[:26])
        return f"{(f(b) - f(a)).total_seconds() / 60:.1f} min"
    except Exception:
        return ""


story = [P("NovaCart Order Analytics: Submission Deliverables", "title"),
         P("Team Bug Byts · Microsoft Fabric workspace NovaCart_HCL · pipeline novacart_medallion · "
           "exported from Fabric on 6 Oct 2026 · repo github.com/sayantansikdar/novacart-medallion-pipeline", "sub"),
         Spacer(1, 4),
         P("Contents: <b>1</b> pipeline definition (export) · <b>2</b> monitored runs (batch 1, batch 2, batch 2 again, "
           "deliberate failure) · <b>3</b> data-quality report per batch · <b>4</b> table reliability evidence (Part 5) "
           "and extras (Part 6). Raw exports are in <i>deliverables/raw/</i> and <i>evidence/run_logs/</i>; "
           "screenshots of the same runs: Fabric → Monitor → novacart_medallion → View run details.")]

# ------------------------------------------------------------------ 1 pipeline definition
d = json.load(open(os.path.join(RAW, "pipeline_definition.json")))["properties"]
cfg = json.load(open(os.path.join(ROOT, "pipeline", "novacart_pipeline.json")))
nb = {s["name"]: s["notebook"] for s in cfg["steps"]}
nb["notify_failure"] = cfg["on_failure"]["notify_notebook"]
story += [P("1 · Pipeline definition (exported from Fabric)", "h1"),
          P("Parameter: " + ", ".join(f"<b>{k}</b> ({v['type']}, default '{v.get('defaultValue')}')"
                                      for k, v in d.get("parameters", {}).items()) +
            ". Every notebook step receives <i>batch_id = @pipeline().parameters.batch_id</i>.")]
rows = [["Step", "Type", "Notebook", "Runs after (condition)", "Retries", "Retry interval", "Timeout"]]
for a in d["activities"]:
    pol = a.get("policy", {})
    rows.append([f"<b>{a['name']}</b>", a["type"], nb.get(a["name"], "—"),
                 ", ".join(f"{x['activity']} ({' or '.join(x['dependencyConditions'])})" for x in a.get("dependsOn", [])) or "start",
                 pol.get("retry", "—"), f"{pol['retryIntervalInSeconds']} s" if "retryIntervalInSeconds" in pol else "—",
                 pol.get("timeout", "—")])
story += [table(rows, [24 * mm, 26 * mm, 30 * mm, 50 * mm, 13 * mm, 20 * mm, FULL - 163 * mm]),
          Spacer(1, 3),
          P("<b>Flow:</b> setup → bronze → silver_reference → silver_orders → silver_items → gold. If any step fails "
            "(after its retries) the steps after it are skipped, so <i>gold</i> ends Failed or Skipped; that starts "
            "<b>notify_failure</b> (records the alert in silver.pipeline_alerts and an alert file), then <b>fail_run</b> "
            "marks the whole run Failed in Monitor. Full JSON: <i>deliverables/raw/pipeline_definition.json</i>.")]

# ------------------------------------------------------------------ 2 monitored runs
logs = {json.load(open(f))["run_id"]: json.load(open(f)) for f in sorted(glob.glob(os.path.join(ROOT, "evidence", "run_logs", "*.json")))}
runs = sorted(json.load(open(os.path.join(RAW, "pipeline_runs.json"))), key=lambda r: r["startTimeUtc"])
label = {}
seen = set()
for r in runs:
    b = logs.get(r["id"], {}).get("batch_id", "?")
    label[r["id"]] = f"batch {b}" + (" (again)" if b in seen and b != "abc" else "") if b != "abc" else "batch_id = abc (deliberate failure)"
    seen.add(b)
story += [P("2 · Monitored runs", "h1"),
          table([["Run", "Run id", "Status", "Start (UTC)", "End (UTC)", "Duration"]] +
                [[label[r["id"]], r["id"][:8] + "…", status(r["status"]), when(r["startTimeUtc"]), when(r["endTimeUtc"]),
                  secs(r["startTimeUtc"], r["endTimeUtc"])] for r in runs],
                [46 * mm, 20 * mm, 20 * mm, 34 * mm, 34 * mm, FULL - 154 * mm])]
for r in runs:
    acts = logs.get(r["id"], {}).get("activity_runs", [])
    if not acts:
        continue
    rows = [["Step", "Status", "Start (UTC)", "Duration", "Attempt", "Error"]]
    attempt = {}
    for a in acts:                                      # Fabric leaves retryAttempt empty, so count attempts per step
        attempt[a["activityName"]] = attempt.get(a["activityName"], 0) + 1
        rows.append([a["activityName"], status(a["status"]), when(a.get("activityRunStart")),
                     f"{(a.get('durationInMs') or 0) / 1000:.0f} s", attempt[a["activityName"]],
                     ((a.get("error") or {}).get("message") or "")[:110]])
    story.append(KeepTogether([P(f"Run: {label[r['id']]} — {status(r['status'])}", "h2"),
                               table(rows, [30 * mm, 20 * mm, 34 * mm, 18 * mm, 15 * mm, FULL - 117 * mm])]))
story.append(P("The failed run shows the on-failure path working: <i>setup</i> rejected batch_id 'abc' three times "
               "(first attempt + 2 retries, about 60 s apart), every later step was skipped (not started), "
               "<i>notify_failure</i> recorded the alert and <i>fail_run</i> marked the run Failed."))
for f in sorted(glob.glob(os.path.join(RAW, "alerts", "*.json"))):
    al = json.load(open(f))
    story.append(P(f"Recorded alert: <i>{al['alerted_at']}</i> — {al['message']}"))

# ------------------------------------------------------------------ 3 DQ report
story += [PageBreak(), P("3 · Data-quality report", "h1"),
          P("Every step writes row counts to silver.dq_report and refuses to write unless "
            "<b>rows in = rows out + quarantined + deduplicated</b>. Each gold run exports its batch's report as JSON "
            "(<i>deliverables/raw/dq_batch_N.json</i>). Inserted/updated are the MERGE results in silver.")]
for b in (1, 2):
    q = json.load(open(os.path.join(RAW, f"dq_batch_{b}.json")))
    totals = [r for r in q["dq_report"] if r["rule"] == "_TOTAL"]
    rules = [r for r in q["dq_report"] if r["rule"] != "_TOTAL"]
    order = {"bronze": 0, "silver": 1, "gold": 2}
    totals.sort(key=lambda r: (order[r["layer"]], r["entity"]))
    fmt = lambda v: "" if v is None else v
    rows = [["Layer", "Entity", "Rows in", "Rows out", "Quarantined", "Deduplicated", "Inserted", "Updated", "Balanced"]]
    for r in totals:
        ok = r["rows_in"] == r["rows_out"] + r["rows_quarantined"] + r["rows_deduped"]
        rows.append([r["layer"], r["entity"], r["rows_in"], r["rows_out"], r["rows_quarantined"], r["rows_deduped"],
                     fmt(r["rows_inserted"]), fmt(r["rows_updated"]), status("PASS" if ok else "FAIL")])
    block = [P(f"Batch {b}", "h2"), table(rows, [16 * mm, 32 * mm, 17 * mm, 17 * mm, 20 * mm, 22 * mm, 17 * mm, 17 * mm, FULL - 158 * mm])]
    if rules:
        block += [Spacer(1, 2), table([["Quarantine rule", "Layer / entity", "Rows quarantined"]] +
                                      [[r["rule"], f"{r['layer']} / {r['entity']}", r["rows_quarantined"]] for r in rules],
                                      [60 * mm, 60 * mm, FULL - 120 * mm])]
    block += [Spacer(1, 2), table([["Reconciliation check", "Value", "Expected", "Result"]] +
                                  [[c["check_name"], c["value"], c["expected"] or "—",
                                    status("PASS") if c["passed"] else ("info" if c["passed"] is None else status("FAIL"))]
                                   for c in q["reconciliation"]], [92 * mm, 30 * mm, 30 * mm, FULL - 152 * mm])]
    story.append(KeepTogether(block))
story.append(P("<b>Batch 2 shows 0 inserted / 0 updated</b> because each batch's DQ rows are replaced on every run and the "
               "last run of batch 2 was the deliberate re-run, which changed nothing. The first batch 2 run inserted "
               "450 orders and updated 137 (silver.orders history, version 4) and inserted 962 order lines."))
story.append(P("<b>Reading the numbers.</b> Snapshot files (customers, products, FX, payments) are reloaded in every batch, "
               "so batch 2 shows them again with 0 inserted / 0 updated (nothing changed). Deduplicated = exact duplicates and "
               "older versions within the batch; for dim_customer it also counts CRM rows that start no new SCD2 version "
               "(e-mail/name-only changes). The 5 CRM rows without updated_at and the order lines whose order does not exist "
               "(NC-990xxx) are the quarantined rows. After batch 1 the payments snapshot already contains the whole month, "
               "so many payments point at orders that only arrive in batch 2; they are counted, not compared."))

# ------------------------------------------------------------------ 4 evidence
m8 = os.path.join(RAW, "module8.json")
if not os.path.exists(m8):
    story += [P("4 · Table reliability evidence (Part 5) and extras (Part 6)", "h1"),
              P("<i>Pending: the evidence notebook (06_evidence) is being re-run; this section is added when it finishes.</i>")]
else:
    e = json.loads(open(m8).read(), strict=False)
    ev = e["evidence"]
    story += [PageBreak(), P("4 · Table reliability evidence (Part 5) and extras (Part 6)", "h1"),
              P(f"Produced by the notebook <i>06_evidence</i> on the official tables; <b>{e['passed']} of "
                f"{e['passed'] + len(e['failed'])} automatic checks passed</b>. Full write-up: <i>evidence/EVIDENCE.md</i>.")]
    story += [P("5.1 Change history of silver.orders (batch 1, batch 2, batch 2 again)", "h2"),
              table([["Version", "Timestamp (UTC)", "Operation", "userMetadata", "Inserted", "Updated"]] +
                    [[h["version"], when(h["timestamp"]), h["operation"], h["userMetadata"] or "", h["rows_inserted"] or "",
                      h["rows_updated"] or ""] for h in ev["history"]],
                    [16 * mm, 36 * mm, 36 * mm, 50 * mm, 18 * mm, FULL - 156 * mm]),
              P("v3 = batch 1, v4 = batch 2. <b>Batch 2 run again produced no new version</b>: the MERGE found nothing newer, "
                "and Delta writes no commit for a MERGE that changes nothing (the re-run is in the run list above and in the DQ "
                "report with 0 inserted, 0 updated).")]
    p = ev["point_in_time"]
    story += [P("5.2 Point-in-time read (state right after batch 1)", "h2"),
              P(f"<font name='Courier'>{p['query']}</font> → <b>{p['rows_then']} orders</b> (now {p['rows_now']}); "
                f"same result with TIMESTAMP AS OF '{p['timestamp']}'. Status mix then: "
                + ", ".join(f"{r['status']} {r['count']}" for r in p["status_then"]) + ".")]
    se, ck = ev["schema_enforcement"], ev["check_constraint"]
    story += [P("5.3 Schema enforcement", "h2"),
              P(f"<b>Attempt:</b> {se['attempt']}. <b>Result:</b> rejected — <i>{se['error'][:300]}</i> "
                f"Table version {se['version_before']} → {se['version_after']} (unchanged)."),
              P("<b>What happened:</b> Delta keeps the table schema in its transaction log and validates every write against it; "
                "a DataFrame with an unknown column is refused as a whole before anything is committed. Adding the column would "
                "need an explicit mergeSchema opt-in, which silver never uses (only bronze does, so new source columns are kept "
                f"there for review). <b>CHECK constraint:</b> {ck['attempt']} → <i>{ck['error'][:220]}</i>")]
    o = ev["optimize"]
    story += [P("5.4 Storage optimisation on the fact table", "h2"),
              table([["", "Files", "Size (bytes)", "Rows", "Sum of net_usd"],
                     ["Before", o["before"]["numFiles"], o["before"]["sizeInBytes"], o["data_before"]["rows"], o["data_before"]["net_usd"]],
                     ["After", o["after"]["numFiles"], o["after"]["sizeInBytes"], o["data_after"]["rows"], o["data_after"]["net_usd"]]],
                    [20 * mm, 25 * mm, 30 * mm, 25 * mm, FULL - 100 * mm]),
              P("OPTIMIZE commits in the table history: " + "; ".join(
                  f"v{c['version']} ({when(c['timestamp'])}): <b>{c['files_removed']} files → {c['files_added']}</b>"
                  for c in o.get("optimize_commits", [])) +
                "  (the first one compacted the files written by the gold rebuild; later runs had nothing left to compact)."),
              P(f"<font name='Courier'>{o['command']}</font>: same data, fewer files. <b>Partition</b> only large tables by a "
                "low-cardinality column every query filters on (fact at scale: order_date/month, ~1 GB per partition); this data is "
                "a few hundred KB, so partitions would just create tiny files. Use <b>Z-order/clustering</b> for selective "
                "high-cardinality filters (customer_id, product_id); Fabric's V-Order sorts and compresses for fast reads. "
                f"<b>VACUUM warning:</b> on a throwaway copy, after VACUUM RETAIN 0 HOURS reading version 0 failed "
                f"(<i>{ev['vacuum']['after_vacuum'][:160]}</i>): VACUUM permanently removes the files old versions need, which "
                "would break the point-in-time read and change feed above. Keep the default 7-day retention or longer.")]
    c = ev["cdf"]
    story += [P("6.1 Row-level changes in batch 2 (Change Data Feed on silver.orders)", "h2"),
              P(f"Version {c['version']}: " + ", ".join(f"<b>{k}</b> {v}" for k, v in c["by_change_type"].items()) + "."),
              table([["Status before", "Status after", "Orders"]] + [[t["status_before"], t["status_after"], t["count"]]
                                                                     for t in c["status_transitions"]],
                    [40 * mm, 40 * mm, 25 * mm])]
    i = ev["incremental_gold"]
    story += [P("6.2 Incremental gold", "h2"),
              P(f"From gold as of batch 1, only the <b>{i['affected_orders']}</b> orders touched by batch 2 were rebuilt "
                f"(<b>{i['lines_rebuilt']}</b> of {i['lines_total']} lines): <b>{i['differences_vs_full_rebuild']} differences</b> "
                "from the full rebuild. Valid while reference tables are unchanged in the batch "
                f"(changed: {i['reference_tables_changed_in_batch_2'] or 'none'}); otherwise rebuild in full."),
              P("6.3 Automatic ingestion", "h2"),
              P("A Fabric storage-event trigger on the landing folder (landing/batch=N/) starts the pipeline with batch_id from "
                "the folder name as soon as files arrive, with a daily schedule as a safety net (re-runs are harmless). For many "
                "small files, Structured Streaming with a checkpoint and trigger(availableNow=True) processes each new file exactly "
                "once (Fabric's equivalent of Auto Loader). <b>Trigger choice:</b> fixed schedule = simple but batch 2's arrival "
                "time is unknown; windowed schedule = good for regular time slices with backfill, still polls; "
                "<b>file-arrival event = chosen</b>, runs exactly when a batch lands."),
              P("All checks", "h2"),
              table([["Result", "Check"]] + [[status("PASS" if r["passed"] else "FAIL"), r["test"]] for r in e["results"]],
                    [16 * mm, FULL - 16 * mm])]


def footer(c, doc):
    c.saveState()
    c.setFont("Uni", 6.5)
    c.setFillColor(MUTED)
    c.drawString(12 * mm, 6 * mm, "NovaCart deliverables · Bug Byts · generated from Fabric exports with AI assistance (Claude)")
    c.drawRightString(A4[0] - 12 * mm, 6 * mm, f"page {doc.page}")
    c.restoreState()


out = os.path.join(ROOT, "deliverables", "NovaCart_Deliverables.pdf")
SimpleDocTemplate(out, pagesize=A4, leftMargin=12 * mm, rightMargin=12 * mm, topMargin=11 * mm, bottomMargin=11 * mm,
                  title="NovaCart Submission Deliverables", author="Bug Byts").build(story, onFirstPage=footer, onLaterPages=footer)
print("written", os.path.relpath(out, ROOT))
