"""Build docs/NovaCart_Design_Note.pdf (max 1.5 pages), the design note for the submission.

Usage (needs reportlab): python docs/make_design_note.py
"""
import os
from reportlab.graphics.shapes import Drawing, Rect, String, Line, Polygon
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.lib.fonts import addMapping
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FONTS = "/System/Library/Fonts/Supplemental/"
pdfmetrics.registerFont(TTFont("Uni", FONTS + "Arial Unicode.ttf"))
pdfmetrics.registerFont(TTFont("UniBold", FONTS + "Arial Bold.ttf"))
pdfmetrics.registerFont(TTFont("UniItalic", FONTS + "Arial Italic.ttf"))
pdfmetrics.registerFont(TTFont("UniBoldItalic", FONTS + "Arial Bold Italic.ttf"))
for b, i, f in [(0, 0, "Uni"), (1, 0, "UniBold"), (0, 1, "UniItalic"), (1, 1, "UniBoldItalic")]:
    addMapping("Uni", b, i, f)

INK, MUTED, ACCENT, RULE, SOFT = (colors.HexColor(c) for c in ("#1d2433", "#5b6475", "#0f6e6e", "#d5dbe3", "#eef3f6"))
BRONZE, SILVER, GOLD = colors.HexColor("#f6ebe0"), colors.HexColor("#eceff3"), colors.HexColor("#f8f1d8")
base = dict(fontName="Uni", textColor=INK, fontSize=7.9, leading=9.8)
S = {
    "title": ParagraphStyle("t", fontName="UniBold", fontSize=13.5, leading=16, textColor=colors.white),
    "h": ParagraphStyle("h", fontName="UniBold", fontSize=9.4, leading=11.5, textColor=ACCENT, spaceBefore=4, spaceAfter=2),
    "p": ParagraphStyle("p", **base, spaceAfter=2),
    "cell": ParagraphStyle("c", **{**base, "fontSize": 7.4, "leading": 9.1}),
    "cellb": ParagraphStyle("cb", **{**base, "fontName": "UniBold", "fontSize": 7.4, "leading": 9.1}),
    "box": ParagraphStyle("bx", **{**base, "fontSize": 7.1, "leading": 8.6, "alignment": 1}),
}
FULL = A4[0] - 22 * mm
P = lambda t, s="p": Paragraph(t, S[s])

story = []
title = Table([[P("NovaCart – Medallion Pipeline Design Note", "title"),
                Paragraph("Bug Byts · Microsoft Fabric (OneLake + Delta Lake)",
                          ParagraphStyle("r", fontName="Uni", fontSize=8, textColor=colors.white, alignment=2))]],
              colWidths=[FULL * 0.62, FULL * 0.38])
title.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, -1), ACCENT), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                           ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5)]))
story.append(title)

# ------------------------------------------------------------------ 1 tools
story.append(P("1 Tools chosen and why", "h"))
tools = [("OneLake lakehouses", "Bronze / Silver / Gold; raw input/ files kept, never edited"),
         ("Delta Lake", "MERGE · HISTORY · time travel · NOT NULL + CHECK · Change Data Feed"),
         ("Fabric Spark", "PySpark + Spark SQL notebooks; scales out, tiny F2 for the lab"),
         ("Data Pipelines", "batch_id parameter · 2 retries · on-failure path · run monitoring"),
         ("Entra ID + roles", "no keys or secrets anywhere; workspace roles for access")]
cells = [[P(f"<b>{t}</b><br/>{d}", "box") for t, d in tools]]
tt = Table(cells, colWidths=[FULL / 5] * 5)
tt.setStyle(TableStyle([("BOX", (i, 0), (i, 0), 0.6, ACCENT) for i in range(5)] +
                       [("BACKGROUND", (0, 0), (-1, -1), SOFT), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                        ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3)]))
story += [tt, Spacer(1, 2),
          P("One platform covers every requirement natively, and Delta keeps the Spark code close to a Databricks design, so "
            "every line stays explainable. <b>Considered:</b> Azure Databricks + ADLS Gen2 (our first plan; the student "
            "subscription was disabled and the trial account could not grant storage roles), Databricks Free Edition, "
            "AWS S3 + Glue + Step Functions. Cost: F2 capacity (~$0.36/h) resumed only while running, paused after.")]

# ------------------------------------------------------------------ 2 architecture diagram
story.append(P("2 Working pipeline architecture", "h"))
W, H = FULL, 64 * mm
d = Drawing(W, H)


def box(x, y, w, h, fill, title, lines, title_color=INK):
    d.add(Rect(x, y, w, h, fillColor=fill, strokeColor=ACCENT, strokeWidth=0.6, rx=3, ry=3))
    d.add(String(x + 4, y + h - 10, title, fontName="UniBold", fontSize=7.6, fillColor=title_color))
    for i, line in enumerate(lines):
        d.add(String(x + 4, y + h - 20 - i * 8.6, line, fontName="Uni", fontSize=6.5, fillColor=INK))


def arrow(x1, y1, x2, y2, dashed=False):
    ln = Line(x1, y1, x2, y2, strokeColor=MUTED, strokeWidth=0.8)
    if dashed:
        ln.strokeDashArray = [2, 2]
    d.add(ln)
    import math
    a = math.atan2(y2 - y1, x2 - x1)
    d.add(Polygon([x2, y2, x2 - 4 * math.cos(a - 0.4), y2 - 4 * math.sin(a - 0.4),
                   x2 - 4 * math.cos(a + 0.4), y2 - 4 * math.sin(a + 0.4)], fillColor=MUTED, strokeColor=MUTED))


# pipeline band
d.add(Rect(0, H - 22, W, 21, fillColor=SOFT, strokeColor=ACCENT, strokeWidth=0.5, rx=3, ry=3))
d.add(String(4, H - 10, "Data Pipeline novacart_medallion (parameter batch_id = 1 | 2):  setup → bronze → silver_reference → "
                        "silver_orders → silver_items → gold", fontName="UniBold", fontSize=6.8, fillColor=INK))
d.add(String(4, H - 19, "each step: 2 retries, 60 s apart  ·  any failure: later steps skipped, gold Failed/Skipped → "
                        "notify_failure (alert table + file) → fail_run (run marked Failed)", fontName="Uni", fontSize=6.6, fillColor=INK))
cw = (W - 3 * 8) / 4
top = H - 28
bh = 92
box(0, top - bh, cw, bh, colors.white, "Source files (input/)",
    ["orders_batch_{1,2}.csv", "order_items_batch_1_json.txt", "order_items_batch_2_jsonl.txt", "customers_changes.csv",
     "products.csv · fx_rates.csv", "payments_json.txt", "batch files = incremental", "others = full snapshots"])
box(cw + 8, top - bh, cw, bh, BRONZE, "BRONZE  as received",
    ["every column as STRING", "+ _source_file, _batch_id,", "   _ingested_at", "replaces only its own file", "   slice (replaceWhere)",
     "malformed → _corrupt_record", "6 tables · re-run safe"])
box(2 * (cw + 8), top - bh, cw, bh, SILVER, "SILVER  clean, conformed, UTC",
    ["3 timestamp formats → UTC", "currency: upper / from country", "bad rows → quarantine + reason", "dedup: latest updated_at",
     "MERGE only if newer version", "customers → SCD Type 2", "snapshots synced (no-op = no", "   commit) · CDF on orders"])
box(3 * (cw + 8), top - bh, cw, bh, GOLD, "GOLD  business-ready",
    ["fact_order_line: local + USD,", "   FX as-of, point-in-time sk", "daily_revenue · revenue_by_", "   category · top_customers",
     "payment_mismatches (> 0.50)", "reconciliation (3 totals agree)", "full deterministic rebuild"])
for i in range(3):
    arrow((i + 1) * cw + i * 8, top - bh / 2, (i + 1) * (cw + 8), top - bh / 2)
box(2 * (cw + 8), 0, cw, 26, colors.white, "silver.quarantine", ["entity · reason · original record"])
box(cw + 8, 0, cw, 26, colors.white, "silver.dq_report", ["in / out / quarantined / deduped"])
box(3 * (cw + 8), 0, cw, 26, colors.white, "exports/dq/dq_batch_N.json", ["per-batch DQ report (gold Files)"])
arrow(2 * (cw + 8) + cw / 2, top - bh, 2 * (cw + 8) + cw / 2, 26, dashed=True)
arrow(cw + 8 + cw / 2, top - bh, cw + 8 + cw / 2, 26, dashed=True)
arrow(3 * (cw + 8) + cw / 2, top - bh, 3 * (cw + 8) + cw / 2, 26, dashed=True)
d.add(String(4, 10, "Trigger: file lands in landing/batch=N/", fontName="UniItalic", fontSize=6.5, fillColor=MUTED))
d.add(String(4, 2, "(event) + daily schedule fallback", fontName="UniItalic", fontSize=6.5, fillColor=MUTED))
story.append(d)

# ------------------------------------------------------------------ 3 correctness
story.append(P("3 Correct after batch 1, after batch 2, and on a re-run", "h"))
story.append(P(
    "<b>Newer wins:</b> NC-100473 is <i>paid</i> (updated 15 Sep) in batch 1 and <i>delivered</i> (19 Sep) in batch 2 → updated. "
    "<b>Late copy ignored:</b> NC-100276 is <i>cancelled</i> (10 Sep) in batch 1; batch 2 carries an older <i>paid</i> copy (9 Sep) → "
    "ignored, so it never re-enters revenue (10 such copies in the data). <b>silver.orders history:</b> v0–v2 create + constraints, "
    "<b>v3 = batch 1</b> (495 inserted), <b>v4 = batch 2</b> (450 inserted, 137 updated); <b>batch 2 again: 0 changes and no new "
    "version</b>. <font face='Courier'>SELECT * FROM silver.orders VERSION AS OF 3</font> returns the state right after batch 1. "
    "Bronze replaces its own file slice, silver's guard is false for equal-or-older versions, quarantine and dq_report replace their own "
    "(entity, batch) rows, gold is rebuilt deterministically. Gold after batch 1 ($150,062.64) and after batch 2 ($295,585.57) equal an "
    "independent plain-Python calculation to the cent."))

# ------------------------------------------------------------------ 4 assumptions
story.append(P("4 Assumptions where the spec was unclear", "h"))
A = [
    ("Timestamps", "No offset ⇒ UTC; dd/MM/yyyy parsed explicitly; order_date = UTC date of order_ts",
     "FX", "Order-date rate, else latest earlier rate; USD = 1; none ⇒ fx_missing (excluded, counted)"),
    ("Same updated_at", "Later batch wins only if the content differs, so batch 2 then 1 = 1 then 2",
     "Item key", "(order_id, line_no); a later batch replaces a line only if it differs"),
    ("Returns", "qty forced to −|qty|; dated by the parent order", "Discount", "NULL = 0; non-numeric or outside 0–100 ⇒ quarantine"),
    ("Bad values", "Unknown status or currency ⇒ quarantine; UK = GB; codes case-insensitive",
     "Orphan items", "Checked against all orders loaded so far ⇒ ORDER_NOT_FOUND"),
    ("SCD2", "Tier/country Type 2; name/e-mail Type 1; first version from 1900-01-01; key = hash(id, valid_from)",
     "Payments", "success +, refunded −, failed ignored; USD at the order-date rate"),
    ("Mismatch", "Expected = order total if paid/shipped/delivered, else 0; flagged if |difference| > 0.50",
     "Unknown-order payments", "Kept in silver, excluded from mismatches, counted (5)"),
    ("Snapshots", "Customers, products, FX, payments are full files reloaded each batch; payments cover the whole month",
     "Top 10", "Excludes the unknown customer (-1); ties broken by customer_id"),
]
rows = [[P(a, "cellb"), P(b, "cell"), P(c, "cellb"), P(e, "cell")] for a, b, c, e in A]
at = Table(rows, colWidths=[22 * mm, FULL / 2 - 22 * mm, 26 * mm, FULL / 2 - 26 * mm])
at.setStyle(TableStyle([("VALIGN", (0, 0), (-1, -1), "TOP"), ("LINEBELOW", (0, 0), (-1, -1), 0.4, RULE),
                        ("LINEABOVE", (0, 0), (-1, 0), 0.8, INK), ("LINEBELOW", (0, -1), (-1, -1), 0.8, INK),
                        ("TOPPADDING", (0, 0), (-1, -1), 1.5), ("BOTTOMPADDING", (0, 0), (-1, -1), 1.5)]))
story.append(at)

# ------------------------------------------------------------------ 5 trigger + next
story.append(P("5 Trigger design", "h"))
story.append(P("<b>File-arrival event</b> (storage-event trigger on landing/batch=N/, batch_id from the folder) because batch 2's arrival "
               "time is unknown: it runs exactly when data lands and never idles. A <b>fixed schedule</b> would run too early or too "
               "late; a <b>windowed schedule</b> suits regular time slices with backfill but still polls. A daily schedule stays as a "
               "safety net, which is harmless because every step is idempotent."))
story.append(P("<b><font color='#0f6e6e'>Next with more time:</font></b> auto-retry of quarantined orphan lines · Change Data Feed on "
               "order_items and incremental gold in production (tested: 0 differences from the full rebuild) · Activator event trigger "
               "and an Outlook/Teams alert · Fabric Git integration + deployment pipelines (dev/test/prod) with CI tests · DQ "
               "expectations as code · Power BI semantic model and a DQ dashboard on gold."))

out = os.path.join(ROOT, "docs", "NovaCart_Design_Note.pdf")
SimpleDocTemplate(out, pagesize=A4, leftMargin=11 * mm, rightMargin=11 * mm, topMargin=9 * mm, bottomMargin=9 * mm,
                  title="NovaCart Medallion Pipeline Design Note", author="Bug Byts").build(story)
print("written", os.path.relpath(out, ROOT))
