"""
Render docs/star_schema.png and docs/star_schema.pdf - the Divvy lakehouse star schema.

Laid out by hand rather than by an auto-layout tool, because the point of a star
schema is the shape: the two transaction facts in the middle, the two conformed
dimensions (dim_date, dim_rider) sitting between them where both can reach them,
and the trip-only dimensions out on the left.

    python tools/render_star_schema.py

Needs matplotlib. Writes a 200 dpi PNG and a vector PDF next to the design doc;
the PDF is the one that goes in the submission zip.
"""

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

HERE = os.path.dirname(os.path.abspath(__file__))
DOCS = os.path.normpath(os.path.join(HERE, "..", "docs"))
OUT_PNG = os.path.join(DOCS, "star_schema.png")
OUT_PDF = os.path.join(DOCS, "star_schema.pdf")

# ----------------------------------------------------------------- palette --
INK = "#1f2430"
MUTED = "#6b7280"
LINE = "#94a3b8"
STYLES = {
    "fact": dict(head="#1d4e89", body="#eef4fb", edge="#1d4e89"),
    "dim": dict(head="#2f6f4e", body="#edf6f0", edge="#2f6f4e"),
    "extra": dict(head="#8a5216", body="#fdf3e5", edge="#8a5216"),
}
KEYC = {"PK": "#b45309", "NK": "#6b7280", "FK": "#1d4e89", "": MUTED}

ROW_H = 0.26
HEAD_H = 0.56
PAD = 0.16

# ------------------------------------------------------------------ tables --
# (name, kind, centre-x, top-y, width, [(key, column, type), ...])
TABLES = [
    ("dim_time", "dim", 3.0, 14.0, 4.4, [
        ("PK", "time_key", "int"),
        ("", "hour_24", "int"),
        ("", "hour_label", "string"),
        ("", "time_of_day", "string"),
        ("", "is_peak_commute", "boolean"),
    ]),
    ("dim_station", "dim", 2.5, 8.2, 4.4, [
        ("PK", "station_key", "int"),
        ("NK", "station_id", "string"),
        ("", "station_name", "string"),
        ("", "latitude", "double"),
        ("", "longitude", "double"),
    ]),
    ("dim_date", "dim", 12.2, 14.9, 4.4, [
        ("PK", "date_key", "int"),
        ("", "full_date", "date"),
        ("", "year", "int"),
        ("", "quarter", "int"),
        ("", "month", "int"),
        ("", "month_name", "string"),
        ("", "day_of_month", "int"),
        ("", "day_of_week", "int"),
        ("", "day_name", "string"),
        ("", "week_of_year", "int"),
        ("", "is_weekend", "boolean"),
        ("", "year_month", "string"),
        ("", "year_quarter", "string"),
        ("", "month_start_date", "date"),
    ]),
    ("dim_rider", "dim", 12.2, 4.5, 4.4, [
        ("PK", "rider_key", "int"),
        ("NK", "rider_id", "int"),
        ("", "first_name", "string"),
        ("", "last_name", "string"),
        ("", "address", "string"),
        ("", "birthday", "date"),
        ("", "account_start_date", "date"),
        ("", "account_end_date", "date"),
        ("", "is_member", "boolean"),
        ("", "rider_type", "string"),
        ("", "age_at_account_start", "int"),
        ("", "age_band_at_account_start", "string"),
        ("", "account_tenure_months", "int"),
        ("", "is_account_open", "boolean"),
    ]),
    ("fact_trip", "fact", 9.0, 10.1, 5.1, [
        ("DD", "trip_id", "string"),
        ("FK", "rider_key", "int"),
        ("FK", "start_station_key", "int"),
        ("FK", "end_station_key", "int"),
        ("FK", "start_date_key", "int"),
        ("FK", "end_date_key", "int"),
        ("FK", "start_time_key", "int"),
        ("FK", "end_time_key", "int"),
        ("", "rideable_type", "string"),
        ("", "started_at", "timestamp"),
        ("", "ended_at", "timestamp"),
        ("", "duration_seconds", "int"),
        ("", "duration_minutes", "decimal(10,2)"),
        ("", "rider_age_at_trip", "int"),
        ("", "rider_age_band_at_trip", "string"),
        ("", "is_member", "boolean"),
        ("", "rider_type", "string"),
    ]),
    ("fact_payment", "fact", 17.0, 10.1, 5.1, [
        ("DD", "payment_id", "int"),
        ("FK", "rider_key", "int"),
        ("FK", "date_key", "int"),
        ("", "payment_date", "date"),
        ("", "amount", "decimal(10,2)"),
        ("", "rider_age_at_payment", "int"),
    ]),
    ("fact_rider_monthly", "extra", 18.6, 6.6, 5.1, [
        ("FK", "rider_key", "int"),
        ("FK", "month_date_key", "int"),
        ("", "month_start_date", "date"),
        ("", "year_month", "string"),
        ("", "year", "int"),
        ("", "quarter", "int"),
        ("", "month", "int"),
        ("", "rides_in_month", "int"),
        ("", "ride_minutes_in_month", "decimal(12,2)"),
        ("", "payments_in_month", "int"),
        ("", "amount_paid_in_month", "decimal(12,2)"),
    ]),
    ("agg_rider_spend_vs_rides", "extra", 25.1, 6.6, 5.3, [
        ("FK", "rider_key", "int"),
        ("", "rider_type", "string"),
        ("", "is_member", "boolean"),
        ("", "age_at_account_start", "int"),
        ("", "age_band_at_account_start", "string"),
        ("", "first_active_month", "date"),
        ("", "last_active_month", "date"),
        ("", "months_observed", "int"),
        ("", "months_active", "int"),
        ("", "total_rides", "int"),
        ("", "total_ride_minutes", "decimal(12,2)"),
        ("", "total_paid", "decimal(12,2)"),
        ("", "avg_rides_per_month", "decimal(10,2)"),
        ("", "avg_spend_per_month", "decimal(12,2)"),
        ("", "spend_per_ride", "decimal(12,2)"),
        ("", "rides_per_month_band", "string"),
    ]),
]

fig, ax = plt.subplots(figsize=(28.6, 17.6))
ax.set_xlim(0, 28.6)
ax.set_ylim(-1.9, 15.5)
ax.axis("off")
fig.patch.set_facecolor("white")

box = {}


def draw_table(name, kind, cx, top, width, rows):
    s = STYLES[kind]
    height = HEAD_H + len(rows) * ROW_H + PAD
    left, right = cx - width / 2.0, cx + width / 2.0
    bottom = top - height

    ax.add_patch(FancyBboxPatch(
        (left, bottom), width, height,
        boxstyle="round,pad=0,rounding_size=0.12",
        facecolor=s["body"], edgecolor=s["edge"], linewidth=1.4, zorder=3))
    ax.add_patch(FancyBboxPatch(
        (left, top - HEAD_H), width, HEAD_H,
        boxstyle="round,pad=0,rounding_size=0.12",
        facecolor=s["head"], edgecolor=s["head"], linewidth=1.4, zorder=4))
    # square off the underside of the rounded header
    ax.add_patch(FancyBboxPatch(
        (left, top - HEAD_H), width, HEAD_H / 2.0,
        boxstyle="square,pad=0",
        facecolor=s["head"], edgecolor=s["head"], linewidth=0, zorder=4))

    ax.text(cx, top - HEAD_H / 2.0, name, ha="center", va="center",
            color="white", fontsize=12.5, fontweight="bold",
            family="DejaVu Sans", zorder=5)

    y = top - HEAD_H - ROW_H / 2.0 - 0.03
    for key, col, typ in rows:
        if key:
            ax.text(left + 0.14, y, key, ha="left", va="center",
                    fontsize=6.4, color=KEYC.get(key, MUTED), fontweight="bold",
                    family="DejaVu Sans", zorder=5)
        ax.text(left + 0.62, y, col, ha="left", va="center", fontsize=8.1,
                color=INK, family="DejaVu Sans Mono",
                fontweight="bold" if key else "normal", zorder=5)
        ax.text(right - 0.14, y, typ, ha="right", va="center", fontsize=6.9,
                color=MUTED, family="DejaVu Sans Mono", zorder=5)
        y -= ROW_H

    box[name] = dict(cx=cx, left=left, right=right, top=top, bottom=bottom)


for spec in TABLES:
    draw_table(*spec)


def link(p0, p1, label=None, dashed=False, lx=None, ly=None):
    ax.add_patch(FancyArrowPatch(
        p0, p1, arrowstyle="-", linewidth=1.6 if not dashed else 1.3,
        color=LINE if not dashed else "#c39a63",
        linestyle="--" if dashed else "-",
        shrinkA=0, shrinkB=0, zorder=2))
    if label:
        mx = lx if lx is not None else (p0[0] + p1[0]) / 2.0
        my = ly if ly is not None else (p0[1] + p1[1]) / 2.0
        ax.text(mx, my, label, ha="center", va="center", fontsize=7.4,
                color="#4b5563", family="DejaVu Sans Mono", zorder=6,
                bbox=dict(boxstyle="round,pad=0.22", facecolor="white",
                          edgecolor="none", alpha=0.94))


t, p = box["fact_trip"], box["fact_payment"]
d, r = box["dim_date"], box["dim_rider"]
tm, st = box["dim_time"], box["dim_station"]
frm, agg = box["fact_rider_monthly"], box["agg_rider_spend_vs_rides"]

# trip-only dimensions
link((tm["cx"] + 1.2, tm["bottom"]), (t["left"], t["top"] - 0.9),
     "start_time_key\nend_time_key", lx=5.55, ly=11.35)
link((st["right"], st["top"] - 0.35), (t["left"], t["top"] - 2.6),
     "start_station_key", lx=5.58, ly=7.72)
link((st["right"], st["top"] - 0.95), (t["left"], t["top"] - 3.2),
     "end_station_key", lx=5.58, ly=7.05)

# conformed dimensions - both facts reach them
link((d["cx"] - 1.6, d["bottom"]), (t["right"] - 0.7, t["top"]),
     "start_date_key\nend_date_key", lx=10.15, ly=10.62)
link((d["cx"] + 1.6, d["bottom"]), (p["left"] + 1.0, p["top"]), "date_key",
     lx=15.3, ly=10.62)
link((r["cx"] - 1.5, r["top"]), (t["cx"] + 0.7, t["bottom"]), "rider_key",
     lx=10.05, ly=4.76)
link((r["cx"] + 1.7, r["top"]), (p["left"] + 0.5, p["bottom"]), "rider_key",
     lx=14.75, ly=6.2)

# extra credit derivations
link((t["right"], t["bottom"] + 0.9), (frm["left"], frm["top"] - 1.0),
     "rides", dashed=True, lx=13.2, ly=5.5)
link((p["cx"] + 1.0, p["bottom"]), (frm["cx"] + 1.0, frm["top"]),
     "payments", dashed=True, lx=18.9, ly=7.1)
link((frm["right"], 4.7), (agg["left"], 4.7),
     "rolled up", dashed=True, lx=21.8, ly=4.7)

# ------------------------------------------------------------------ labels --
ax.text(0.55, 15.35, "Divvy Bikeshare - Lakehouse Star Schema", fontsize=25,
        fontweight="bold", color=INK, family="DejaVu Sans", va="top")
ax.text(0.55, 14.65,
        "Azure Databricks - bronze Delta files to a gold star schema",
        fontsize=12.5, color=MUTED, family="DejaVu Sans", va="top")

legend = [("fact", "Fact table"), ("dim", "Dimension"),
          ("extra", "Extra credit (derived)")]
lx0 = 22.4
for i, (kind, text) in enumerate(legend):
    ly0 = 15.15 - i * 0.52
    ax.add_patch(FancyBboxPatch(
        (lx0, ly0 - 0.17), 0.42, 0.28,
        boxstyle="round,pad=0,rounding_size=0.05",
        facecolor=STYLES[kind]["head"], edgecolor=STYLES[kind]["edge"], zorder=5))
    ax.text(lx0 + 0.62, ly0 - 0.03, text, fontsize=11, color=INK,
            family="DejaVu Sans", va="center", zorder=5)

ax.text(lx0, 13.55, "PK  surrogate key      NK  natural key from bronze      "
                    "FK  foreign key      DD  degenerate dimension",
        fontsize=8.6, color=MUTED, family="DejaVu Sans", va="center", zorder=5,
        ha="left")

ax.text(0.55, -0.62,
        "dim_date and dim_rider are conformed: both fact tables key to them, so trip "
        "behaviour and payment behaviour can be compared on the same calendar and the "
        "same rider.",
        fontsize=11, color=MUTED, family="DejaVu Sans", va="top")
ax.text(0.55, -1.12,
        "dim_station role-plays twice on fact_trip (start and end). Age at time of trip "
        "is a fact, not a rider attribute - it differs on every ride; age at account "
        "start is fixed, so it lives on dim_rider.",
        fontsize=11, color=MUTED, family="DejaVu Sans", va="top")

fig.tight_layout(pad=0.6)
fig.savefig(OUT_PNG, dpi=200, facecolor="white", bbox_inches="tight")
fig.savefig(OUT_PDF, facecolor="white", bbox_inches="tight")
print("wrote {0}".format(OUT_PNG))
print("wrote {0}".format(OUT_PDF))
