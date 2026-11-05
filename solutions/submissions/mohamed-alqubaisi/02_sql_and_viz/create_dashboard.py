
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime

# Find the main assessment folder
ROOT = Path(__file__).resolve().parents[4]
OUTPUT = ROOT / "outputs"
OUTPUT.mkdir(parents=True, exist_ok=True)

# Load actual assessment data
projects = pd.read_csv(OUTPUT / "projects_clean.csv")
transactions = pd.read_csv(OUTPUT / "transactions_clean.csv")

projects["budget"] = pd.to_numeric(
    projects["budget"], errors="coerce"
).fillna(0)

projects["actual_cost"] = pd.to_numeric(
    projects["actual_cost"], errors="coerce"
).fillna(0)

transactions["amount"] = pd.to_numeric(
    transactions["amount"], errors="coerce"
)

transactions["transaction_date"] = pd.to_datetime(
    transactions["transaction_date"], errors="coerce"
)

# Calculate KPIs
total_budget = projects["budget"].sum()
total_actual = projects["actual_cost"].sum()
total_transactions = len(transactions)

over_budget_pct = (
    projects["is_over_budget"]
    .astype(str).str.lower().eq("true").mean() * 100
)

# Department budget comparison
department = projects.groupby("department")[
    ["budget", "actual_cost"]
].sum().sort_values("budget", ascending=False)

# Monthly spending by category
valid_tx = transactions.dropna(
    subset=["transaction_date"]
).copy()

valid_tx["month"] = (
    valid_tx["transaction_date"].dt.to_period("M")
)

# Use the latest 12 months in the dataset
latest_month = valid_tx["month"].max()
first_month = latest_month - 11

recent = valid_tx[
    valid_tx["month"].between(first_month, latest_month)
].copy()

top_categories = (
    valid_tx.groupby("category")["amount"]
    .sum().nlargest(5).index
)

monthly = recent[
    recent["category"].isin(top_categories)
].pivot_table(
    index="month",
    columns="category",
    values="amount",
    aggfunc="sum",
    fill_value=0
)

months = pd.period_range(
    first_month, latest_month, freq="M"
)
monthly = monthly.reindex(months, fill_value=0)

# Top 10 absolute budget variances
top_projects = projects.copy()
top_projects["variance"] = (
    top_projects["actual_cost"] - top_projects["budget"]
)
top_projects = top_projects.loc[
    top_projects["variance"].abs().nlargest(10).index
].sort_values("variance", ascending=False)

# Vendor concentration
vendors = (
    transactions.groupby("vendor_name")["amount"]
    .sum().sort_values(ascending=False)
)

vendor_top5 = vendors.head(5)
vendor_values = list(vendor_top5.values)
vendor_values.append(vendors.iloc[5:].sum())
vendor_labels = list(vendor_top5.index) + ["Other"]

# Create one-page landscape dashboard
fig = plt.figure(figsize=(16, 10), facecolor="#F3F6FA")

NAVY = "#142B45"
BLUE = "#2875B9"
TEAL = "#139A91"
GRAY = "#637487"

# Header
fig.text(
    0.045, 0.955,
    "PRESIGHT  |  PROJECT SPEND PERFORMANCE",
    fontsize=20, fontweight="bold", color=NAVY
)

fig.text(
    0.045, 0.922,
    "Executive Dashboard - StackUp Engineering Academy",
    fontsize=10, color=GRAY
)

fig.text(
    0.955, 0.922,
    f"Prepared: {datetime.now():%d %b %Y}",
    fontsize=9, color=GRAY, ha="right"
)

def kpi(x, title, value):
    fig.text(
        x, 0.858, title,
        fontsize=11, color=GRAY
    )
    fig.text(
        x, 0.818, value,
        fontsize=19, fontweight="bold", color=NAVY
    )

kpi(0.05, "TOTAL PROJECT BUDGET",
    f"AED {total_budget / 1e6:,.1f}M")

kpi(0.29, "TOTAL PROJECT ACTUAL COST",
    f"AED {total_actual / 1e6:,.1f}M")

kpi(0.56, "OVER-BUDGET PROJECTS",
    f"{over_budget_pct:.1f}%")

kpi(0.80, "TRANSACTIONS",
    f"{total_transactions:,}")

# Static slicer mockup
fig.text(
    0.05, 0.765,
    "FILTER MOCKUPS:   Region: All     |     "
    "Project Status: All     |     Year: All",
    fontsize=10, color=NAVY,
    bbox=dict(
        boxstyle="round,pad=0.6",
        facecolor="#E3EBF3",
        edgecolor="none"
    )
)

# Chart 1: Department budget vs actual
ax1 = fig.add_axes([0.07, 0.43, 0.42, 0.27])
y = np.arange(len(department))

ax1.barh(
    y - 0.2, department["budget"] / 1e6,
    height=0.38, label="Budget", color=BLUE
)

ax1.barh(
    y + 0.2, department["actual_cost"] / 1e6,
    height=0.38, label="Actual", color=TEAL
)

ax1.set_yticks(y)
ax1.set_yticklabels(department.index, fontsize=8)
ax1.invert_yaxis()
ax1.set_xlabel("AED millions", fontsize=8)
ax1.set_title(
    "Budget vs Actual by Department",
    fontsize=12, fontweight="bold", loc="left"
)
ax1.legend(fontsize=8, loc="lower right")
ax1.grid(axis="x", alpha=0.15)
ax1.set_axisbelow(True)

# Chart 2: Monthly spending trend
ax2 = fig.add_axes([0.59, 0.43, 0.36, 0.27])

for category in monthly.columns:
    ax2.plot(
        range(len(monthly)),
        monthly[category] / 1e6,
        marker="o", markersize=2,
        linewidth=1.5, label=category
    )

ax2.set_xticks(range(0, len(monthly), 2))
ax2.set_xticklabels(
    [str(monthly.index[i]) for i in range(
        0, len(monthly), 2
    )],
    rotation=30, ha="right", fontsize=7
)
ax2.set_ylabel("AED millions", fontsize=8)
ax2.set_title(
    "Monthly Transaction Spend - Top 5 Categories",
    fontsize=12, fontweight="bold", loc="left"
)
ax2.legend(fontsize=6.5, loc="upper left")
ax2.grid(alpha=0.15)

# Chart 3: Top 10 project variances table
ax3 = fig.add_axes([0.05, 0.115, 0.48, 0.245])
ax3.axis("off")
ax3.set_title(
    "Top 10 Projects by Absolute Budget Variance",
    fontsize=12, fontweight="bold", loc="left",
    pad=10
)

table_data = [
    [
        row.project_id,
        str(row.project_name)[:26],
        f"{row.variance:,.0f}"
    ]
    for row in top_projects.itertuples()
]

table = ax3.table(
    cellText=table_data,
    colLabels=[
        "Project ID", "Project Name", "Variance (AED)"
    ],
    colWidths=[0.19, 0.50, 0.29],
    cellLoc="left",
    loc="center"
)
table.auto_set_font_size(False)
table.set_fontsize(7.2)
table.scale(1, 1.40)

for (r, c), cell in table.get_celld().items():
    cell.set_edgecolor("#DDE5EE")
    if r == 0:
        cell.set_facecolor(NAVY)
        cell.get_text().set_color("white")
        cell.get_text().set_weight("bold")

# Chart 4: Vendor concentration donut
ax4 = fig.add_axes([0.64, 0.105, 0.29, 0.25])

colors = [
    "#2075AD", "#159A92", "#E4A44B",
    "#7D72C4", "#E17967", "#CCD6E0"
]

ax4.pie(
    vendor_values,
    startangle=90,
    colors=colors,
    wedgeprops={"width": 0.38, "edgecolor": "white"}
)
ax4.set_title(
    "Vendor Concentration - Top 5 + Other",
    fontsize=12, fontweight="bold"
)
ax4.legend(
    vendor_labels,
    loc="center left",
    bbox_to_anchor=(1.0, 0.5),
    fontsize=7,
    frameon=False
)

# Explanation and limitations
fig.text(
    0.05, 0.387,
    "Department bars compare budget allocation with actual costs; "
    "category lines show changes across the latest 12 data months.",
    fontsize=8, color=GRAY
)

fig.text(
    0.05, 0.071,
    "Variance = actual cost minus budget; negative values indicate "
    "underspend. Donut slices represent known vendor transaction amounts.",
    fontsize=8, color=GRAY
)

fig.text(
    0.05, 0.039,
    "Mockup only: filter labels are not interactive. "
    "740 disputed transaction amounts are unknown and "
    "excluded from spend totals.",
    fontsize=8, color=GRAY
)

fig.text(
    0.95, 0.015,
    "Source: StackUp assessment datasets  |  "
    "Prepared by mohamed-alqubaisi",
    fontsize=8, ha="right", color=GRAY
)

# Export PDF
pdf_path = OUTPUT / "dashboard_mockup.pdf"
fig.savefig(
    pdf_path,
    format="pdf",
    facecolor=fig.get_facecolor()
)

plt.close(fig)

print("Dashboard created successfully!")
print("Saved to:", pdf_path)
print("Total project budget:", total_budget)
print("Total project actual cost:", total_actual)
print("Over-budget projects (%):", round(over_budget_pct, 2))
print("Transactions:", total_transactions)
