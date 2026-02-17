"""
Generate comprehensive analytics and visualizations from the enriched
taxi-weather dataset in MongoDB.

Produces 8 publication-quality charts saved to outputs/.

Usage:
    python scripts/generate_visualizations.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv

load_dotenv()

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from dags.utils.mongo_helpers import get_database
from config.settings import TAXI_COLLECTION, WEATHER_COLLECTION

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "outputs")
os.makedirs(OUT_DIR, exist_ok=True)

sns.set_theme(style="whitegrid", font_scale=1.1)
PALETTE = sns.color_palette("Set2")
WEATHER_COLORS = {
    "Clear/Normal": "#66c2a5",
    "Light Rain": "#fc8d62",
    "Heavy Rain": "#8da0cb",
    "Snow": "#e78ac3",
    "Freezing": "#a6d854",
    "Extreme Heat": "#ffd92f",
}


def load_enriched_data() -> pd.DataFrame:
    """
    Pull taxi_trips and weather_daily from MongoDB,
    join them in pandas on pickup_date = DATE.
    """
    db = get_database()

    # Load taxi trips
    taxi_cursor = db[TAXI_COLLECTION].find(
        {},
        {
            "_id": 0,
            "tpep_pickup_datetime": 1,
            "pickup_date": 1,
            "pickup_hour": 1,
            "trip_distance": 1,
            "fare_amount": 1,
            "tip_amount": 1,
            "total_amount": 1,
            "passenger_count": 1,
            "payment_type": 1,
        },
    )
    df = pd.DataFrame(list(taxi_cursor))

    # Load weather
    wx_cursor = db[WEATHER_COLLECTION].find({}, {"_id": 0})
    wx = pd.DataFrame(list(wx_cursor))

    # Join on date
    df = df.merge(wx, left_on="pickup_date", right_on="DATE", how="left")
    df.drop(columns=["DATE"], inplace=True, errors="ignore")

    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["pickup_date"] = pd.to_datetime(df["pickup_date"])
    df["month"] = df["pickup_date"].dt.month
    df["day_of_week"] = df["pickup_date"].dt.dayofweek
    df["day_name"] = df["pickup_date"].dt.day_name()
    df["is_weekend"] = df["day_of_week"].isin([5, 6])

    mask = (df["payment_type"] == 1) & (df["fare_amount"] > 2)
    df["tip_pct"] = np.where(mask, (df["tip_amount"] / df["fare_amount"]) * 100, np.nan)
    # Cap outliers: nobody realistically tips > 60% on a taxi
    df.loc[df["tip_pct"] > 60, "tip_pct"] = np.nan

    print(f"Loaded {len(df):,} enriched records")
    print(f"Unique dates: {df['pickup_date'].nunique()}")
    print(f"Hour range: {df['pickup_hour'].min()} - {df['pickup_hour'].max()}")
    return df


def get_weather_order(df):
    """Return weather conditions present in data, in a logical order."""
    preferred = ["Clear/Normal", "Light Rain", "Heavy Rain", "Snow", "Freezing", "Extreme Heat"]
    present = df["weather_condition"].unique()
    return [w for w in preferred if w in present]


# =========================================================================
# VIZ 1: Temperature vs Tip Percentage (one point per date, 48 points)
# =========================================================================
def plot_temp_vs_tip(df):
    daily = (
        df[df["tip_pct"].notna()]
        .groupby("pickup_date")
        .agg(avg_tip_pct=("tip_pct", "mean"), TMAX=("TMAX", "first"),
             weather_condition=("weather_condition", "first"))
        .dropna()
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(11, 6))
    order = get_weather_order(daily)
    for cond in order:
        subset = daily[daily["weather_condition"] == cond]
        ax.scatter(subset["TMAX"], subset["avg_tip_pct"], s=60, alpha=0.75,
                   color=WEATHER_COLORS.get(cond, "gray"), label=cond, edgecolors="white", linewidth=0.8)

    z = np.polyfit(daily["TMAX"], daily["avg_tip_pct"], 1)
    p = np.poly1d(z)
    x_line = np.linspace(daily["TMAX"].min(), daily["TMAX"].max(), 100)
    ax.plot(x_line, p(x_line), color="black", linewidth=2, linestyle="--", alpha=0.6,
            label=f"Linear Trend (slope={z[0]:.3f})")

    ax.set_xlabel("Daily Max Temperature (°F)")
    ax.set_ylabel("Average Tip Percentage (%)")
    ax.set_title(f"Temperature vs. Tip Percentage ({len(daily)} sample days, 2023)")
    ax.legend(title="Weather", loc="upper right")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "01_temp_vs_tip.png"), dpi=150)
    plt.close(fig)
    print("  [1/8] Temperature vs Tip Percentage")


# =========================================================================
# VIZ 2: Hourly Tipping Patterns by Weather (full 0-23 hours)
# =========================================================================
def plot_hourly_by_weather(df):
    hourly = (
        df[df["tip_pct"].notna()]
        .groupby(["pickup_hour", "weather_condition"])
        .agg(avg_tip_pct=("tip_pct", "mean"), count=("tip_pct", "size"))
        .reset_index()
    )
    # Only show hour/condition combos with at least 30 records
    hourly = hourly[hourly["count"] >= 30]

    fig, ax = plt.subplots(figsize=(13, 6))
    order = get_weather_order(df)
    for cond in order:
        subset = hourly[hourly["weather_condition"] == cond].sort_values("pickup_hour")
        if not subset.empty:
            ax.plot(subset["pickup_hour"], subset["avg_tip_pct"],
                    marker="o", linewidth=2.5, markersize=6, label=cond,
                    color=WEATHER_COLORS.get(cond, "gray"))

    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Average Tip Percentage (%)")
    ax.set_title("Hourly Tipping Patterns by Weather Condition")
    # Only label hours we actually have data for
    sampled_hours = sorted(hourly["pickup_hour"].unique())
    ax.set_xticks(sampled_hours)
    ax.set_xticklabels([f"{h}:00" for h in sampled_hours], rotation=45, ha="right")
    ax.legend(title="Weather")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "02_hourly_tip_by_weather.png"), dpi=150)
    plt.close(fig)
    print("  [2/8] Hourly Tipping by Weather")


# =========================================================================
# VIZ 3: Precipitation Intensity vs Tipping (bars + line, 0-based axes)
# =========================================================================
def plot_precip_vs_tip(df):
    cc = df[(df["tip_pct"].notna()) & (df["PRCP"].notna())].copy()
    bins = [0, 0.01, 0.1, 0.25, 0.5, 1.0, float("inf")]
    labels = ["None\n(0\")", "Trace\n(<0.1\")", "Light\n(0.1-0.25\")",
              "Moderate\n(0.25-0.5\")", "Heavy\n(0.5-1\")", "Very Heavy\n(>1\")"]
    cc["precip_bin"] = pd.cut(cc["PRCP"], bins=bins, labels=labels, right=False)

    agg = cc.groupby("precip_bin", observed=True).agg(
        avg_tip=("tip_pct", "mean"),
        count=("tip_pct", "size"),
    ).reset_index()

    fig, ax1 = plt.subplots(figsize=(11, 6))

    x_pos = range(len(agg))
    bars = ax1.bar(x_pos, agg["avg_tip"], color=PALETTE[0], edgecolor="white", width=0.55)
    for bar, row in zip(bars, agg.itertuples()):
        ax1.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.2,
                 f"{row.avg_tip:.1f}%", ha="center", fontsize=9, fontweight="bold")

    ax1.set_xlabel("Precipitation Level")
    ax1.set_ylabel("Average Tip Percentage (%)", color=PALETTE[0])
    ax1.tick_params(axis="y", labelcolor=PALETTE[0])
    ax1.set_xticks(x_pos)
    ax1.set_xticklabels(agg["precip_bin"].astype(str))

    ax2 = ax1.twinx()
    ax2.plot(x_pos, agg["count"], color=PALETTE[3], marker="D", linewidth=2.5, markersize=8)
    for i, row in enumerate(agg.itertuples()):
        ax2.annotate(f"{row.count:,}", xy=(i, row.count), xytext=(0, 8),
                     textcoords="offset points", ha="center", fontsize=8, color=PALETTE[3])
    ax2.set_ylabel("Number of Trips", color=PALETTE[3])
    ax2.tick_params(axis="y", labelcolor=PALETTE[3])
    ax2.set_ylim(bottom=0)
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    ax1.set_title("Precipitation Intensity: Tip Percentage & Trip Volume")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "03_precipitation_vs_tip.png"), dpi=150)
    plt.close(fig)
    print("  [3/8] Precipitation Intensity vs Tip")


# =========================================================================
# VIZ 4: Monthly Tip % and Temperature (simplified 2-panel)
# =========================================================================
def plot_monthly_trends(df):
    cc = df[df["tip_pct"].notna()].copy()
    monthly = cc.groupby("month").agg(
        avg_tip_pct=("tip_pct", "mean"),
        avg_temp=("TMAX", "mean"),
        trip_count=("fare_amount", "size"),
    ).reset_index()

    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    monthly["month_name"] = monthly["month"].map(lambda m: month_names[m - 1])

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True,
                                     gridspec_kw={"height_ratios": [2, 1]})

    # Top panel: Tip % bars + Temp line
    color_tip = PALETTE[0]
    ax1.bar(monthly["month_name"], monthly["avg_tip_pct"], color=color_tip,
            edgecolor="white", width=0.6, label="Avg Tip %")
    for i, row in monthly.iterrows():
        ax1.text(i, row["avg_tip_pct"] + 0.15, f"{row['avg_tip_pct']:.1f}%",
                 ha="center", fontsize=8, fontweight="bold")
    ax1.set_ylabel("Avg Tip %", color=color_tip)
    ax1.tick_params(axis="y", labelcolor=color_tip)

    ax1t = ax1.twinx()
    ax1t.plot(monthly["month_name"], monthly["avg_temp"], color="tomato",
              marker="o", linewidth=2.5, markersize=7, label="Avg Max Temp (°F)")
    ax1t.set_ylabel("Avg Max Temp (°F)", color="tomato")
    ax1t.tick_params(axis="y", labelcolor="tomato")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax1t.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper right")
    ax1.set_title("Monthly Trends: Tip Percentage vs. Temperature (2023)")

    # Bottom panel: Trip count
    ax2.bar(monthly["month_name"], monthly["trip_count"], color=PALETTE[2], edgecolor="white", width=0.6)
    for i, row in monthly.iterrows():
        ax2.text(i, row["trip_count"] + 20, f"{row['trip_count']:,}",
                 ha="center", fontsize=7, rotation=45)
    ax2.set_ylabel("Trip Count")
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "04_monthly_trends.png"), dpi=150)
    plt.close(fig)
    print("  [4/8] Monthly Trends")


# =========================================================================
# VIZ 5: Trip Distance by Weather (box plots with stats labeled)
# =========================================================================
def plot_distance_by_weather(df):
    cc = df[df["trip_distance"].between(0.1, 30)].copy()
    order = get_weather_order(cc)
    colors = [WEATHER_COLORS.get(w, "gray") for w in order]

    fig, ax = plt.subplots(figsize=(10, 6))
    bp = ax.boxplot(
        [cc[cc["weather_condition"] == w]["trip_distance"] for w in order],
        labels=order, patch_artist=True, showfliers=False, widths=0.5,
        medianprops=dict(color="black", linewidth=2),
    )
    for patch, color in zip(bp["boxes"], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    # Annotate median and mean
    for i, w in enumerate(order):
        subset = cc[cc["weather_condition"] == w]["trip_distance"]
        med = subset.median()
        mean = subset.mean()
        n = len(subset)
        q75 = subset.quantile(0.75)
        ax.text(i + 1, med + 0.2, f"median: {med:.1f} mi", ha="center", fontsize=8, fontweight="bold")
        ax.text(i + 1, q75 + 1.0, f"mean: {mean:.1f} mi", ha="center", fontsize=8, color="blue")
        ax.text(i + 1, 16.5, f"n={n:,}", ha="center", fontsize=8, color="gray")

    ax.set_xlabel("Weather Condition")
    ax.set_ylabel("Trip Distance (miles)")
    ax.set_title("Trip Distance Distribution by Weather")
    ax.set_ylim(0, 18)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "05_distance_by_weather.png"), dpi=150)
    plt.close(fig)
    print("  [5/8] Trip Distance by Weather")


# =========================================================================
# VIZ 6: Weekend vs Weekday Tipping by Weather (all groups)
# =========================================================================
def plot_weekend_weekday(df):
    cc = df[df["tip_pct"].notna()].copy()
    cc["period"] = cc["is_weekend"].map({True: "Weekend", False: "Weekday"})

    agg = cc.groupby(["weather_condition", "period"]).agg(
        avg_tip=("tip_pct", "mean"),
        count=("tip_pct", "size"),
    ).reset_index()

    # Only keep groups with at least 20 data points
    agg = agg[agg["count"] >= 20]

    order = get_weather_order(cc)
    # Only keep conditions that have both weekday AND weekend data
    conditions_with_both = []
    for w in order:
        subset = agg[agg["weather_condition"] == w]
        if len(subset["period"].unique()) == 2:
            conditions_with_both.append(w)
        elif len(subset) > 0:
            conditions_with_both.append(w)

    fig, ax = plt.subplots(figsize=(11, 6))
    x = np.arange(len(conditions_with_both))
    width = 0.35

    weekday_vals = []
    weekend_vals = []
    for w in conditions_with_both:
        wd = agg[(agg["weather_condition"] == w) & (agg["period"] == "Weekday")]
        we = agg[(agg["weather_condition"] == w) & (agg["period"] == "Weekend")]
        weekday_vals.append(wd["avg_tip"].values[0] if len(wd) > 0 else 0)
        weekend_vals.append(we["avg_tip"].values[0] if len(we) > 0 else 0)

    bars1 = ax.bar(x - width / 2, weekday_vals, width, label="Weekday", color=PALETTE[0], edgecolor="white")
    bars2 = ax.bar(x + width / 2, weekend_vals, width, label="Weekend", color=PALETTE[1], edgecolor="white")

    for bars in [bars1, bars2]:
        for bar in bars:
            h = bar.get_height()
            if h > 0:
                ax.annotate(f"{h:.1f}%", xy=(bar.get_x() + bar.get_width() / 2, h),
                            xytext=(0, 4), textcoords="offset points", ha="center", fontsize=9)
            else:
                ax.annotate("N/A", xy=(bar.get_x() + bar.get_width() / 2, 0.5),
                            ha="center", fontsize=8, color="gray", fontstyle="italic")

    ax.set_xlabel("Weather Condition")
    ax.set_ylabel("Average Tip Percentage (%)")
    ax.set_title("Weekday vs. Weekend Tipping by Weather Condition")
    ax.set_xticks(x)
    ax.set_xticklabels(conditions_with_both)
    ax.legend()
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "06_weekend_vs_weekday.png"), dpi=150)
    plt.close(fig)
    print("  [6/8] Weekend vs Weekday Tipping")


# =========================================================================
# VIZ 7: Trip Volume Heatmap (Hour x Day of Week) - full 0-23
# =========================================================================
def plot_trip_heatmap(df):
    """Only show the sampled hours (no blank/zero columns)."""
    # We only sample Wed and Sat, so only those days have data
    day_order = ["Wednesday", "Saturday"]

    pivot = df.pivot_table(
        index="day_name", columns="pickup_hour",
        values="fare_amount", aggfunc="count",
    ).reindex(day_order)

    # Keep only hours that have data
    pivot = pivot.loc[:, pivot.sum() > 0]

    fig, ax = plt.subplots(figsize=(12, 4))
    sns.heatmap(
        pivot, cmap="YlOrRd", annot=True, fmt=".0f",
        linewidths=0.5, ax=ax, cbar_kws={"label": "Trip Count"},
        annot_kws={"size": 9},
    )
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("")
    ax.set_xticklabels([f"{int(h)}:00" for h in pivot.columns])
    ax.set_title("Trip Volume: Weekday (Wed) vs. Weekend (Sat) by Hour")
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "07_trip_heatmap.png"), dpi=150)
    plt.close(fig)
    print("  [7/8] Trip Volume Heatmap")


# =========================================================================
# VIZ 8: Fare Per Mile by Weather (with annotated stats)
# =========================================================================
def plot_fare_premium(df):
    cc = df[(df["trip_distance"] > 0.5) & (df["fare_amount"] > 0)].copy()
    cc["fare_per_mile"] = cc["fare_amount"] / cc["trip_distance"]
    cc = cc[cc["fare_per_mile"] < 50]

    order = get_weather_order(cc)
    agg = cc.groupby("weather_condition").agg(
        avg_fare_per_mile=("fare_per_mile", "mean"),
        median_fare_per_mile=("fare_per_mile", "median"),
        avg_fare=("fare_amount", "mean"),
        avg_distance=("trip_distance", "mean"),
        count=("fare_per_mile", "size"),
    ).reindex(order).reset_index().dropna(subset=["avg_fare_per_mile"])

    colors = [WEATHER_COLORS.get(w, "gray") for w in agg["weather_condition"]]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(agg["weather_condition"], agg["avg_fare_per_mile"],
                  color=colors, edgecolor="white", width=0.5)

    for bar, row in zip(bars, agg.itertuples()):
        ax.annotate(
            f"${row.avg_fare_per_mile:.2f}/mi\n({row.count:,} trips)\navg dist: {row.avg_distance:.1f} mi",
            xy=(bar.get_x() + bar.get_width() / 2, bar.get_height()),
            xytext=(0, 6), textcoords="offset points", ha="center", fontsize=8,
        )

    ax.set_xlabel("Weather Condition")
    ax.set_ylabel("Average Fare per Mile ($)")
    ax.set_title("Fare Per Mile by Weather Condition", pad=20)
    max_val = agg["avg_fare_per_mile"].max()
    ax.set_ylim(bottom=0, top=max_val * 1.35)
    fig.tight_layout()
    fig.savefig(os.path.join(OUT_DIR, "08_fare_premium.png"), dpi=150)
    plt.close(fig)
    print("  [8/8] Fare Premium by Weather")


# =========================================================================
# MAIN
# =========================================================================
def main():
    print("Loading data from MongoDB...")
    df = load_enriched_data()

    print(f"\nWeather condition distribution:")
    print(df["weather_condition"].value_counts().to_string())
    print(f"\nPayment type distribution:")
    print(df["payment_type"].value_counts().to_string())
    print(f"\nHour distribution (min/max): {df['pickup_hour'].min()} - {df['pickup_hour'].max()}")
    print(f"Unique dates: {df['pickup_date'].nunique()}")

    print(f"\nGenerating 8 visualizations...\n")
    plot_temp_vs_tip(df)
    plot_hourly_by_weather(df)
    plot_precip_vs_tip(df)
    plot_monthly_trends(df)
    plot_distance_by_weather(df)
    plot_weekend_weekday(df)
    plot_trip_heatmap(df)
    plot_fare_premium(df)

    print(f"\nAll charts saved to: {os.path.abspath(OUT_DIR)}/")

    # =====================================================================
    # KEY STATS SUMMARY — write to outputs/key_stats.txt
    # =====================================================================
    cc = df[(df["tip_pct"].notna())].copy()
    stats_path = os.path.join(OUT_DIR, "key_stats.txt")

    lines = []
    lines.append("=" * 70)
    lines.append("KEY STATISTICS SUMMARY")
    lines.append("=" * 70)
    lines.append(f"\nTotal records: {len(df):,}")
    lines.append(f"Credit card trips (with tip data): {len(cc):,}")
    lines.append(f"Unique dates sampled: {df['pickup_date'].nunique()}")
    lines.append(f"Date range: {df['pickup_date'].min().date()} to {df['pickup_date'].max().date()}")

    lines.append(f"\n--- Average Tip % by Weather Condition ---")
    tip_by_weather = cc.groupby("weather_condition").agg(
        avg_tip_pct=("tip_pct", "mean"),
        median_tip_pct=("tip_pct", "median"),
        avg_tip_amt=("tip_amount", "mean"),
        avg_fare=("fare_amount", "mean"),
        avg_distance=("trip_distance", "mean"),
        trip_count=("tip_pct", "size"),
    ).round(2)
    lines.append(tip_by_weather.to_string())

    lines.append(f"\n--- Average Tip % by Hour ---")
    tip_by_hour = cc.groupby("pickup_hour").agg(
        avg_tip_pct=("tip_pct", "mean"),
        trip_count=("tip_pct", "size"),
    ).round(2)
    lines.append(tip_by_hour.to_string())

    lines.append(f"\n--- Average Tip % by Month ---")
    tip_by_month = cc.groupby("month").agg(
        avg_tip_pct=("tip_pct", "mean"),
        trip_count=("tip_pct", "size"),
    ).round(2)
    lines.append(tip_by_month.to_string())

    lines.append(f"\n--- Weekday vs Weekend ---")
    cc["period"] = cc["is_weekend"].map({True: "Weekend (Sat)", False: "Weekday (Wed)"})
    tip_by_period = cc.groupby("period").agg(
        avg_tip_pct=("tip_pct", "mean"),
        trip_count=("tip_pct", "size"),
    ).round(2)
    lines.append(tip_by_period.to_string())

    lines.append("\n" + "=" * 70)

    stats_text = "\n".join(lines)
    with open(stats_path, "w") as f:
        f.write(stats_text)

    print(f"\nKey stats saved to: {stats_path}")
    print(stats_text)


if __name__ == "__main__":
    main()
