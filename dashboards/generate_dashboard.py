"""
Workshop-3: Streaming ETL with Apache Kafka and Machine Learning
Part C — Step 9: PostgreSQL Dashboard & KPI Visualization Generator

Connects directly to the PostgreSQL database ('happiness_db', NEVER CSV files),
executes the analytical queries for the 4 required KPIs, and generates high-resolution
dashboard images and KPI summary reports inside the 'dashboards/' directory.
"""

import os
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
import seaborn as sns


sns.set_theme(style="whitegrid", palette="deep")
DASHBOARDS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DASHBOARDS_DIR.parent
ENV_FILE_PATH = PROJECT_ROOT / ".env"


def load_env_file(env_path: Path = ENV_FILE_PATH) -> None:
    """Load key=value pairs from .env into os.environ."""
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


load_env_file()


def get_postgres_connection() -> psycopg2.extensions.connection:
    """Connect directly to PostgreSQL database using credentials loaded from .env."""
    required_vars = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
    missing_vars = [v for v in required_vars if not os.getenv(v)]
    if missing_vars:
        raise EnvironmentError(
            f"Missing required PostgreSQL environment variables in .env: {missing_vars}. "
            "Please copy .env.example to .env and configure your credentials."
        )
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=int(os.environ["POSTGRES_PORT"]),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
    )


def generate_dashboard() -> None:
    conn = get_postgres_connection()
    try:
        # 1. Query KPI 1: Global Average Prediction Error & Summary Metrics
        kpi1_df = pd.read_sql_query(
            """
            SELECT
                COUNT(fp.prediction_id) AS total_predictions,
                ROUND(AVG(fp.prediction_error)::numeric, 4) AS avg_prediction_error_mae,
                ROUND(SQRT(AVG(POWER(fp.actual_score - fp.predicted_score, 2)))::numeric, 4) AS rmse,
                ROUND(MIN(fp.prediction_error)::numeric, 4) AS min_prediction_error,
                ROUND(MAX(fp.prediction_error)::numeric, 4) AS max_prediction_error,
                ROUND(AVG(fp.actual_score)::numeric, 4) AS avg_actual_score,
                ROUND(AVG(fp.predicted_score)::numeric, 4) AS avg_predicted_score
            FROM fact_predictions fp;
            """,
            conn,
        )

        # 2. Query KPI 2: Predictions by Country (Top 15 & Lowest Error Countries)
        kpi2_df = pd.read_sql_query(
            """
            SELECT
                dc.country_name AS country,
                COUNT(fp.prediction_id) AS total_predictions,
                ROUND(AVG(fp.actual_score)::numeric, 4) AS avg_actual_score,
                ROUND(AVG(fp.predicted_score)::numeric, 4) AS avg_predicted_score,
                ROUND(AVG(fp.prediction_error)::numeric, 4) AS avg_prediction_error
            FROM fact_predictions fp
            INNER JOIN dim_country dc ON fp.country_id = dc.country_id
            GROUP BY dc.country_name
            ORDER BY avg_predicted_score DESC;
            """,
            conn,
        )

        # 3. Query KPI 3: Predicted vs. Actual Score (All Records)
        kpi3_df = pd.read_sql_query(
            """
            SELECT
                fp.prediction_id,
                rhe.event_id,
                dc.country_name AS country,
                dd.observation_year AS year,
                fp.actual_score,
                fp.predicted_score,
                fp.prediction_error
            FROM fact_predictions fp
            INNER JOIN dim_country dc ON fp.country_id = dc.country_id
            INNER JOIN dim_date dd ON fp.date_id = dd.date_id
            INNER JOIN raw_happiness_events rhe ON fp.raw_event_id = rhe.raw_event_id
            ORDER BY dd.observation_year ASC, fp.actual_score DESC;
            """,
            conn,
        )

        # 4. Query KPI 4: Prediction Trends Over Time (By Observation Year)
        kpi4_df = pd.read_sql_query(
            """
            SELECT
                dd.observation_year AS year,
                COUNT(fp.prediction_id) AS predictions_count,
                ROUND(AVG(fp.actual_score)::numeric, 4) AS avg_actual_score,
                ROUND(AVG(fp.predicted_score)::numeric, 4) AS avg_predicted_score,
                ROUND(AVG(fp.prediction_error)::numeric, 4) AS avg_prediction_error
            FROM fact_predictions fp
            INNER JOIN dim_date dd ON fp.date_id = dd.date_id
            GROUP BY dd.observation_year
            ORDER BY dd.observation_year ASC;
            """,
            conn,
        )

        # 5. Query Raw Event Contract Status Breakdown
        audit_df = pd.read_sql_query(
            """
            SELECT
                processing_status,
                COUNT(*) AS total_events
            FROM raw_happiness_events
            GROUP BY processing_status
            ORDER BY total_events DESC;
            """,
            conn,
        )

    finally:
        conn.close()

    # Create a comprehensive 4-panel Executive Dashboard Figure
    fig, axes = plt.subplots(2, 2, figsize=(18, 12))
    fig.suptitle(
        "Streaming ETL & Real-Time ML Inference Dashboard (PostgreSQL Live Query)\n"
        f"Total Valid Predictions: {int(kpi1_df['total_predictions'].iloc[0])} | "
        f"Global MAE: {kpi1_df['avg_prediction_error_mae'].iloc[0]:.4f} | "
        f"Global RMSE: {kpi1_df['rmse'].iloc[0]:.4f} | "
        f"Mean Actual: {kpi1_df['avg_actual_score'].iloc[0]:.3f} vs. Mean Predicted: {kpi1_df['avg_predicted_score'].iloc[0]:.3f}",
        fontsize=14,
        fontweight="bold",
        y=0.98,
    )

    # Panel 1 (KPI 1): Prediction Error Distribution & Average Error (MAE)
    ax1 = axes[0, 0]
    sns.histplot(
        kpi3_df["prediction_error"],
        bins=28,
        kde=True,
        color="steelblue",
        ax=ax1,
    )
    mae_val = float(kpi1_df["avg_prediction_error_mae"].iloc[0])
    rmse_val = float(kpi1_df["rmse"].iloc[0])
    ax1.axvline(
        mae_val,
        color="crimson",
        linestyle="--",
        linewidth=2,
        label=f"Average Error (MAE) = {mae_val:.4f}",
    )
    ax1.axvline(
        rmse_val,
        color="darkorange",
        linestyle=":",
        linewidth=2,
        label=f"RMSE = {rmse_val:.4f}",
    )
    ax1.set_title(
        "KPI 1: Average Prediction Error & Error Distribution",
        fontsize=12,
        fontweight="bold",
    )
    ax1.set_xlabel("Absolute Prediction Error |Actual - Predicted|")
    ax1.set_ylabel("Number of Streaming Events")
    ax1.legend()

    # Panel 2 (KPI 2): Predictions by Country (Top 12 Countries Actual vs Predicted)
    ax2 = axes[0, 1]
    top_countries = kpi2_df.head(12).sort_values("avg_predicted_score", ascending=True)
    y_pos = range(len(top_countries))
    ax2.barh(
        [y - 0.18 for y in y_pos],
        top_countries["avg_actual_score"],
        height=0.35,
        label="Avg Actual Score",
        color="seagreen",
        alpha=0.85,
    )
    ax2.barh(
        [y + 0.18 for y in y_pos],
        top_countries["avg_predicted_score"],
        height=0.35,
        label="Avg Predicted Score",
        color="royalblue",
        alpha=0.85,
    )
    ax2.set_yticks(list(y_pos))
    ax2.set_yticklabels(top_countries["country"])
    ax2.set_xlim(0, 8.5)
    ax2.set_title(
        "KPI 2: Predictions by Country (Top 12 Countries Comparison)",
        fontsize=12,
        fontweight="bold",
    )
    ax2.set_xlabel("Happiness Score")
    ax2.legend(loc="lower right")

    # Panel 3 (KPI 3): Predicted vs. Actual Score Scatter Plot
    ax3 = axes[1, 0]
    scatter = ax3.scatter(
        kpi3_df["actual_score"],
        kpi3_df["predicted_score"],
        c=kpi3_df["year"],
        cmap="viridis",
        alpha=0.65,
        edgecolors="k",
        linewidth=0.3,
        s=35,
    )
    min_s = min(kpi3_df["actual_score"].min(), kpi3_df["predicted_score"].min()) - 0.2
    max_s = max(kpi3_df["actual_score"].max(), kpi3_df["predicted_score"].max()) + 0.2
    ax3.plot(
        [min_s, max_s],
        [min_s, max_s],
        "r--",
        linewidth=2,
        label="Ideal Alignment (y = x)",
    )
    cbar = plt.colorbar(scatter, ax=ax3)
    cbar.set_label("Observation Year")
    ax3.set_title(
        "KPI 3: Predicted vs. Actual Happiness Score (All Streamed Events)",
        fontsize=12,
        fontweight="bold",
    )
    ax3.set_xlabel("Actual Happiness Score")
    ax3.set_ylabel("Predicted Happiness Score")
    ax3.legend(loc="upper left")

    # Panel 4 (KPI 4): Prediction Trends Over Time (2015–2019)
    ax4 = axes[1, 1]
    ax4.plot(
        kpi4_df["year"],
        kpi4_df["avg_actual_score"],
        marker="o",
        linewidth=2.5,
        color="seagreen",
        label="Mean Actual Score",
    )
    ax4.plot(
        kpi4_df["year"],
        kpi4_df["avg_predicted_score"],
        marker="s",
        linewidth=2.5,
        linestyle="--",
        color="royalblue",
        label="Mean Predicted Score",
    )
    ax4_twin = ax4.twinx()
    ax4_twin.bar(
        kpi4_df["year"],
        kpi4_df["avg_prediction_error"],
        alpha=0.25,
        color="crimson",
        width=0.35,
        label="Mean Prediction Error (MAE)",
    )
    ax4_twin.set_ylabel("Mean Absolute Error (MAE)", color="crimson")
    ax4_twin.set_ylim(0, 1.0)
    ax4_twin.grid(False)

    ax4.set_xticks(kpi4_df["year"].tolist())
    ax4.set_ylim(4.8, 5.9)
    ax4.set_title(
        "KPI 4: Prediction Trends & Error Stability Over Time (2015–2019)",
        fontsize=12,
        fontweight="bold",
    )
    ax4.set_xlabel("Observation Year")
    ax4.set_ylabel("Average Happiness Score")
    lines1, labels1 = ax4.get_legend_handles_labels()
    lines2, labels2 = ax4_twin.get_legend_handles_labels()
    ax4.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    plt.tight_layout(rect=[0, 0, 1, 0.93])
    dashboard_img_path = DASHBOARDS_DIR / "streaming_kpis_dashboard.png"
    plt.savefig(dashboard_img_path, dpi=180, bbox_inches="tight")
    plt.close()

    print(f"Saved dashboard visualization to: {dashboard_img_path}")
    print("\n=== KPI 1 Summary ===")
    print(kpi1_df.to_string(index=False))
    print("\n=== KPI 4 Trends Over Time ===")
    print(kpi4_df.to_string(index=False))
    print("\n=== Raw Event Contract Status Audit ===")
    print(audit_df.to_string(index=False))


if __name__ == "__main__":
    generate_dashboard()
