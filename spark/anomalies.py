"""
Spark-based anomaly detectors for Medicaid claims data.

Implements the same four detection strategies as scanner/anomalies.py
but using PySpark DataFrames and Window functions — no Python loops over rows.

Each detector returns a Spark DataFrame with columns:
    npi, flag_type, description, severity, evidence_json
"""

import json

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# --- Thresholds (same values as scanner/anomalies.py) ---
MAX_CLAIMS_PER_MONTH = 1500
REVENUE_ZSCORE_THRESHOLD = 3.0
SPIKE_MULTIPLIER = 5.0
DOMINANCE_THRESHOLD = 0.70
RATE_CV_THRESHOLD = 0.08
CONSISTENCY_MIN_MONTHS = 3
MIN_TOTAL_PAID = 100_000

# Flag type constants (mirroring RedFlagType enum values)
VOLUME_IMPOSSIBILITY = "volume_impossibility"
REVENUE_OUTLIER = "revenue_outlier"
BILLING_SPIKE = "billing_spike"
SUSPICIOUS_CONSISTENCY = "suspicious_consistency"


def filter_qualifying_providers(monthly_df: DataFrame) -> DataFrame:
    """Return only rows for providers whose total paid meets the minimum threshold."""
    totals = (
        monthly_df.groupBy("npi")
        .agg(F.sum("total_paid").alias("total_paid_sum"))
        .filter(F.col("total_paid_sum") >= MIN_TOTAL_PAID)
        .select("npi")
    )
    return monthly_df.join(totals, on="npi", how="inner")


def detect_volume_impossibility(monthly_df: DataFrame) -> DataFrame:
    """Flag months where a single provider's claim count exceeds MAX_CLAIMS_PER_MONTH."""
    return (
        monthly_df
        .filter(F.col("total_claims") > MAX_CLAIMS_PER_MONTH)
        .withColumn("flag_type", F.lit(VOLUME_IMPOSSIBILITY))
        .withColumn(
            "description",
            F.concat_ws(
                "",
                F.format_number(F.col("total_claims"), 0),
                F.lit(f" claims in "),
                F.col("service_month").cast("string"),
                F.lit(f" (max plausible: {MAX_CLAIMS_PER_MONTH:,})"),
            ),
        )
        .withColumn(
            "severity",
            F.least(F.lit(1.0), F.col("total_claims") / F.lit(MAX_CLAIMS_PER_MONTH * 3.0)),
        )
        .withColumn(
            "evidence_json",
            F.to_json(F.struct(
                F.col("service_month").alias("month"),
                F.col("total_claims").alias("claims"),
            )),
        )
        .select("npi", "flag_type", "description", "severity", "evidence_json")
    )


def detect_revenue_outliers(monthly_df: DataFrame) -> DataFrame:
    """Flag providers whose revenue-per-claim is far above peers (modified z-score via MAD).

    Median and MAD are computed as 1-row DataFrames that are crossJoined back onto the
    provider totals.  This keeps the entire computation lazy — no intermediate driver-side
    .collect() calls — which avoids a Python 3.14 + PySpark 4.x Windows GC bug where
    BufferedRWPair.__del__ races with a live JVM socket when the collected Row objects are
    garbage-collected during a subsequent Spark action.
    """
    provider_totals = (
        monthly_df.groupBy("npi")
        .agg(
            F.sum("total_paid").alias("total_paid_sum"),
            F.sum("total_claims").alias("total_claims_sum"),
        )
        .filter(F.col("total_claims_sum") > 0)
        .withColumn(
            "paid_per_claim",
            F.col("total_paid_sum") / F.col("total_claims_sum"),
        )
    )

    # Compute median lazily: 1-row agg crossJoined back rather than .collect()
    median_df = provider_totals.agg(
        F.percentile_approx("paid_per_claim", 0.5).alias("median_val")
    )
    with_dev = (
        provider_totals.crossJoin(median_df)
        .withColumn("abs_dev", F.abs(F.col("paid_per_claim") - F.col("median_val")))
    )

    # MAD = median of |x - median|, also kept lazy
    mad_df = with_dev.agg(
        F.percentile_approx("abs_dev", 0.5).alias("mad_val")
    )

    return (
        with_dev.crossJoin(mad_df)
        # Null/zero MAD means no variance — nothing to flag
        .filter(F.col("mad_val").isNotNull() & (F.col("mad_val") > 0))
        .withColumn(
            "modified_zscore",
            (F.col("paid_per_claim") - F.col("median_val"))
            / (F.col("mad_val") * F.lit(1.4826)),
        )
        .filter(F.col("modified_zscore") > REVENUE_ZSCORE_THRESHOLD)
        .withColumn("flag_type", F.lit(REVENUE_OUTLIER))
        .withColumn(
            "description",
            F.concat_ws(
                "",
                F.lit("Revenue per claim $"),
                F.format_number(F.col("paid_per_claim"), 2),
                F.lit(" ("),
                F.format_number(F.col("modified_zscore"), 1),
                F.lit(" MADs above median/claim)"),
            ),
        )
        .withColumn(
            "severity",
            F.least(F.lit(1.0), F.col("modified_zscore") / F.lit(10.0)),
        )
        .withColumn(
            "evidence_json",
            F.to_json(F.struct(
                F.round(F.col("paid_per_claim"), 2).alias("paid_per_claim"),
                F.col("total_paid_sum").alias("total_paid"),
                F.col("total_claims_sum").alias("total_claims"),
                F.round(F.col("modified_zscore"), 2).alias("modified_zscore"),
            )),
        )
        .select("npi", "flag_type", "description", "severity", "evidence_json")
    )


def detect_billing_spikes(monthly_df: DataFrame) -> DataFrame:
    """Flag months where a provider's billing spikes vs their own historical average."""
    window = Window.partitionBy("npi")

    with_avg = monthly_df.withColumn(
        "provider_avg",
        F.avg("total_paid").over(window),
    ).withColumn(
        "row_count",
        F.count("*").over(window),
    )

    return (
        with_avg
        .filter(F.col("row_count") >= 3)        # need history to detect spikes
        .filter(F.col("provider_avg") > 0)
        .withColumn("ratio", F.col("total_paid") / F.col("provider_avg"))
        .filter(F.col("ratio") > SPIKE_MULTIPLIER)
        .withColumn("flag_type", F.lit(BILLING_SPIKE))
        .withColumn(
            "description",
            F.concat_ws(
                "",
                F.lit("Monthly paid $"),
                F.format_number(F.col("total_paid"), 2),
                F.lit(" in "),
                F.col("service_month").cast("string"),
                F.lit(" is "),
                F.format_number(F.col("ratio"), 1),
                F.lit("x their average $"),
                F.format_number(F.col("provider_avg"), 2),
            ),
        )
        .withColumn(
            "severity",
            F.least(F.lit(1.0), F.col("ratio") / F.lit(10.0)),
        )
        .withColumn(
            "evidence_json",
            F.to_json(F.struct(
                F.col("service_month").alias("month"),
                F.col("total_paid").alias("amount"),
                F.round(F.col("ratio"), 2).alias("ratio"),
            )),
        )
        .select("npi", "flag_type", "description", "severity", "evidence_json")
    )


def detect_suspicious_consistency(code_df: DataFrame) -> DataFrame:
    """Flag providers whose billing is dominated by one procedure billed at a
    suspiciously uniform per-claim rate — same logic as scanner/anomalies.py.
    """
    # Per-provider total paid
    provider_totals = (
        code_df.groupBy("npi")
        .agg(F.sum("total_paid").alias("grand_total"))
    )

    # Per (npi, procedure_code) totals — to find the dominant code
    proc_totals = (
        code_df.groupBy("npi", "procedure_code")
        .agg(
            F.sum("total_paid").alias("proc_paid"),
            F.sum("total_claims").alias("proc_claims"),
        )
        .join(provider_totals, on="npi")
        .withColumn("dominance", F.col("proc_paid") / F.col("grand_total"))
    )

    # Keep only the single highest-revenue procedure per provider
    window_dom = Window.partitionBy("npi").orderBy(F.col("dominance").desc())
    dominant = (
        proc_totals
        .withColumn("dom_rank", F.row_number().over(window_dom))
        .filter(F.col("dom_rank") == 1)
        .filter(F.col("dominance") >= DOMINANCE_THRESHOLD)
        .drop("dom_rank")
    )

    # Per-claim rate per (npi, procedure_code, month) — CV across months
    rate_stats = (
        code_df.filter(F.col("total_claims") > 0)
        .withColumn("rate", F.col("total_paid") / F.col("total_claims"))
        .groupBy("npi", "procedure_code")
        .agg(
            F.mean("rate").alias("mean_rate"),
            F.stddev("rate").alias("std_rate"),
            F.count("rate").alias("month_count"),
        )
        .withColumn("std_rate", F.coalesce(F.col("std_rate"), F.lit(0.0)))
        .filter(F.col("month_count") >= CONSISTENCY_MIN_MONTHS)
        .withColumn("cv", F.col("std_rate") / F.col("mean_rate"))
        .filter(F.col("cv") < RATE_CV_THRESHOLD)
    )

    return (
        dominant.join(rate_stats, on=["npi", "procedure_code"])
        .withColumn("flag_type", F.lit(SUSPICIOUS_CONSISTENCY))
        .withColumn(
            "description",
            F.concat_ws(
                "",
                F.format_number(F.col("dominance") * 100, 0),
                F.lit("% of billing is procedure "),
                F.col("procedure_code"),
                F.lit(" at $"),
                F.format_number(F.col("mean_rate"), 2),
                F.lit("/claim with "),
                F.format_number(F.col("cv") * 100, 1),
                F.lit("% rate variation across "),
                F.col("month_count").cast("string"),
                F.lit(" months"),
            ),
        )
        .withColumn(
            "severity",
            F.least(F.lit(1.0), F.col("dominance") * (F.lit(1.0) - F.col("cv"))),
        )
        .withColumn(
            "evidence_json",
            F.to_json(F.struct(
                F.col("procedure_code"),
                F.round(F.col("dominance"), 3).alias("dominance_ratio"),
                F.round(F.col("mean_rate"), 2).alias("mean_rate_per_claim"),
                F.round(F.col("cv"), 4).alias("rate_cv"),
                F.col("month_count").alias("months_observed"),
            )),
        )
        .select("npi", "flag_type", "description", "severity", "evidence_json")
    )


def score_and_rank(flags_df: DataFrame) -> DataFrame:
    """Aggregate per-provider flags into a ranked result with an overall score.

    Returns a DataFrame with columns:
        npi, overall_score, num_flags, distinct_types, flag_types

    Uses conditional-max aggregation instead of collect_set to avoid
    ObjectHashAggregateExec Python worker serialization issues in Spark 4.x.
    """
    agg_df = (
        flags_df
        .groupBy("npi")
        .agg(
            F.max("severity").alias("max_severity"),
            F.count("*").alias("num_flags"),
            # One column per flag type: non-null when that type is present
            F.max(F.when(F.col("flag_type") == VOLUME_IMPOSSIBILITY,
                         F.lit(VOLUME_IMPOSSIBILITY))).alias("_v"),
            F.max(F.when(F.col("flag_type") == REVENUE_OUTLIER,
                         F.lit(REVENUE_OUTLIER))).alias("_r"),
            F.max(F.when(F.col("flag_type") == BILLING_SPIKE,
                         F.lit(BILLING_SPIKE))).alias("_s"),
            F.max(F.when(F.col("flag_type") == SUSPICIOUS_CONSISTENCY,
                         F.lit(SUSPICIOUS_CONSISTENCY))).alias("_c"),
        )
    )

    # concat_ws skips nulls — gives "type1, type2" for only present types
    return (
        agg_df
        .withColumn(
            "flag_types",
            F.concat_ws(", ", F.col("_v"), F.col("_r"), F.col("_s"), F.col("_c")),
        )
        .withColumn(
            "distinct_types",
            F.when(F.col("_v").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("_r").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("_s").isNotNull(), F.lit(1)).otherwise(F.lit(0))
            + F.when(F.col("_c").isNotNull(), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "overall_score",
            F.least(
                F.lit(1.0),
                F.col("max_severity") * F.lit(0.5)
                + F.col("distinct_types").cast("double") * F.lit(0.2),
            ),
        )
        .drop("_v", "_r", "_s", "_c")
        .orderBy(F.col("overall_score").desc())
    )


def _empty_flags_df(spark) -> DataFrame:
    """Return an empty DataFrame with the standard flags schema."""
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    schema = StructType([
        StructField("npi", StringType()),
        StructField("flag_type", StringType()),
        StructField("description", StringType()),
        StructField("severity", DoubleType()),
        StructField("evidence_json", StringType()),
    ])
    return spark.createDataFrame([], schema)


def run_all_detectors(
    monthly_df: DataFrame,
    procedure_df: DataFrame,
    threshold: float = 0.3,
) -> DataFrame:
    """Run all four detectors and return a scored, ranked result DataFrame."""
    monthly_q = filter_qualifying_providers(monthly_df)
    procedure_q = procedure_df.join(
        monthly_q.select("npi").distinct(), on="npi", how="inner"
    )

    volume = detect_volume_impossibility(monthly_q)
    revenue = detect_revenue_outliers(monthly_q)
    spikes = detect_billing_spikes(monthly_q)
    consistency = detect_suspicious_consistency(procedure_q)

    all_flags = volume.union(revenue).union(spikes).union(consistency)

    scored = score_and_rank(all_flags)
    return scored.filter(F.col("overall_score") >= threshold)
