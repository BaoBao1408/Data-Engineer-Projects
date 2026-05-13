"""
src/transformations/billing_clicks_mart.py — Build fct_billing, fct_clicks, mart_daily_performance.

Ported from SQL files (DuckDB) → pandas merge logic.
Business logic is UNCHANGED — only the execution engine has changed.

AWS note:
    fct_billing    → s3://warehouse/facts/fct_billing/report_date=YYYY-MM-DD/
    fct_clicks     → s3://warehouse/facts/fct_clicks/report_date=YYYY-MM-DD/
    mart_*         → s3://warehouse/mart/mart_daily_performance/report_date=YYYY-MM-DD/
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, timezone, datetime

import pandas as pd
import numpy as np

from src.utils.aws_warehouse import AWSWarehouse

logger = logging.getLogger(__name__)


def _now_str() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_uuids(n: int) -> list[str]:
    return [str(uuid.uuid4()) for _ in range(n)]


# ── fct_billing ───────────────────────────────────────────────────

def build_fct_billing(warehouse: AWSWarehouse, run_date: date) -> int:
    """
    Build fct_billing from operator_a (event_code=2) + operator_b (REN rows).

    billing_sequence + is_first_bill are calculated using a window function:
        ROW_NUMBER() OVER (PARTITION BY msisdn, campaign_id ORDER BY billed_at)

    Operator A: event_code=2, status=SUCCESS
    Operator B: transaction_type=REN + SUB (with amount > 0)
    """
    df_a      = warehouse.query(f"SELECT * FROM raw_operator_a WHERE _loaded_date='{run_date}'", layer="raw")
    df_b      = warehouse.query(f"SELECT * FROM raw_operator_b WHERE _loaded_date='{run_date}'", layer="raw")
    df_cl     = warehouse.query("SELECT * FROM raw_clicks", layer="raw")
    df_dim    = warehouse.query("SELECT * FROM dim_campaigns", layer="dimensions")
    df_subs   = warehouse.query(
        f"SELECT * FROM fct_subscriptions WHERE report_date='{run_date}'",
        layer="facts"
    )

    frames = []

    # ── Operator A billings ───────────────────────────────────────
    bills_a = df_a[
        (df_a["event_code"] == 2) &
        (df_a["status"].str.upper() == "SUCCESS") &
        (df_a["_loaded_date"] == str(run_date))
    ].copy()

    if not bills_a.empty:
        merged_a = bills_a.merge(
            df_cl[["rotate_id", "campaign_id"]].drop_duplicates("rotate_id"),
            on="rotate_id", how="inner"
        ).merge(
            df_dim[["campaign_id", "service_name", "partner_id"]],
            on="campaign_id", how="inner"
        )

        # LEFT JOIN with fct_subscriptions to get subscription_id
        if not df_subs.empty:
            # Get the most recent subscription before the billing time
            subs_a = df_subs[df_subs["operator"] == "operator_A"][
                ["msisdn", "campaign_id", "subscription_id", "subscribed_at"]
            ].sort_values("subscribed_at")
            merged_a = merged_a.merge(
                subs_a.rename(columns={"subscribed_at": "sub_at"}),
                on=["msisdn", "campaign_id"],
                how="left"
            )
            # Keep only the most recent subscription before billed_at
            merged_a["event_time"] = pd.to_datetime(merged_a["event_time"], utc=True, errors="coerce")
            if "sub_at" in merged_a.columns:
                merged_a["sub_at"] = pd.to_datetime(merged_a["sub_at"], utc=True, errors="coerce")
                merged_a = merged_a[
                    merged_a["sub_at"].isna() | (merged_a["sub_at"] <= merged_a["event_time"])
                ]
                merged_a = merged_a.sort_values("sub_at", ascending=False).drop_duplicates(
                    subset=["transaction_id"]
                )
        else:
            merged_a["subscription_id"] = None

        # Window function: billing_sequence + is_first_bill
        merged_a = merged_a.sort_values("event_time")
        merged_a["billing_sequence"] = merged_a.groupby(["msisdn", "campaign_id"]).cumcount() + 1
        merged_a["is_first_bill"]    = merged_a["billing_sequence"] == 1

        df_bills_a = pd.DataFrame({
            "billing_id":            _gen_uuids(len(merged_a)),
            "operator":              "operator_A",
            "source_transaction_id": merged_a["transaction_id"].values,
            "subscription_id":       merged_a.get("subscription_id", pd.Series([None]*len(merged_a))).values,
            "campaign_id":           merged_a["campaign_id"].values,
            "service_name":          merged_a["service_name"].values,
            "partner_id":            merged_a["partner_id"].values,
            "msisdn":                merged_a["msisdn"].values,
            "amount":                pd.to_numeric(merged_a["amount"], errors="coerce").fillna(0).values,
            "currency":              "GBP",
            "billed_at":             merged_a["event_time"].values,
            "report_date":           str(run_date),
            "is_first_bill":         merged_a["is_first_bill"].values,
            "billing_sequence":      merged_a["billing_sequence"].values,
            "billing_status":        "SUCCESS",
            "loaded_at":             _now_str(),
        })
        frames.append(df_bills_a)
        logger.info(f"[fct_billing] operator_A: {len(df_bills_a):,} billing rows.")

    # ── Operator B billings (REN + SUB with amount) ───────────────
    bills_b = df_b[
        (df_b["transaction_type"].str.upper().isin(["REN", "SUB"])) &
        (pd.to_numeric(df_b["amount"], errors="coerce").fillna(0) > 0) &
        (df_b["_loaded_date"] == str(run_date))
    ].copy()

    if not bills_b.empty and not df_cl.empty:
        merged_b = bills_b[bills_b["rotate_id"].notna()].merge(
            df_cl[["rotate_id", "campaign_id"]].drop_duplicates("rotate_id"),
            on="rotate_id", how="inner"
        ).merge(
            df_dim[["campaign_id", "service_name", "partner_id"]],
            on="campaign_id", how="inner"
        )

        if not merged_b.empty:
            merged_b["created_at"]       = pd.to_datetime(merged_b["created_at"], utc=True, errors="coerce")
            merged_b                     = merged_b.sort_values("created_at")
            merged_b["billing_sequence"] = merged_b.groupby(["msisdn", "campaign_id"]).cumcount() + 1
            merged_b["is_first_bill"]    = merged_b["billing_sequence"] == 1

            df_bills_b = pd.DataFrame({
                "billing_id":            _gen_uuids(len(merged_b)),
                "operator":              "operator_B",
                "source_transaction_id": merged_b["transaction_id"].values,
                "subscription_id":       None,
                "campaign_id":           merged_b["campaign_id"].values,
                "service_name":          merged_b["service_name"].values,
                "partner_id":            merged_b["partner_id"].values,
                "msisdn":                merged_b["msisdn"].values,
                "amount":                pd.to_numeric(merged_b["amount"], errors="coerce").fillna(0).values,
                "currency":              "GBP",
                "billed_at":             merged_b["created_at"].values,
                "report_date":           str(run_date),
                "is_first_bill":         merged_b["is_first_bill"].values,
                "billing_sequence":      merged_b["billing_sequence"].values,
                "billing_status":        "SUCCESS",
                "loaded_at":             _now_str(),
            })
            frames.append(df_bills_b)
            logger.info(f"[fct_billing] operator_B: {len(df_bills_b):,} billing rows.")

    if not frames:
        logger.warning(f"[fct_billing] No billing rows found for {run_date}")
        return 0

    df_billing = pd.concat(frames, ignore_index=True)
    warehouse.write_table(df_billing, layer="facts", table="fct_billing",
                          partition_date=run_date, mode="overwrite_partitions")
    logger.info(f"[fct_billing] Total {len(df_billing):,} rows written for {run_date}.")
    return len(df_billing)


# ── fct_clicks ────────────────────────────────────────────────────

def build_fct_clicks(warehouse: AWSWarehouse, run_date: date) -> int:
    """
    Build fct_clicks — denormalised click events with subscription + billing flags.

    Each row = 1 click event, enriched with:
      - has_page_view   : click has a page_view in the same session
      - has_cta_click   : click has a CTA click event
      - has_entry       : click has an entry event (user entered msisdn)
      - has_subscription: click led to a subscription
      - has_first_bill  : subscription led to a first billing

    Raw clicks have no date partition → filter by clicked_at::date = run_date
    """
    df_cl  = warehouse.query("SELECT * FROM raw_clicks", layer="raw")
    df_pe  = warehouse.query("SELECT * FROM raw_page_events", layer="raw")
    df_dim = warehouse.query("SELECT * FROM dim_campaigns", layer="dimensions")
    df_subs = warehouse.query(
        f"SELECT * FROM fct_subscriptions WHERE report_date='{run_date}'",
        layer="facts"
    )
    df_bill = warehouse.query(
        f"SELECT * FROM fct_billing WHERE report_date='{run_date}'",
        layer="facts"
    )

    # Filter clicks for run_date
    df_cl["clicked_at"] = pd.to_datetime(df_cl["clicked_at"], utc=True, errors="coerce")
    day_clicks = df_cl[df_cl["clicked_at"].dt.date == run_date].copy()

    if day_clicks.empty:
        logger.warning(f"[fct_clicks] No clicks found for {run_date}")
        return 0

    # JOIN with dim_campaigns
    day_clicks = day_clicks.merge(
        df_dim[["campaign_id", "operator", "service_name", "partner_id"]],
        on="campaign_id", how="inner"
    )

    # ── Page event flags (pivot by event_type) ────────────────────
    if not df_pe.empty:
        df_pe["rotate_id"] = df_pe["rotate_id"].astype(str)
        pe_pivot = df_pe.groupby("rotate_id")["event_type"].apply(
            lambda x: set(x.str.upper())
        ).reset_index().rename(columns={"event_type": "event_set"})
        pe_pivot["has_page_view"]  = pe_pivot["event_set"].apply(lambda s: "PAGE_VIEW" in s)
        pe_pivot["has_cta_click"]  = pe_pivot["event_set"].apply(lambda s: "CTA_CLICK" in s)
        pe_pivot["has_entry"]      = pe_pivot["event_set"].apply(lambda s: "ENTRY" in s)
        day_clicks = day_clicks.merge(
            pe_pivot[["rotate_id", "has_page_view", "has_cta_click", "has_entry"]],
            on="rotate_id", how="left"
        )
    else:
        day_clicks["has_page_view"] = False
        day_clicks["has_cta_click"] = False
        day_clicks["has_entry"]     = False

    # Fill NaN flags
    for flag in ["has_page_view", "has_cta_click", "has_entry"]:
        day_clicks[flag] = day_clicks[flag].fillna(False)

    # ── Subscription flag ─────────────────────────────────────────
    if not df_subs.empty:
        sub_rotate_ids = set(df_subs["rotate_id"].dropna().astype(str))
        day_clicks["has_subscription"] = day_clicks["rotate_id"].isin(sub_rotate_ids)
    else:
        day_clicks["has_subscription"] = False

    # ── First bill flag ───────────────────────────────────────────
    if not df_bill.empty:
        first_bill_subs = set(df_bill[df_bill["is_first_bill"] == True]["subscription_id"].dropna())
        if not df_subs.empty:
            subs_with_first_bill = set(
                df_subs[df_subs["subscription_id"].isin(first_bill_subs)]["rotate_id"].dropna()
            )
            day_clicks["has_first_bill"] = day_clicks["rotate_id"].isin(subs_with_first_bill)
        else:
            day_clicks["has_first_bill"] = False
    else:
        day_clicks["has_first_bill"] = False

    # ── Build final DataFrame ─────────────────────────────────────
    df_fct_clicks = pd.DataFrame({
        "click_id":          _gen_uuids(len(day_clicks)),
        "rotate_id":         day_clicks["rotate_id"].values,
        "campaign_id":       day_clicks["campaign_id"].values,
        "operator":          day_clicks["operator"].values,
        "service_name":      day_clicks["service_name"].values,
        "partner_id":        day_clicks["partner_id"].values,
        "pub_id":            day_clicks.get("pub_id", pd.Series([None]*len(day_clicks))).values,
        "clicked_at":        day_clicks["clicked_at"].values,
        "report_date":       str(run_date),
        "has_page_view":     day_clicks["has_page_view"].values,
        "has_cta_click":     day_clicks["has_cta_click"].values,
        "has_entry":         day_clicks["has_entry"].values,
        "has_subscription":  day_clicks["has_subscription"].values,
        "has_first_bill":    day_clicks["has_first_bill"].values,
        "loaded_at":         _now_str(),
    })

    warehouse.write_table(df_fct_clicks, layer="facts", table="fct_clicks",
                          partition_date=run_date, mode="overwrite_partitions")
    logger.info(f"[fct_clicks] {len(df_fct_clicks):,} rows written for {run_date}.")
    return len(df_fct_clicks)


# ── mart_daily_performance ────────────────────────────────────────

def build_mart_daily_performance(warehouse: AWSWarehouse, run_date: date) -> int:
    """
    Pre-aggregated daily performance rollup consumed by BI tools.

    Granularity: 1 row per (report_date, campaign_id, operator)

    Metrics:
      - Clicks funnel : total_clicks, page_views, cta_clicks, entries
      - Conversion    : subscriptions, first_bills, renewals
      - Revenue       : total_revenue (GBP)
      - Rates         : sub_conversion_rate, bill_conversion_rate
    """
    df_clicks = warehouse.query(
        f"SELECT * FROM fct_clicks WHERE report_date='{run_date}'",
        layer="facts"
    )
    df_billing = warehouse.query(
        f"SELECT * FROM fct_billing WHERE report_date='{run_date}'",
        layer="facts"
    )

    if df_clicks.empty:
        logger.warning(f"[mart_daily_performance] No fct_clicks data found for {run_date}")
        return 0

    # ── Aggregate clicks ──────────────────────────────────────────
    # Convert bool columns
    bool_cols = ["has_page_view", "has_cta_click", "has_entry",
                 "has_subscription", "has_first_bill"]
    for col in bool_cols:
        if col in df_clicks.columns:
            df_clicks[col] = df_clicks[col].astype(bool)

    agg = df_clicks.groupby(
        ["campaign_id", "operator", "service_name", "partner_id"]
    ).agg(
        total_clicks          = ("click_id", "count"),
        total_page_views      = ("has_page_view", "sum"),
        total_cta_clicks      = ("has_cta_click", "sum"),
        total_entries         = ("has_entry", "sum"),
        total_subscriptions   = ("has_subscription", "sum"),
        total_first_bills     = ("has_first_bill", "sum"),
    ).reset_index()

    # ── Revenue + renewals from fct_billing ───────────────────────
    if not df_billing.empty:
        revenue_agg = df_billing.groupby("campaign_id").agg(
            total_revenue = ("amount", "sum"),
            total_renewals = ("is_first_bill", lambda x: (~x.astype(bool)).sum())
        ).reset_index()
        agg = agg.merge(revenue_agg, on="campaign_id", how="left")
    else:
        agg["total_revenue"]  = 0.0
        agg["total_renewals"] = 0

    agg["total_revenue"]  = agg["total_revenue"].fillna(0.0)
    agg["total_renewals"] = agg["total_renewals"].fillna(0).astype(int)

    # ── Conversion rates ──────────────────────────────────────────
    agg["sub_conversion_rate"]  = (
        agg["total_subscriptions"] / agg["total_clicks"].replace(0, np.nan)
    ).round(6).fillna(0)
    agg["bill_conversion_rate"] = (
        agg["total_first_bills"] / agg["total_clicks"].replace(0, np.nan)
    ).round(6).fillna(0)

    # ── Final columns ─────────────────────────────────────────────
    agg["report_date"] = str(run_date)
    agg["currency"]    = "GBP"
    agg["loaded_at"]   = _now_str()

    warehouse.write_table(agg, layer="mart", table="mart_daily_performance",
                          partition_date=run_date, mode="overwrite_partitions")
    logger.info(f"[mart_daily_performance] {len(agg):,} campaign rows for {run_date}.")
    return len(agg)