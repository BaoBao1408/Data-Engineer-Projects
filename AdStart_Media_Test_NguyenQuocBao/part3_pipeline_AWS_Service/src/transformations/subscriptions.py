"""
src/transformations/subscriptions.py — Build fct_subscriptions + fct_unattributed_events.

Attribution logic per operator (giống y hệt SQL version, ported sang pandas):
  - operator_A: direct rotate_id → clicks → campaign_id
  - operator_B: direct rotate_id (SUB rows only) → clicks → campaign_id
  - operator_C: tracking_code → raw_tracking_codes (30min window) → rotate_id → campaign_id

Unattributed operator_C rows → fct_unattributed_events (Layer 1 quarantine)

AWS note:
    Trước đây dùng DuckDB SQL files. Bây giờ dùng pandas merge/join.
    Logic hoàn toàn giống nhau — chỉ đổi SQL JOIN → DataFrame merge.
    awswrangler write → S3 Parquet + Glue Catalog update.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, timezone, datetime
from typing import Tuple

import pandas as pd

from src.utils.aws_warehouse import AWSWarehouse

logger = logging.getLogger(__name__)


def _now_str() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_uuid() -> str:
    return str(uuid.uuid4())


# ── Operator A subscriptions ─────────────────────────────────────

def _build_subs_operator_a(
    df_a: pd.DataFrame,
    df_clicks: pd.DataFrame,
    df_dim: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    """
    operator_A: event_code=1 (subscribe), status=SUCCESS
    Attribution: rotate_id → raw_clicks → campaign_id → dim_campaigns
    """
    subs_a = df_a[
        (df_a["event_code"] == 1) &
        (df_a["status"].str.upper() == "SUCCESS") &
        (df_a["_loaded_date"] == str(run_date))
    ].copy()

    if subs_a.empty:
        logger.info(f"[fct_subscriptions] Không có operator_A subscriptions cho {run_date}")
        return pd.DataFrame()

    # JOIN: operator_a → clicks (on rotate_id)
    merged = subs_a.merge(
        df_clicks[["rotate_id", "campaign_id"]].drop_duplicates("rotate_id"),
        on="rotate_id",
        how="inner",
    )

    # JOIN: clicks → dim_campaigns (on campaign_id)
    merged = merged.merge(
        df_dim[["campaign_id", "service_name", "partner_id"]],
        on="campaign_id",
        how="inner",
    )

    result = pd.DataFrame({
        "subscription_id":      [_gen_uuid() for _ in range(len(merged))],
        "operator":             "operator_A",
        "source_transaction_id": merged["transaction_id"].values,
        "rotate_id":            merged["rotate_id"].values,
        "campaign_id":          merged["campaign_id"].values,
        "service_name":         merged["service_name"].values,
        "partner_id":           merged["partner_id"].values,
        "msisdn":               merged["msisdn"].values,
        "subscribed_at":        merged["event_time"].values,
        "report_date":          str(run_date),
        "attribution_method":   "direct_rotate_id",
        "loaded_at":            _now_str(),
    })
    return result


# ── Operator B subscriptions ─────────────────────────────────────

def _build_subs_operator_b(
    df_b: pd.DataFrame,
    df_clicks: pd.DataFrame,
    df_dim: pd.DataFrame,
    run_date: date,
) -> pd.DataFrame:
    """
    operator_B: transaction_type='SUB' only (REN/UNSUB không có rotate_id)
    Attribution: rotate_id → raw_clicks → campaign_id
    """
    subs_b = df_b[
        (df_b["transaction_type"].str.upper() == "SUB") &
        (df_b["rotate_id"].notna()) &
        (df_b["_loaded_date"] == str(run_date))
    ].copy()

    if subs_b.empty:
        logger.info(f"[fct_subscriptions] Không có operator_B subscriptions cho {run_date}")
        return pd.DataFrame()

    merged = subs_b.merge(
        df_clicks[["rotate_id", "campaign_id"]].drop_duplicates("rotate_id"),
        on="rotate_id",
        how="inner",
    )
    merged = merged.merge(
        df_dim[["campaign_id", "service_name", "partner_id"]],
        on="campaign_id",
        how="inner",
    )

    result = pd.DataFrame({
        "subscription_id":      [_gen_uuid() for _ in range(len(merged))],
        "operator":             "operator_B",
        "source_transaction_id": merged["transaction_id"].values,
        "rotate_id":            merged["rotate_id"].values,
        "campaign_id":          merged["campaign_id"].values,
        "service_name":         merged["service_name"].values,
        "partner_id":           merged["partner_id"].values,
        "msisdn":               merged["msisdn"].values,
        "subscribed_at":        merged["created_at"].values,
        "report_date":          str(run_date),
        "attribution_method":   "direct_rotate_id",
        "loaded_at":            _now_str(),
    })
    return result


# ── Operator C subscriptions + quarantine ────────────────────────

def _build_subs_operator_c(
    df_c: pd.DataFrame,
    df_tc: pd.DataFrame,    # raw_tracking_codes
    df_clicks: pd.DataFrame,
    df_dim: pd.DataFrame,
    run_date: date,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    operator_C: DELIVERED = subscribe + bill in one event.
    Attribution: tracking_code → raw_tracking_codes (30min window) → rotate_id

    Returns: (attributed_df, unattributed_df)
    Unattributed rows go to fct_unattributed_events (Layer 1 quarantine).
    """
    delivered = df_c[
        (df_c["delivery_status"] == "DELIVERED") &
        (df_c["_loaded_date"] == str(run_date))
    ].copy()

    if delivered.empty:
        logger.info(f"[fct_subscriptions] Không có operator_C DELIVERED rows cho {run_date}")
        return pd.DataFrame(), pd.DataFrame()

    # ── Step 1: Tách bad tracking codes ra quarantine ngay ────────
    bad_mask = delivered["tracking_code"].str.len() > 3
    bad_rows = delivered[bad_mask].copy()
    good_rows = delivered[~bad_mask].copy()

    # ── Step 2: JOIN với tracking_codes (30-min validity window) ──
    # Athena equivalent: ON tc.code = oc.tracking_code
    #                    AND oc.received_time BETWEEN tc.created_at AND tc.expired_at
    merged = good_rows.merge(
        df_tc[["rotate_id", "code", "created_at", "expired_at"]].rename(
            columns={"created_at": "tc_created_at", "expired_at": "tc_expired_at"}
        ),
        left_on="tracking_code",
        right_on="code",
        how="left",
    )

    # Apply 30-min window filter
    within_window = (
        (merged["received_time"] >= merged["tc_created_at"]) &
        (merged["received_time"] <= merged["tc_expired_at"])
    )
    matched   = merged[within_window].copy()
    no_window = merged[~within_window & merged["rotate_id"].isna()].copy()

    # ── Step 3: JOIN với clicks → dim_campaigns ───────────────────
    attributed = matched.merge(
        df_clicks[["rotate_id", "campaign_id"]].drop_duplicates("rotate_id"),
        on="rotate_id", how="inner",
    ).merge(
        df_dim[["campaign_id", "service_name", "partner_id"]],
        on="campaign_id", how="inner",
    )

    # Rows không match clicks
    no_click = matched[~matched["rotate_id"].isin(attributed["rotate_id"])].copy()

    # ── Build attributed DataFrame ────────────────────────────────
    df_attr = pd.DataFrame()
    if not attributed.empty:
        df_attr = pd.DataFrame({
            "subscription_id":       [_gen_uuid() for _ in range(len(attributed))],
            "operator":              "operator_C",
            "source_transaction_id": attributed["message_id"].values,
            "rotate_id":             attributed["rotate_id"].values,
            "campaign_id":           attributed["campaign_id"].values,
            "service_name":          attributed["service_name"].values,
            "partner_id":            attributed["partner_id"].values,
            "msisdn":                attributed["msisdn"].values,
            "subscribed_at":         attributed["received_time"].values,
            "report_date":           str(run_date),
            "attribution_method":    "tracking_code_lookup",
            "loaded_at":             _now_str(),
        })

    # ── Build quarantine DataFrame ────────────────────────────────
    def _build_quarantine(df_src: pd.DataFrame, reason: str) -> pd.DataFrame:
        if df_src.empty:
            return pd.DataFrame()
        return pd.DataFrame({
            "event_id":             [_gen_uuid() for _ in range(len(df_src))],
            "operator":             "operator_C",
            "source_table":         "raw_operator_c",
            "msisdn":               df_src["msisdn"].values,
            "raw_tracking_code":    df_src["tracking_code"].values,
            "event_time":           df_src["received_time"].values,
            "report_date":          str(run_date),
            "unattributed_reason":  reason,
            "loaded_at":            _now_str(),
        })

    quarantine_frames = []
    if not bad_rows.empty:
        quarantine_frames.append(
            _build_quarantine(bad_rows, "tracking_code_too_long")
        )
    if not no_window.empty:
        quarantine_frames.append(
            _build_quarantine(no_window, "tracking_code_expired_or_no_match")
        )
    if not no_click.empty:
        quarantine_frames.append(
            _build_quarantine(no_click, "no_matching_click_record")
        )

    df_quarantine = (
        pd.concat(quarantine_frames, ignore_index=True)
        if quarantine_frames else pd.DataFrame()
    )

    return df_attr, df_quarantine


# ── Main build function ───────────────────────────────────────────

def build_fct_subscriptions(warehouse: AWSWarehouse, run_date: date) -> int:
    """
    Merge subscription events từ 3 operators vào fct_subscriptions.
    Quarantine unattributed operator_C rows vào fct_unattributed_events.

    IDEMPOTENCY:
        mode="overwrite_partitions" → xóa partition report_date=run_date
        trước khi ghi → safe để re-run cùng ngày.
    """
    # ── Load raw tables ──────────────────────────────────────────
    df_a   = warehouse.query(f"SELECT * FROM raw_operator_a WHERE _loaded_date='{run_date}'", layer="raw")
    df_b   = warehouse.query(f"SELECT * FROM raw_operator_b WHERE _loaded_date='{run_date}'", layer="raw")
    df_c   = warehouse.query(f"SELECT * FROM raw_operator_c WHERE _loaded_date='{run_date}'", layer="raw")
    df_tc  = warehouse.query("SELECT * FROM raw_tracking_codes", layer="raw")
    df_cl  = warehouse.query("SELECT * FROM raw_clicks", layer="raw")
    df_dim = warehouse.query("SELECT * FROM dim_campaigns", layer="dimensions")

    # ── Build per-operator ────────────────────────────────────────
    df_a_subs = _build_subs_operator_a(df_a, df_cl, df_dim, run_date)
    df_b_subs = _build_subs_operator_b(df_b, df_cl, df_dim, run_date)
    df_c_subs, df_c_quarantine = _build_subs_operator_c(df_c, df_tc, df_cl, df_dim, run_date)

    # ── Combine + write fct_subscriptions ────────────────────────
    all_subs_frames = [f for f in [df_a_subs, df_b_subs, df_c_subs] if not f.empty]
    if not all_subs_frames:
        logger.warning(f"[fct_subscriptions] Không có subscriptions nào cho {run_date}")
        return 0

    df_all_subs = pd.concat(all_subs_frames, ignore_index=True)
    warehouse.write_table(df_all_subs, layer="facts", table="fct_subscriptions",
                          partition_date=run_date, mode="overwrite_partitions")

    # ── Write quarantine ─────────────────────────────────────────
    if not df_c_quarantine.empty:
        warehouse.write_table(df_c_quarantine, layer="facts",
                              table="fct_unattributed_events",
                              partition_date=run_date, mode="overwrite_partitions")
        total_c     = len(df_c[df_c["delivery_status"] == "DELIVERED"])
        unattr_pct  = len(df_c_quarantine) / total_c * 100 if total_c else 0
        logger.warning(
            f"[fct_subscriptions] operator_C attribution cho {run_date}: "
            f"{len(df_c_subs) if not df_c_subs.empty else 0}/{total_c} "
            f"attributed ({100-unattr_pct:.1f}%) — "
            f"quarantined {len(df_c_quarantine)} rows."
        )

    count = len(df_all_subs)
    logger.info(f"[fct_subscriptions] {count:,} rows inserted for {run_date}.")
    return count
