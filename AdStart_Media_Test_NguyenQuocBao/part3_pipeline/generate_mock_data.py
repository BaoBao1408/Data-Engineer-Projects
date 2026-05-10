"""
generate_mock_data.py — Generate realistic mock CSV data for all 7 source files.

Output (written to data/ folder):
  campaigns.csv       10 rows   (static reference)
  clicks.csv         600 rows   (spine — reduced from 6k for fast local testing)
  tracking_codes.csv ~120 rows  (subset of clicks that go through Op C)
  page_events.csv    ~730 rows  (VIEW / CLICK_CTA / ENTRY events)
  operator_a.csv     ~320 rows  (subscribe / bill / unsubscribe)
  operator_b.csv     ~330 rows  (SUB / REN / UNSUB)
  operator_c.csv      ~74 rows  (DELIVERED / SMSC_QUEUED / FAILED)

Column names match the real CSVs — all timestamps in "received_time" as the source.
Quirks intentionally replicated from the real dataset:
  - operator_b REN/UNSUB: rotate_id is always NULL
  - operator_b SUB: rotate_id always populated
  - operator_c: ~13% of tracking_codes are 4-5 chars (known SMS parser bug)
  - operator_a: ~82 cases where bill arrives before subscribe (race condition)
  - page_events msisdn: only set on ENTRY events (~10% of all events)
  - operator_a event_code=2 has amount; codes 1 and 3 have amount=0

Run:
  python generate_mock_data.py
  python generate_mock_data.py --rows 6000   # full-scale
"""
import argparse
import csv
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

random.seed(42)

# ── Constants ────────────────────────────────────────────────────────────────
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

JAN_2026_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
JAN_2026_END   = datetime(2026, 1, 31, 23, 59, 59, tzinfo=timezone.utc)

OPERATORS      = ["operator_A", "operator_B", "operator_C"]
SERVICES       = ["service_1", "service_2", "service_3", "service_4", "service_5"]
SERVICE_MODELS = {"service_1": "subscription", "service_2": "subscription",
                  "service_3": "subscription", "service_4": "one-off",
                  "service_5": "subscription"}
PARTNERS       = [str(uuid.UUID(int=i * 0x1000 + 0xABC)) for i in range(1, 7)]
BILL_AMOUNTS   = {"operator_A": 2.99, "operator_B": 1.99, "operator_C": 0.00}
OP_A_STATUSES  = ["SUCCESS", "FAILED", "PENDING"]
OP_A_STATUS_W  = [0.51, 0.37, 0.12]   # weights matching real data distribution
UK_PREFIX      = "4477"


def rand_ts(start: datetime = JAN_2026_START, end: datetime = JAN_2026_END) -> str:
    delta = end - start
    return (start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))).isoformat()


def rand_msisdn() -> str:
    return UK_PREFIX + str(random.randint(10_000_000, 99_999_999))


def rand_uuid() -> str:
    return str(uuid.uuid4())


def rand_code(valid: bool = True) -> str:
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ0123456789"
    if valid:
        return "".join(random.choices(chars, k=3))
    # ~13% of real codes are 4-5 chars (SMS parser appends suffix)
    length = random.choice([4, 5])
    return "".join(random.choices(chars, k=length))


# ── 1. campaigns ─────────────────────────────────────────────────────────────
def make_campaigns() -> list[dict]:
    rows = []
    for i, op in enumerate(OPERATORS):
        for svc in random.sample(SERVICES, k=random.randint(2, 4)):
            rows.append({
                "id":            rand_uuid(),
                "country":       "GB",
                "operator":      op,
                "service_name":  svc,
                "service_model": SERVICE_MODELS[svc],
                "partner_id":    random.choice(PARTNERS),
                "status":        "active",
                "created_at":    "2025-12-01T00:00:00+00:00",
            })
            if len(rows) == 10:
                break
        if len(rows) == 10:
            break
    return rows[:10]


# ── 2. clicks ────────────────────────────────────────────────────────────────
def make_clicks(campaigns: list[dict], n: int) -> list[dict]:
    rows = []
    for _ in range(n):
        c = random.choice(campaigns)
        rows.append({
            "rotate_id":   rand_uuid(),
            "campaign_id": c["id"],
            "pub_id":      f"pub_{random.randint(100, 299)}",
            "received_time": rand_ts(),
        })
    return rows


# ── 3. tracking_codes ────────────────────────────────────────────────────────
def make_tracking_codes(clicks: list[dict], campaigns: list[dict]) -> list[dict]:
    """Only clicks from operator_C campaigns get tracking codes."""
    op_c_campaign_ids = {c["id"] for c in campaigns if c["operator"] == "operator_C"}
    op_c_clicks = [cl for cl in clicks if cl["campaign_id"] in op_c_campaign_ids]
    # ~20% of op_c clicks generate a tracking code
    selected = random.sample(op_c_clicks, k=max(1, len(op_c_clicks) // 5))
    rows = []
    for cl in selected:
        created = datetime.fromisoformat(cl["received_time"])
        expired = created + timedelta(minutes=30)
        rows.append({
            "rotate_id":  cl["rotate_id"],
            "code":       rand_code(valid=True),
            "service_id": f"svc_{random.randint(1, 5)}",
            "created_at": created.isoformat(),
            "expired_at": expired.isoformat(),
        })
    return rows


# ── 4. page_events ───────────────────────────────────────────────────────────
def make_page_events(clicks: list[dict]) -> list[dict]:
    """
    For each click, generate a realistic funnel sequence.
    ~100% VIEW, ~55% CLICK_CTA, ~33% ENTRY.
    msisdn only on ENTRY events (matches real dataset: 89.6% null overall).
    """
    rows = []
    msisdn_pool = [rand_msisdn() for _ in range(200)]

    for cl in clicks:
        base_ts = datetime.fromisoformat(cl["received_time"])

        # VIEW — always
        rows.append({
            "event_id":    rand_uuid(),
            "rotate_id":   cl["rotate_id"],
            "campaign_id": cl["campaign_id"],
            "event_type":  "VIEW",
            "msisdn":      "",
            "device_type": random.choice(["mobile", "mobile", "desktop"]),
            "received_time": (base_ts + timedelta(seconds=1)).isoformat(),
        })

        # CLICK_CTA — ~55%
        if random.random() < 0.55:
            cta_ts = base_ts + timedelta(seconds=random.randint(30, 120))
            rows.append({
                "event_id":    rand_uuid(),
                "rotate_id":   cl["rotate_id"],
                "campaign_id": cl["campaign_id"],
                "event_type":  "CLICK_CTA",
                "msisdn":      "",
                "device_type": "mobile",
                "received_time": cta_ts.isoformat(),
            })

            # ENTRY — ~33% of those who clicked CTA
            if random.random() < 0.60:
                entry_ts = cta_ts + timedelta(seconds=random.randint(20, 90))
                rows.append({
                    "event_id":    rand_uuid(),
                    "rotate_id":   cl["rotate_id"],
                    "campaign_id": cl["campaign_id"],
                    "event_type":  "ENTRY",
                    "msisdn":      random.choice(msisdn_pool),
                    "device_type": "mobile",
                    "received_time": entry_ts.isoformat(),
                })

    return rows


# ── 5. operator_a ────────────────────────────────────────────────────────────
def make_operator_a(clicks: list[dict], campaigns: list[dict]) -> list[dict]:
    """
    event_code: 1=subscribe, 2=bill, 3=unsubscribe
    ~48% of subscribes succeed. Bills have amount. Includes ~82 race-condition cases.
    """
    op_a_ids = {c["id"] for c in campaigns if c["operator"] == "operator_A"}
    op_a_clicks = [cl for cl in clicks if cl["campaign_id"] in op_a_ids]
    rows = []
    msisdn_map = {}

    for cl in op_a_clicks:
        msisdn = rand_msisdn()
        msisdn_map[cl["rotate_id"]] = msisdn
        base_ts = datetime.fromisoformat(cl["received_time"])

        # Subscribe event (code=1)
        sub_status = random.choices(OP_A_STATUSES, weights=OP_A_STATUS_W)[0]
        sub_ts = base_ts + timedelta(seconds=random.randint(10, 300))
        rows.append({
            "transaction_id": rand_uuid(),
            "rotate_id":      cl["rotate_id"],
            "msisdn":         msisdn,
            "received_time":  sub_ts.isoformat(),
            "event_code":     1,
            "status":         sub_status,
            "amount":         0.00,
            "currency":       "GBP",
        })

        if sub_status != "SUCCESS":
            continue

        # Billing events (code=2): 2–5 weekly charges
        n_bills = random.randint(2, 5)
        for week in range(1, n_bills + 1):
            bill_ts = sub_ts + timedelta(days=7 * week, seconds=random.randint(-120, 120))

            # Simulate race condition (~82/3194 in real data): bill arrives before subscribe
            # We replicate this by occasionally making bill_ts slightly before sub_ts
            race = (week == 1 and random.random() < 0.05)
            if race:
                bill_ts = sub_ts - timedelta(seconds=random.randint(7, 120))

            bill_status = random.choices(OP_A_STATUSES, weights=OP_A_STATUS_W)[0]
            rows.append({
                "transaction_id": rand_uuid(),
                "rotate_id":      cl["rotate_id"],
                "msisdn":         msisdn,
                "received_time":  bill_ts.isoformat(),
                "event_code":     2,
                "status":         bill_status,
                "amount":         BILL_AMOUNTS["operator_A"] if bill_status == "SUCCESS" else 0.00,
                "currency":       "GBP",
            })

        # Unsubscribe (code=3): ~15% of subscribers
        if random.random() < 0.15:
            unsub_ts = sub_ts + timedelta(days=random.randint(14, 45))
            rows.append({
                "transaction_id": rand_uuid(),
                "rotate_id":      cl["rotate_id"],
                "msisdn":         msisdn,
                "received_time":  unsub_ts.isoformat(),
                "event_code":     3,
                "status":         "SUCCESS",
                "amount":         0.00,
                "currency":       "GBP",
            })

    return rows


# ── 6. operator_b ────────────────────────────────────────────────────────────
def make_operator_b(clicks: list[dict], campaigns: list[dict]) -> list[dict]:
    """
    SUB: rotate_id always populated (user in browser session).
    REN: rotate_id is always NULL (triggered 7 days later, no session) — by design.
    UNSUB: rotate_id is always NULL — by design.
    """
    op_b_ids = {c["id"] for c in campaigns if c["operator"] == "operator_B"}
    op_b_clicks = [cl for cl in clicks if cl["campaign_id"] in op_b_ids]
    rows = []

    for cl in op_b_clicks:
        msisdn = rand_msisdn()
        base_ts = datetime.fromisoformat(cl["received_time"])
        sub_ts  = base_ts + timedelta(seconds=random.randint(10, 300))

        # SUB — rotate_id populated
        rows.append({
            "transaction_id":   rand_uuid(),
            "rotate_id":        cl["rotate_id"],
            "msisdn":           msisdn,
            "received_time":    sub_ts.isoformat(),
            "transaction_type": "SUB",
            "package_id":       f"pkg_{random.randint(1, 5)}",
            "amount":           0.00,
            "currency":         "GBP",
        })

        # REN — rotate_id NULL, 1–4 weekly renewals
        n_rens = random.randint(1, 4)
        for week in range(1, n_rens + 1):
            ren_ts = sub_ts + timedelta(days=7 * week, seconds=random.randint(-60, 60))
            rows.append({
                "transaction_id":   rand_uuid(),
                "rotate_id":        "",   # intentionally blank → will be NULL after ingest
                "msisdn":           msisdn,
                "received_time":    ren_ts.isoformat(),
                "transaction_type": "REN",
                "package_id":       f"pkg_{random.randint(1, 5)}",
                "amount":           BILL_AMOUNTS["operator_B"],
                "currency":         "GBP",
            })

        # UNSUB — rotate_id NULL, ~20% of subscribers
        if random.random() < 0.20:
            unsub_ts = sub_ts + timedelta(days=random.randint(14, 45))
            rows.append({
                "transaction_id":   rand_uuid(),
                "rotate_id":        "",
                "msisdn":           msisdn,
                "received_time":    unsub_ts.isoformat(),
                "transaction_type": "UNSUB",
                "package_id":       "",
                "amount":           0.00,
                "currency":         "GBP",
            })

    return rows


# ── 7. operator_c ────────────────────────────────────────────────────────────
def make_operator_c(tracking_codes: list[dict]) -> list[dict]:
    """
    One row per SMS delivered.
    ~13% have invalid tracking_code (4-5 chars) — replicates real SMS parser bug.
    delivery_status DELIVERED = subscribe + charge happened simultaneously.
    """
    rows = []
    for tc in tracking_codes:
        # Mostly valid, ~13% invalid
        use_bad_code = (random.random() < 0.13)
        code = rand_code(valid=False) if use_bad_code else tc["code"]

        recv_ts = datetime.fromisoformat(tc["created_at"]) + timedelta(minutes=random.randint(1, 25))

        status = random.choices(
            ["DELIVERED", "SMSC_QUEUED", "FAILED"],
            weights=[0.87, 0.08, 0.05]
        )[0]

        rows.append({
            "message_id":      rand_uuid(),
            "msisdn":          rand_msisdn(),
            "received_time":   recv_ts.isoformat(),
            "tracking_code":   code,
            "service_id":      tc["service_id"],
            "delivery_status": status,
        })

    return rows


# ── Writer ───────────────────────────────────────────────────────────────────
def write_csv(rows: list[dict], filename: str) -> None:
    if not rows:
        print(f"  [WARN] No rows generated for {filename} — skipping.")
        return
    path = DATA_DIR / filename
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    print(f"  ✓  {filename:<30} {len(rows):>6,} rows → {path}")


# ── Main ─────────────────────────────────────────────────────────────────────
def main(n_clicks: int = 600) -> None:
    print(f"\nGenerating mock data ({n_clicks:,} clicks)...\n")

    campaigns      = make_campaigns()
    clicks         = make_clicks(campaigns, n_clicks)
    tracking_codes = make_tracking_codes(clicks, campaigns)
    page_events    = make_page_events(clicks)
    op_a           = make_operator_a(clicks, campaigns)
    op_b           = make_operator_b(clicks, campaigns)
    op_c           = make_operator_c(tracking_codes)

    write_csv(campaigns,      "campaigns.csv")
    write_csv(clicks,         "clicks.csv")
    write_csv(tracking_codes, "tracking_codes.csv")
    write_csv(page_events,    "page_events.csv")
    write_csv(op_a,           "operator_a.csv")
    write_csv(op_b,           "operator_b.csv")
    write_csv(op_c,           "operator_c.csv")

    print(f"\nAll files written to ./{DATA_DIR}/")
    print("Next step: python pipeline.py --date 2026-01-15\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate mock CSV data for AdStart pipeline")
    parser.add_argument("--rows", type=int, default=600,
                        help="Number of click rows to generate (default: 600). Use 6000 for full-scale.")
    args = parser.parse_args()
    main(n_clicks=args.rows)