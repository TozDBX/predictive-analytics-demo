"""Synthetic residents + SMS subscriptions for the air-quality alert demo.

Deterministic via fixed seed. Phone numbers use the UK Ofcom-reserved
"drama" range +44 7700 9000xx so they're guaranteed non-routable.

Outputs two CSVs to ../../data/ alongside the existing Intelligence Hub data:
- residents.csv
- subscriptions.csv
"""
import csv
import datetime as dt
import random
from pathlib import Path

random.seed(2026)

# Mirror the ward list from the parent demo so IDs line up exactly.
WARDS = [
    "TH01", "TH02", "TH03", "TH04", "TH05", "TH06", "TH07", "TH08", "TH09", "TH10",
    "TH11", "TH12", "TH13", "TH14", "TH15", "TH16", "TH17", "TH18", "TH19", "TH20",
]

LANGUAGES = ["en"] * 8 + ["bn"] * 2  # Bengali is the largest non-English first language in LBTH

OUT = Path(__file__).resolve().parents[2] / "data"
OUT.mkdir(parents=True, exist_ok=True)

residents_path = OUT / "residents.csv"
subscriptions_path = OUT / "subscriptions.csv"

# 2,000 synthetic residents distributed across wards proportional to a rough
# population skew (heavier in Bethnal Green, Whitechapel, Mile End).
WEIGHTS = {w: 1.0 for w in WARDS}
for w in ("TH01", "TH02", "TH03", "TH11"):
    WEIGHTS[w] = 1.6

residents = []
with residents_path.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["resident_id", "ward_id", "phone_e164", "preferred_language"])
    for i in range(2000):
        ward = random.choices(WARDS, weights=[WEIGHTS[w] for w in WARDS])[0]
        # Ofcom drama range: +44 7700 900000 to 900999 — safe for demos.
        phone = f"+447700900{random.randint(0, 999):03d}"
        lang = random.choice(LANGUAGES)
        rid = f"R{i:05d}"
        residents.append((rid, ward, phone, lang))
        writer.writerow((rid, ward, phone, lang))

# ~70% of residents opt in to alerts, with mixed sensitivity thresholds.
with subscriptions_path.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "subscription_id",
        "resident_id",
        "ward_id",
        "opt_in_status",
        "daqi_threshold",
        "opt_in_at",
    ])
    sub_idx = 0
    base_time = dt.datetime(2025, 11, 1, 9, 0)
    for rid, ward, _phone, _lang in residents:
        if random.random() > 0.70:
            continue
        sub_idx += 1
        # Most residents alert on Moderate (4); asthma/respiratory residents on Low+ (3); cautious on High (7).
        threshold = random.choices([3, 4, 5, 6, 7], weights=[0.10, 0.55, 0.15, 0.10, 0.10])[0]
        status = random.choices(["ACTIVE", "PAUSED", "REVOKED"], weights=[0.92, 0.05, 0.03])[0]
        opted_in_at = base_time + dt.timedelta(minutes=random.randint(0, 60 * 24 * 180))
        writer.writerow((
            f"S{sub_idx:05d}", rid, ward, status, threshold,
            opted_in_at.strftime("%Y-%m-%d %H:%M:%S"),
        ))

print(f"Wrote {residents_path}")
print(f"Wrote {subscriptions_path}")
