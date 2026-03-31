#!/usr/bin/env python3
"""
analyze_tmdb.py
Simple ETL/analysis for tmdb.csv to answer 7 questions:
1. Sort by release year desc -> outputs sorted_by_release.csv
2. Filter vote_average > 7.5 -> outputs high_rating_gt_7.5.csv
3. Movie with max/min revenue
4. Total revenue sum
5. Top 10 by profit (revenue - budget) -> top10_by_profit.csv
6. Director with most movies & Actor with most movies -> director_counts.csv, actor_counts.csv
7. Count movies by genre -> genre_counts.csv

Place tmdb.csv in same folder as this script. Outputs go to ./tmdb_outputs/
"""
import ast
from pathlib import Path
import pandas as pd
import numpy as np

IN = Path("tmdb.csv")
OUT = Path("python_tmdb_outputs")
OUT.mkdir(parents=True, exist_ok=True)

if not IN.exists():
    raise SystemExit(f"Input file not found: {IN.resolve()}")

df = pd.read_csv(IN)
print(f"Loaded {len(df)} rows, columns: {list(df.columns)}")

# Heuristics for common columns
title_col = next((c for c in df.columns if c.lower() in ("title","original_title","name")), df.columns[0])
release_candidates = [c for c in df.columns if "release" in c.lower() or "year" in c.lower()]
release_col = release_candidates[0] if release_candidates else None
revenue_col = next((c for c in df.columns if "revenue" in c.lower()), None)
budget_col = next((c for c in df.columns if "budget" in c.lower()), None)
vote_col = next((c for c in df.columns if "vote" in c.lower() or "rating" in c.lower()), None)
cast_col = next((c for c in df.columns if "cast" == c.lower() or "actors" in c.lower() or "cast" in c.lower()), None)
director_col = next((c for c in df.columns if "director" in c.lower()), None)
genre_col = next((c for c in df.columns if "genre" in c.lower()), None)

# Normalize numeric columns
for c in (revenue_col, budget_col, vote_col):
    if c and c in df.columns:
        df[c] = pd.to_numeric(df[c], errors="coerce")

# TASK 1: sort by release (if year or date exists)
if release_col and release_col in df.columns:
    # if release contains full date, try parse; otherwise sort by numeric year
    try:
        df["_release_parsed"] = pd.to_datetime(df[release_col], errors="coerce")
        sorted_df = df.sort_values(by="_release_parsed", ascending=False)
        sorted_df.drop(columns=["_release_parsed"], inplace=True)
    except Exception:
        sorted_df = df.sort_values(by=release_col, ascending=False, na_position="last")
else:
    sorted_df = df.copy()
sorted_df.to_csv(OUT / "sorted_by_release.csv", index=False)

# TASK 2: filter rating > 7.5
if vote_col:
    high_rating = df[df[vote_col] > 7.5].copy()
else:
    high_rating = pd.DataFrame()
high_rating.to_csv(OUT / "high_rating_gt_7.5.csv", index=False)

# TASK 3 & 4: max/min revenue and total
max_rev_row = None
min_rev_row = None
total_revenue = None
if revenue_col:
    df_valid_revenue = df[df[revenue_col] > 0].copy()

    if not df_valid_revenue.empty:

        max_val = df_valid_revenue[revenue_col].max(skipna=True)
        max_rev_row = df_valid_revenue[df_valid_revenue[revenue_col] == max_val].head(1)

        min_val = df_valid_revenue[revenue_col].min(skipna=True)
        min_rev_row = df_valid_revenue[df_valid_revenue[revenue_col] == min_val].head(1)

    total_revenue = df[revenue_col].sum(skipna=True)

# TASK 5: top 10 by profit
if revenue_col and budget_col:
    df["profit"] = df[revenue_col].fillna(0) - df[budget_col].fillna(0)
    top10 = df.sort_values(by="profit", ascending=False).head(10)
    top10.to_csv(OUT / "top10_by_profit.csv", index=False)
elif revenue_col:
    top10 = df.sort_values(by=revenue_col, ascending=False).head(10)
    top10.to_csv(OUT / "top10_by_revenue.csv", index=False)

# Helpers for parsing list-like strings
def explode_names(series):
    names = []
    for v in series.dropna():
        if isinstance(v, list):
            names.extend([str(x).strip() for x in v if x])
        elif isinstance(v, str):
            s = v.strip()
            # try JSON list
            try:
                parsed = ast.literal_eval(s)
                if isinstance(parsed, list):
                    # items may be dicts or strings
                    for it in parsed:
                        if isinstance(it, dict):
                            nm = it.get("name") or it.get("title") or None
                            if nm: names.append(str(nm).strip())
                        else:
                            names.append(str(it).strip())
                    continue
            except Exception:
                pass
            # fallback: separators
            if "|" in s:
                parts = s.split("|")
                names.extend([p.strip() for p in parts if p.strip()])
            elif "," in s and not s.startswith("{"):
                parts = s.split(",")
                names.extend([p.strip() for p in parts if p.strip()])
            else:
                names.append(s)
        else:
            names.append(str(v))
    return names

# TASK 6: director & actor counts
if director_col and director_col in df.columns:
    directors = explode_names(df[director_col])
    pd.Series(directors).value_counts().to_csv(OUT / "director_counts.csv", header=["count"])
else:
    # try to infer from 'crew' column if present (not implemented heavily)
    pass

if cast_col and cast_col in df.columns:
    actors = explode_names(df[cast_col])
    pd.Series(actors).value_counts().to_csv(OUT / "actor_counts.csv", header=["count"])

# TASK 7: genres count
if genre_col and genre_col in df.columns:
    genres = explode_names(df[genre_col])
    pd.Series(genres).value_counts().to_csv(OUT / "genre_counts.csv", header=["count"])

# Write small summary file
lines = []
lines.append(f"rows={len(df)}, cols={len(df.columns)}")
lines.append(f"title_col={title_col}")
lines.append(f"release_col={release_col}")
lines.append(f"revenue_col={revenue_col}, budget_col={budget_col}, vote_col={vote_col}")
if max_rev_row is not None and not max_rev_row.empty:
    lines.append("max_revenue_movie=" + str(max_rev_row[title_col].values[0]) + f" -> {max_val}")
if min_rev_row is not None and not min_rev_row.empty:
    lines.append("min_revenue_movie=" + str(min_rev_row[title_col].values[0]) + f" -> {min_val}")
if total_revenue is not None:
    lines.append(f"total_revenue={total_revenue}")
with open(OUT / "summary.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(lines))

print("Analysis done. Outputs in:", OUT.resolve())
