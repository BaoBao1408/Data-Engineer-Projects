#!/usr/bin/env bash
# run_all_cli.sh - Safe, robust TMDB CSV analysis for WSL/Ubuntu
# Produces files in tmdb_outputs/ and ensures CSVs are UTF-8 with BOM for Excel.

set -euo pipefail
IFS=$'\n\t'

IN="tmdb.csv"
OUT_DIR="tmdb_outputs"
BACKUP_DIR="tmdb_outputs_backup_$(date +%Y%m%d_%H%M%S)"

# Ensure csvkit available
command -v csvcut >/dev/null 2>&1 || { echo "csvcut not found. Install csvkit (pip3 install --user csvkit) and re-run."; exit 1; }
command -v csvsql >/dev/null 2>&1 || { echo "csvsql not found. Install csvkit (pip3 install --user csvkit) and re-run."; exit 1; }

# backup previous outputs if exist
if [ -d "$OUT_DIR" ]; then
  echo "Backing up existing $OUT_DIR -> $BACKUP_DIR"
  cp -r "$OUT_DIR" "$BACKUP_DIR"
  rm -rf "$OUT_DIR"
fi

mkdir -p "$OUT_DIR"

# helper: safe-run a command that may fail without aborting script
safe_run() {
  set +e
  "$@"
  rc=$?
  set -e
  return $rc
}

echo "1) Sort by release_date (descending) -> $OUT_DIR/sorted_by_release.csv"
if csvcut -n "$IN" | grep -qi "release_date"; then
  safe_run csvsort -c release_date -r "$IN" > "$OUT_DIR/sorted_by_release.csv" || cp "$IN" "$OUT_DIR/sorted_by_release.csv"
elif csvcut -n "$IN" | grep -qi "release_year"; then
  safe_run csvsort -c release_year -r "$IN" > "$OUT_DIR/sorted_by_release.csv" || cp "$IN" "$OUT_DIR/sorted_by_release.csv"
else
  cp "$IN" "$OUT_DIR/sorted_by_release.csv"
fi

echo "2) Filter vote_average > 7.5 -> $OUT_DIR/high_rating_gt_7.5.csv"
# allow this query to fail gracefully
safe_run csvsql --query "select * from tmdb where CAST(vote_average AS REAL) > 7.5" "$IN" > "$OUT_DIR/high_rating_gt_7.5.csv"
if [ ! -s "$OUT_DIR/high_rating_gt_7.5.csv" ]; then
  # create header fallback (try to get header from file)
  head -n1 "$IN" > "$OUT_DIR/high_rating_gt_7.5.csv"
fi

echo "3a) Max revenue -> $OUT_DIR/max_revenue.csv"
safe_run csvsql --query "select original_title, revenue from tmdb where CAST(revenue AS REAL) IS NOT NULL order by CAST(revenue AS REAL) desc limit 1" "$IN" > "$OUT_DIR/max_revenue.csv" || true
echo "->"
safe_run cat "$OUT_DIR/max_revenue.csv" || true

echo "3b) Min revenue (ignore revenue = 0) -> $OUT_DIR/min_revenue.csv"
safe_run csvsql --query "select original_title, revenue from tmdb where CAST(revenue AS REAL) > 0 order by CAST(revenue AS REAL) asc limit 1" "$IN" > "$OUT_DIR/min_revenue.csv" || true
echo "->"
safe_run cat "$OUT_DIR/min_revenue.csv" || true

echo "4) Total revenue -> $OUT_DIR/total_revenue.csv"
safe_run csvsql --query "select sum(CAST(revenue AS REAL)) as total_revenue from tmdb" "$IN" > "$OUT_DIR/total_revenue.csv" || true
echo "->"
safe_run cat "$OUT_DIR/total_revenue.csv" || true

echo "5) Top 10 profit -> $OUT_DIR/top10_profit.csv"
safe_run csvsql --query "select original_title, (CAST(revenue AS REAL) - CAST(budget AS REAL)) as profit from tmdb order by profit desc limit 10" "$IN" > "$OUT_DIR/top10_profit.csv" || true
# show nicely if csvlook available
safe_run csvlook "$OUT_DIR/top10_profit.csv" | sed -n '1,12p' 2>/dev/null || head -n 12 "$OUT_DIR/top10_profit.csv" 2>/dev/null || true

# -------------------------
# 6a: Director counts (robust)
# -------------------------
echo "6a) Top directors -> $OUT_DIR/director_count.csv"
if csvcut -n "$IN" | grep -qi "director"; then
  # Extract director column, trim whitespace, drop empties, count
  csvcut -c director "$IN" \
    | tail -n +2 \
    | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' \
    | sed '/^$/d' \
    | awk 'NF' \
    | sort | uniq -c | sort -nr > "$OUT_DIR/director_count.csv" \
    || echo "error extracting director" > "$OUT_DIR/director_count.csv"
else
  echo "NO DIRECTOR COLUMN FOUND" > "$OUT_DIR/director_count.csv"
fi
echo "->"
sed -n '1,20p' "$OUT_DIR/director_count.csv" 2>/dev/null || true

# -------------------------
# 6b: Actor counts (robust)
# -------------------------
echo "6b) Top actors -> $OUT_DIR/actor_count.csv"
if csvcut -n "$IN" | grep -qi "cast"; then
  csvcut -c cast "$IN" \
    | tail -n +2 \
    | sed '/^$/d' \
    | tr '|' '\n' \
    | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' \
    | sed '/^$/d' \
    | awk 'NF' \
    | sort | uniq -c | sort -nr > "$OUT_DIR/actor_count.csv" \
    || echo "error extracting cast" > "$OUT_DIR/actor_count.csv"
else
  echo "NO CAST COLUMN FOUND" > "$OUT_DIR/actor_count.csv"
fi
echo "->"
sed -n '1,20p' "$OUT_DIR/actor_count.csv" 2>/dev/null || true

# -------------------------
# 7: Genres counts (robust, drop empty)
# -------------------------
echo "7) Genre counts -> $OUT_DIR/genres_count.csv"
if csvcut -n "$IN" | grep -qi "genre"; then
  echo "Thống kê số lượng phim theo các thể loại:" > "$OUT_DIR/genres_count.csv"
  csvcut -c genres "$IN" \
    | tail -n +2 \
    | sed '/^$/d' \
    | tr '|' '\n' \
    | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' \
    | sed '/^$/d' \
    | grep -v '^"$' \
    | awk 'NF' \
    | sort | uniq -c | sort -nr >> "$OUT_DIR/genres_count.csv"
else
  echo "NO GENRES COLUMN FOUND" > "$OUT_DIR/genres_count.csv"
fi
echo "->"
sed -n '1,80p' "$OUT_DIR/genres_count.csv" 2>/dev/null || true

# -------------------------
# 8) Convert all .csv outputs to UTF-8 with BOM for Excel
# -------------------------
echo "8) Convert outputs to UTF-8 with BOM for Excel (overwrite files)"
# create backup (already backed up at top)
for f in "$OUT_DIR"/*.csv; do
  # skip if no csv found
  [ -f "$f" ] || continue
  # write BOM + original content safely to temp file then mv
  ( printf '\xEF\xBB\xBF'; cat "$f" ) > "$f.bom" && mv "$f.bom" "$f"
done

echo "Finished. All outputs saved to: $OUT_DIR/"
