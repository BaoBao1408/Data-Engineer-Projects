#!/usr/bin/env bash
set -euo pipefail

IN=tmdb.csv
OUT_DIR=tmdb_outputs
mkdir -p "$OUT_DIR"

echo "1) Sort by release_date (giảm dần) -> $OUT_DIR/sorted_by_release.csv"
csvsort -c release_date -r "$IN" > "$OUT_DIR/sorted_by_release.csv"

echo "2) Filter vote_average > 7.5 -> $OUT_DIR/high_rating.csv"
csvsql --query "select * from tmdb where CAST(vote_average AS REAL) > 7.5" "$IN" > "$OUT_DIR/high_rating.csv"

echo "3a) Max revenue -> $OUT_DIR/max_revenue.csv"
csvsql --query "select original_title, revenue from tmdb order by CAST(revenue AS REAL) desc limit 1" "$IN" > "$OUT_DIR/max_revenue.csv"
echo "->"
cat "$OUT_DIR/max_revenue.csv"

echo "3b) Min revenue -> $OUT_DIR/min_revenue.csv"
csvsql --query "select original_title, revenue from tmdb order by CAST(revenue AS REAL) asc limit 1" "$IN" > "$OUT_DIR/min_revenue.csv"
echo "->"
cat "$OUT_DIR/min_revenue.csv"

echo "4) Total revenue -> $OUT_DIR/total_revenue.csv"
csvsql --query "select sum(CAST(revenue AS REAL)) as total_revenue from tmdb" "$IN" > "$OUT_DIR/total_revenue.csv"
echo "->"
cat "$OUT_DIR/total_revenue.csv"

echo "5) Top 10 profit -> $OUT_DIR/top10_profit.csv"
csvsql --query "select original_title, (CAST(revenue AS REAL) - CAST(budget AS REAL)) as profit from tmdb order by profit desc limit 10" "$IN" > "$OUT_DIR/top10_profit.csv"
csvlook "$OUT_DIR/top10_profit.csv" | sed -n '1,12p'

echo "6a) Top directors -> $OUT_DIR/director_count.txt"
csvcut -c 9 "$IN" | tail -n +2 | sed '/^$/d' | sort | uniq -c | sort -nr | head -n 50 > "$OUT_DIR/director_count.txt"
echo "->"
sed -n '1,20p' "$OUT_DIR/director_count.txt"

echo "6b) Top actors -> $OUT_DIR/actor_count.txt"
csvcut -c 7 "$IN" | tail -n +2 | sed '/^$/d' \
  | awk -v RS='|' '{ g=$0; gsub(/^[ \t]+|[ \t]+$/,"",g); if(g!="") print g }' \
  | sort | uniq -c | sort -nr | head -n 50 > "$OUT_DIR/actor_count.txt"
echo "->"
sed -n '1,20p' "$OUT_DIR/actor_count.txt"

echo "7) Genre counts -> $OUT_DIR/genres_count.txt"
echo "Thống kê số lượng phim theo các thể loại:" > "$OUT_DIR/genres_count.txt"
csvcut -c 14 "$IN" | tail -n +2 | sed '/^$/d' \
  | tr '|' '\n' \
  | sed 's/^[ \t]*//; s/[ \t]*$//' \
  | sed '/^$/d' \
  | sort | uniq -c | sort -nr >> "$OUT_DIR/genres_count.txt"
echo "->"
cat "$OUT_DIR/genres_count.txt"

echo "Finished. All outputs saved to: $OUT_DIR/"

