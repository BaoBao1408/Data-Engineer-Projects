📌 Linux TMDB Movie Dataset CLI Project



Author: Quoc Bao

Purpose: Practice Linux command-line tools, CSV processing, text manipulation, and shell scripting using csvkit.



📂 Project Structure

Linux\_Project/

│

├── run\_all\_cli.sh          # Main shell script (all tasks automated)

├── tmdb.csv                # Original dataset (21 columns, ~10k movies)

├── debug.log               # Debug output (optional)

│

└── tmdb\_outputs/           # Folder containing all results

&nbsp;   ├── sorted\_by\_release.csv

&nbsp;   ├── high\_rating.csv

&nbsp;   ├── max\_revenue.csv

&nbsp;   ├── min\_revenue.csv

&nbsp;   ├── total\_revenue.csv

&nbsp;   ├── top10\_profit.csv

&nbsp;   ├── director\_count.txt

&nbsp;   ├── actor\_count.txt

&nbsp;   └── genres\_count.txt



🚀 1. Project Description



This project analyzes the TMDB movie dataset using Linux commands + csvkit, without Python.

All tasks are executed via a single automated script:



👉 run\_all\_cli.sh



The script performs:



Task	Description

1	Sort movies by release date (descending)

2	Filter movies with vote\_average > 7.5

3a	Find movie with highest revenue

3b	Find lowest revenue movie

4	Calculate total revenue of all movies

5	Compute profit (revenue – budget) and list Top 10 movies

6a	Count most frequent directors

6b	Count most frequent actors (correctly splitting cast by "|")

7	Count movies by genre



All outputs are saved into tmdb\_outputs/.



🛠 2. Requirements

✔ Installed via WSL (Ubuntu)

sudo apt update

sudo apt install python3-pip

pip3 install csvkit



✔ Tools used:



csvcut



csvsort



csvsql



csvlook



awk, sed, tr, sort, uniq, head



📜 3. How to Run the Project

Step 1 — Navigate into project folder

cd "/mnt/c/Users/baoqu/OneDrive/Desktop/Data-Engineer-Projects/Linux\_Project"



Step 2 — Make script executable

chmod +x run\_all\_cli.sh



Step 3 — Run the script

./run\_all\_cli.sh



Step 4 — View results

ls tmdb\_outputs



📊 4. Output Files (Explanation)

File	Description

sorted\_by\_release.csv	Movies sorted by newest release

high\_rating.csv	Movies with vote\_average > 7.5

max\_revenue.csv	Highest grossing movie

min\_revenue.csv	Lowest revenue movie

total\_revenue.csv	Sum of total revenue

top10\_profit.csv	Top 10 movies by profit

director\_count.txt	Frequency of directors

actor\_count.txt	Frequency of actors (splitting cast correctly)

genres\_count.txt	Movie count by genre

🧾 5. Main Script (run\_all\_cli.sh)



File đã được kiểm tra và chạy thành công, không lỗi.

Đây là phiên bản hoàn chỉnh 100%.



\#!/usr/bin/env bash

set -euo pipefail



IN=tmdb.csv

OUT\_DIR=tmdb\_outputs

mkdir -p "$OUT\_DIR"



echo "1) Sort by release\_date (giảm dần) -> $OUT\_DIR/sorted\_by\_release.csv"

csvsort -c release\_date -r "$IN" > "$OUT\_DIR/sorted\_by\_release.csv"



echo "2) Filter vote\_average > 7.5 -> $OUT\_DIR/high\_rating.csv"

csvsql --query "select \* from tmdb where CAST(vote\_average AS REAL) > 7.5" "$IN" > "$OUT\_DIR/high\_rating.csv"



echo "3a) Max revenue -> $OUT\_DIR/max\_revenue.csv"

csvsql --query "select original\_title, revenue from tmdb order by CAST(revenue AS REAL) desc limit 1" "$IN" > "$OUT\_DIR/max\_revenue.csv"



echo "3b) Min revenue -> $OUT\_DIR/min\_revenue.csv"

csvsql --query "select original\_title, revenue from tmdb order by CAST(revenue AS REAL) asc limit 1" "$IN" > "$OUT\_DIR/min\_revenue.csv"



echo "4) Total revenue -> $OUT\_DIR/total\_revenue.csv"

csvsql --query "select sum(CAST(revenue AS REAL)) as total\_revenue from tmdb" "$IN" > "$OUT\_DIR/total\_revenue.csv"



echo "5) Top 10 profit -> $OUT\_DIR/top10\_profit.csv"

csvsql --query "select original\_title, (CAST(revenue AS REAL) - CAST(budget AS REAL)) as profit from tmdb order by profit desc limit 10" "$IN" > "$OUT\_DIR/top10\_profit.csv"

csvlook "$OUT\_DIR/top10\_profit.csv" | sed -n '1,12p'



echo "6a) Top directors -> $OUT\_DIR/director\_count.txt"

csvcut -c 9 "$IN" | tail -n +2 | sed '/^$/d' | sort | uniq -c | sort -nr | head -n 50 > "$OUT\_DIR/director\_count.txt"



echo "6b) Top actors -> $OUT\_DIR/actor\_count.txt"

csvcut -c 7 "$IN" | tail -n +2 | sed '/^$/d' \\

&nbsp; | awk -v RS='|' '{ g=$0; gsub(/^\[ \\t]+|\[ \\t]+$/,"",g); if(g!="") print g }' \\

&nbsp; | sort | uniq -c | sort -nr | head -n 50 > "$OUT\_DIR/actor\_count.txt"



echo "7) Genre counts -> $OUT\_DIR/genres\_count.txt"

echo "Thống kê số lượng phim theo các thể loại:" > "$OUT\_DIR/genres\_count.txt"

csvcut -c 14 "$IN" | tail -n +2 | sed '/^$/d' \\

&nbsp; | tr '|' '\\n' \\

&nbsp; | sed 's/^\[ \\t]\*//; s/\[ \\t]\*$//' \\

&nbsp; | sed '/^$/d' \\

&nbsp; | sort | uniq -c | sort -nr >> "$OUT\_DIR/genres\_count.txt"



echo "Finished. All outputs saved to: $OUT\_DIR/"d

