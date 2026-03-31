📌 Linux TMDB Movie Dataset CLI Project



Author: Quoc Bao

Purpose: Practice Linux command-line tools, CSV processing, text manipulation, and shell scripting using csvkit \& Python Data Engineering skills using TMDB movie dataset.



📂 Project Structure

Linux\_Project/

│

├── run\_all\_cli.sh              # Main Linux CLI automation script

├── analyze\_tmdb.py             # Python analysis script (full 7 tasks)

├── requirements.txt            # Python dependencies

├── tmdb.csv                    # Dataset (21 columns, ~10k movies)

│

├── tmdb\_outputs/               # Output from Linux CLI project

│   ├── sorted\_by\_release.csv

│   ├── high\_rating.csv

│   ├── max\_revenue.csv

│   ├── min\_revenue.csv

│   ├── total\_revenue.csv

│   ├── top10\_profit.csv

│   ├── director\_count.txt

│   ├── actor\_count.txt

│   └── genres\_count.txt

│

└── pythonoutput/               # Output from Python project

    ├── sorted\_by\_release.csv

    ├── high\_rating\_gt\_7.5.csv

    ├── top10\_by\_profit.csv

    ├── actor\_counts.csv

    ├── director\_counts.csv

    ├── genre\_counts.csv

    └── summary.txt

🚀 1. Project Overview



This project analyzes the TMDB movie dataset using two different approaches:



A) Linux CLI Project (csvkit + Shell Script)



✔ No Python

✔ 100% Linux command-line

✔ Uses:



csvcut, csvsort, csvsql, csvlook



awk, sed, sort, uniq, head



B) Python Project (Pandas + venv)



✔ Python 3.12

✔ Pandas + NumPy

✔ Outputs same 7 tasks as Linux version

✔ Everything saved to pythonoutput/





**A) Linux CLI Project (csvkit + Shell Script)**



👉 run\_all\_cli.sh



chmod +x run\_all\_cli.sh

./run\_all\_cli.sh



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

  | awk -v RS='|' '{ g=$0; gsub(/^\[ \\t]+|\[ \\t]+$/,"",g); if(g!="") print g }' \\

  | sort | uniq -c | sort -nr | head -n 50 > "$OUT\_DIR/actor\_count.txt"



echo "7) Genre counts -> $OUT\_DIR/genres\_count.txt"

echo "Thống kê số lượng phim theo các thể loại:" > "$OUT\_DIR/genres\_count.txt"

csvcut -c 14 "$IN" | tail -n +2 | sed '/^$/d' \\

  | tr '|' '\\n' \\

  | sed 's/^\[ \\t]\*//; s/\[ \\t]\*$//' \\

  | sed '/^$/d' \\

  | sort | uniq -c | sort -nr >> "$OUT\_DIR/genres\_count.txt"



echo "Finished. All outputs saved to: $OUT\_DIR/"d



**🐍 B. Python Project**



This is the Python version of the same 7 tasks.



Step 1 — Create \& Activate Virtual Environment

If python launcher not found, use full path:

\& "C:\\Users\\baoqu\\AppData\\Local\\Programs\\Python\\Python312\\python.exe" -m venv venv



Activate (PowerShell):

Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

.\\venv\\Scripts\\activate



Step 2 — Install dependencies

pip install -r requirements.txt



requirements.txt

pandas

numpy

matplotlib



pip freeze > requirements.txt



Step 3 — Run the Python script

python analyze\_tmdb.py





All outputs are saved to:



python\_tmdb\_outputs/



📊 5. Python Output Files



File	Description

sorted\_by\_release.csv	Sorted by release date

high\_rating\_gt\_7.5.csv	Vote > 7.5

top10\_by\_profit.csv	Top profit movies

director\_counts.csv	Director frequency

actor\_counts.csv	Actor frequency

genre\_counts.csv	Genre frequency

summary.txt	Overview of max/min revenue \& metadata



🧾 6. Python Script Included (analyze\_tmdb.py)



Full script already included in repo (latest version).



Handles:



Flexible column detection



Missing values



String list parsing (|, ,, JSON-like lists)



Automated folder creation



**🎯 Conclusion**



You now have 2 complete Data Engineering mini-projects:



✔ Linux version → Using CLI tools

✔ Python version → Using Pandas



Both accomplish the same 7 tasks, giving you:



Shell scripting skills



CSV manipulation skills



Python data analysis skills



Reproducible end-to-end pipeline

