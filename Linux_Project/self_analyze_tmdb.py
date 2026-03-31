import pandas as pd
from pathlib import Path


INPUT = "tmdb.csv"
out = Path("python_tmdb_outputs_by_me")
out.mkdir(exist_ok=True)

df = pd.read_csv(INPUT)
print(f"Loaded, {len(df)}, rows")

df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
df["budget"] = pd.to_numeric(df["budget"], errors="coerce")
df["vote_average"] = pd.to_numeric(df["vote_average"], errors="coerce")
df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")

# 1.Sắp xếp các bộ phim theo ngày phát hành giảm dần rồi lưu ra một file mới

sorted_df = df.sort_values(by="release_date", ascending=False)
sorted_df.to_csv(out / "1.sorted_by_release.csv", index=False)

# 2.Lọc ra các bộ phim có đánh giá trung bình trên 7.5 rồi lưu ra một file mới

high_rating = df[df["vote_average"] > 7.5]
high_rating.to_csv(out / "2.high_rating_gt_7.5.csv", index=False)

# 3.Tìm ra phim nào có doanh thu cao nhất và doanh thu thấp nhất

df_valid_revenue = df[df["revenue"] > 0]

max_movie = df_valid_revenue[df_valid_revenue["revenue"] == df_valid_revenue["revenue"].max()]
min_movie = df_valid_revenue[df_valid_revenue["revenue"] == df_valid_revenue["revenue"].min()]  

# 4.Tính tổng doanh thu tất cả các bộ phim

total_revenue = df_valid_revenue["revenue"].sum()

with open(out / "3_and_4.revenue_stats.txt", "w") as f:
    f.write("Movie with highest revenue:\n")
    f.write(max_movie.to_string(index=False))
    f.write("\n\n")
    f.write("Movie with lowest revenue:\n")
    f.write(min_movie.to_string(index=False))
    f.write("\n\n")
    f.write(f"Total revenue of all movies: {total_revenue}\n")

# 5.Top 10 bộ phim đem về lợi nhuận cao nhất

df["profit"] = df["revenue"] - df["budget"]
top_10_profit = df.sort_values(by="profit", ascending=False).head(10)
top_10_profit.to_csv(out / "5.top_10_highest_profit.csv", index=False)

# 6.Đạo diễn nào có nhiều bộ phim nhất và diễn viên nào đóng nhiều phim nhất

def split_list_column(col):
    names = []
    for a in col.dropna():
        a = str(a).replace("[", '"').replace("]", '"')
        a = a.replace("'", '"')
        if "|" in a:
            parts = a.split("|")
        else: 
            parts = a.split(",")
        parts = [p.strip().strip('"') for p in parts if p.strip()]
        names.extend(parts)
    return names

director = split_list_column(df["director"])
pd.Series(director).value_counts().to_csv(out / "6a.director_counts.csv")

actor = split_list_column(df["cast"])
pd.Series(actor).value_counts().to_csv(out / "6b.actor_counts.csv")

# 7.Thống kê số lượng phim theo các thể loại. 
# Ví dụ có bao nhiêu phim thuộc thể loại Action, bao nhiêu thuộc thể loại Family, ….

genres = split_list_column(df["genres"])
pd.Series(genres).value_counts().to_csv(out / "7.genre_counts.csv")

print("Analysis complete. Outputs saved to", out.resolve())