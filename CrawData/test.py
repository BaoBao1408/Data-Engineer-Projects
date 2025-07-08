from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import csv
import random

# Thiết lập trình duyệt
driver = webdriver.Chrome()
driver.maximize_window()

# ==== BƯỚC 1: Đăng nhập vào LinkedIn ====
driver.get("https://www.linkedin.com/login")
time.sleep(2)

# TODO: Thay bằng tài khoản phụ để tránh khoá account chính
EMAIL = "your_email@example.com"
PASSWORD = "your_password"

driver.find_element(By.ID, "username").send_keys(EMAIL)
driver.find_element(By.ID, "password").send_keys(PASSWORD)
driver.find_element(By.XPATH, "//button[@type='submit']").click()
time.sleep(5)

# ==== BƯỚC 2: Truy cập trang Job Search ====
url = "https://www.linkedin.com/jobs/search/?keywords=data%20engineer&location=Vietnam"
driver.get(url)
time.sleep(5)

# ==== BƯỚC 3: Scroll để tải toàn bộ job ====
last_height = driver.execute_script("return document.body.scrollHeight")
for _ in range(25):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(random.uniform(2, 4))  # tránh bị nghi crawl
    new_height = driver.execute_script("return document.body.scrollHeight")
    if new_height == last_height:
        break
    last_height = new_height

# ==== BƯỚC 4: Lấy danh sách job ====
job_titles = driver.find_elements(By.CLASS_NAME, "base-search-card__title")
companies = driver.find_elements(By.CLASS_NAME, "base-search-card__subtitle")
locations = driver.find_elements(By.CLASS_NAME, "job-search-card__location")

# ==== BƯỚC 5: Ghi vào CSV ====
with open("linkedin_jobs.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow(["Job Title", "Company", "Location"])

    for i in range(max(len(job_titles), len(companies), len(locations))):
        title = job_titles[i].text.strip() if i < len(job_titles) else "N/A"
        company = companies[i].text.strip() if i < len(companies) else "N/A"
        location = locations[i].text.strip() if i < len(locations) else "N/A"
        writer.writerow([title, company, location])

print(f"✅ Crawled {len(job_titles)} jobs và lưu vào 'linkedin_jobs.csv'")
driver.quit()
