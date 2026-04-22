from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from datetime import datetime
import time
import json

# SETUP
options = webdriver.ChromeOptions()
options.add_argument("--headless=new")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()),
    options=options
)

driver.get("https://www.wired.com")

time.sleep(6)

# SCROLL UNTUK LOAD ARTIKEL
for _ in range(15):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

# AMBIL LINK ARTIKEL
links = driver.find_elements(By.TAG_NAME, "a")

urls = set()

for a in links:
    url = a.get_attribute("href")
    if url and "wired.com" in url and "/story/" in url:
        urls.add(url)

urls = list(urls)

print("Total kandidat URL:", len(urls))

# KEYWORDS FILTER
keywords = ["AI", "Climate", "Security"]
articles = []

# VISIT DETAIL PAGE
for url in urls:
    if len(articles) >= 100:
        break

    try:
        driver.get(url)
        time.sleep(3)

        title = driver.title


        try:
            description = driver.find_element(By.CSS_SELECTOR, "meta[name='description']").get_attribute("content")
        except:
            description = None
        try:
            author = driver.find_element(By.CSS_SELECTOR, "a[href*='author']").text
            if author == "":
                author = None
        except:
            author = None

        text_page = driver.page_source

        if not any(k.lower() in text_page.lower() for k in keywords):
            continue

        article = {
            "title": title,
            "url": url,
            "description": description,
            "author": author,
            "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

        articles.append(article)

        print(f"[{len(articles)}] OK:", title[:60])

    except Exception as e:
        continue

# SAVE JSON
with open("wired_articles.json", "w", encoding="utf-8") as f:
    json.dump(articles, f, indent=4, ensure_ascii=False)

driver.quit()

print("\nDONE TOTAL:", len(articles))