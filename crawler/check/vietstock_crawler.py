import json
import time

from bs4 import BeautifulSoup as soup
from ftfy import fix_encoding
from kafka import KafkaProducer
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By

from crawler.baodautu_crawler import BaoDauTuCrawler
from utils.logger_utils import get_logger

logger = get_logger('VietStock Crawler')


class CafeBizCrawler(BaoDauTuCrawler):
    def __init__(self, url, tag, start_page, producer: KafkaProducer = None, use_kafka=False):
        super().__init__(url, tag, start_page, producer, use_kafka)
        self.name = "saigontime"
        self.save_file = f"../../.data"

    @staticmethod
    def get_all_news_url(driver):
        result = []
        h4_tags = driver.find_elements(By.TAG_NAME, "h4")
        for tag in h4_tags:
            a_tag = tag.find_element(By.TAG_NAME, "a")
            href = a_tag.get_attribute('href')
            result.append(f"https://vietstock.vn{href}")
        return result

    @staticmethod
    def preprocess_data(data):
        if data:
            text = data.text
        else:
            return None
        return fix_encoding(text).strip()

    def get_news_info(self, page_soup: soup):
        try:
            title = page_soup.find("h1", class_="article-title")
            attract = page_soup.find("p", class_="pHead")
            author = page_soup.find("p", class_="pAuthor")
            author = self.preprocess_data(author)
            date = page_soup.find("span", class_="date")
            date = self.preprocess_data(date)

            main_content = page_soup.find("div", id="vst_detail")
            contents = main_content.find_all("p")
            news_contents = []
            for content in contents:
                news_contents.append(self.preprocess_data(content))

            imgs = main_content.find_all("tbody")
            news_imgs = self.get_images(imgs)
            tags = page_soup.find("div", "tags")
            news_tags = self.get_tags(tags)
            result = {
                "journal": self.name,
                "type": self.tag,
                "title": self.preprocess_data(title),
                "attract": self.preprocess_data(attract),
                "author": author,
                "date": date,
                "content": news_contents,
                "image": news_imgs,
                "tags": news_tags
            }
            return result
        except Exception as e:
            logger.error(e)
            return None

    def write_to_file(self, data, file_name):
        with open(f"{self.save_file}/{file_name}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1, ensure_ascii=False)

    def get_file_name(self, news_url):
        file_name = news_url.split("/")[-1]
        file_name = file_name.split(".")[0]
        file_name = f"{self.name}_{file_name}"
        return file_name

    def get_images(self, imgs):
        news_imgs = []
        for img in imgs:
            img_url = img.find("img")
            if not img_url:
                continue
            img_url = img_url["src"]
            img_name = img.find("div", class_="pCaption")

            if img_name:
                img_name = self.preprocess_data(img_name)
            else:
                img_name = ""
            news_imgs.append({
                "url": img_url,
                "title": img_name
            })
        return news_imgs

    def get_tags(self, tags):
        news_tags = []
        _tags = tags.find_all("a")
        if not _tags:
            return news_tags
        for tag in _tags:
            news_tags.append(self.preprocess_data(tag))

        return news_tags

    @staticmethod
    def get_crawled_url():
        with open("../../.data/crawled_url.json", "r") as f:
            data = json.loads(f.read())
        if not data:
            return []
        return data

    @staticmethod
    def write_crawled_url(data):
        with open("../../.data/crawled_url.json", "w") as f:
            json.dump(data, f)

    def export_data(self):
        page = self.start_page
        driver = self.get_driver()
        crawled_url = self.get_crawled_url()
        try:
            while True:
                begin = time.time()
                old_length = len(crawled_url)
                news_urls = self.use_chrome_driver(driver, self.url, self.get_all_news_url)
                if not news_urls:
                    break
                for news_url in news_urls:
                    if news_url in crawled_url:
                        continue
                    else:
                        crawled_url.append(news_url)
                    logger.info(f"Export page {page}: {news_url}")
                    data = self.fetch_data(news_url, self.get_news_info)
                    file_name = self.get_file_name(news_url)
                    if data:
                        if not self.use_kafka:
                            self.write_to_file(data, file_name)
                        else:
                            self.write_to_kafka(data, file_name)
                page += 1
                logger.info(f"Crawl {len(news_urls)} in {round(time.time() - begin, 2)}s")
                if old_length == len(crawled_url):
                    break
                li_tags = driver.find_element(By.TAG_NAME, 'li')
                li_tags[-1].click()
                time.sleep(3)
            self.write_crawled_url(crawled_url)
        except Exception as e:
            logger.error(e)
        finally:
            driver.quit()


if __name__ == "__main__":
    url = {
        'https://vietstock.vn/tai-chinh.htm': "finance",
        'https://vietstock.vn/the-gioi/tai-chinh-quoc-te.htm': 'finance',
        'https://vietstock.vn/the-gioi/chung-khoan-the-gioi.htm': "stock-market",
        'https://vietstock.vn/the-gioi/kinh-te-nganh.htm': "market",
        "https://vietstock.vn/the-gioi/tien-ky-thuat-so.htm": "fintech",
        "https://vietstock.vn/kinh-te.htm": "market",
        "https://vietstock.vn/chung-khoan.htm": "stock-market"
    }
    for key, value in url.items():
        job = CafeBizCrawler(url=key, tag=value, start_page=1)
        job.export_data()
