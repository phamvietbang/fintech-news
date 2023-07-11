import json
import time
from bs4 import BeautifulSoup as soup
from crawler.base_crawler import BaseCrawler
from ftfy import fix_encoding
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By

from utils.logger_utils import get_logger

logger = get_logger('VnEconomy Crawler')


class VnEconomyCrawler(BaseCrawler):
    def __init__(self, url, tag, start_page):
        super().__init__()
        self.start_page = start_page
        self.tag = tag
        self.url = url
        self.save_file = f".data/vneconomy"

    @staticmethod
    def get_all_news_url(page_soup: soup):
        result = []
        section = page_soup.find("section", "zone zone--featured")
        a_tags = section.find_all("a")
        for tag in a_tags:
            result.append(f"https://vneconomy.vn{tag['href']}")
        return result

    @staticmethod
    def preprocess_data(data):
        if data:
            text = data.text
        else:
            return None
        return fix_encoding(text).strip()

    def get_news_info(self, page_soup: soup):
        title = page_soup.find("h1", class_="detail__title")
        attract = page_soup.find("h2", "detail__summary")
        author = page_soup.find("div", class_="detail__author")
        date = page_soup.find("div", "detail__meta")
        contents = page_soup.find("p")
        news_contents = []
        for content in contents:
            news_contents.append(self.preprocess_data(content))
        news_imgs = []
        article = page_soup.find("article")
        for img in article.find_all("figure"):
            img_url = img.find("img")
            if img_url:
                img_url = img_url["src"]
            img_name = img.find("figcaption")
            if img_name:
                img_name = self.preprocess_data(img_name)
            news_imgs.append({
                "url": img_url,
                "title": img_name
            })
        result = {
            "tag": self.tag,
            "title": self.preprocess_data(title),
            "attract": self.preprocess_data(attract),
            "author": self.preprocess_data(author),
            "date": self.preprocess_data(date),
            "content": contents,
            "image": news_imgs
        }
        return result

    def write_to_file(self, data, file_name):
        with open(f"{self.save_file}/{file_name}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1, ensure_ascii=False)

    @staticmethod
    def get_file_name(news_url):
        file_name = news_url.split("/")[-1]
        file_name = file_name.split(".")[0]
        return file_name

    def export_data(self):
        page = self.start_page

        while True:
            try:
                begin = time.time()
                url = f"{self.url}?trang={page}"
                news_urls = self.fetch_data(url, self.get_all_news_url)
                if not news_urls:
                    break
                for news_url in news_urls:
                    logger.info(f"Export page {page}: {news_url}")
                    data = self.fetch_data(news_url, self.get_news_info)
                    file_name = self.get_file_name(news_url)
                    if data:
                        self.write_to_file(data, file_name)
                page += 1
                logger.info(f"Crawl {len(news_urls)} in {round(time.time() - begin, 2)}s")
            except Exception as e:
                logger.warning(e)
                pass


if __name__ == "__main__":
    job = VnEconomyCrawler(url="https://vneconomy.vn/tai-chinh-ngan-hang.htm", tag="ngan hang", start_page=1)
    job.export_data()
