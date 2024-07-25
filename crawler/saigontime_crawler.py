import json
import time

from bs4 import BeautifulSoup as soup
from ftfy import fix_encoding
from kafka import KafkaProducer
from selenium.webdriver import Keys
from selenium.webdriver.common.by import By

from crawler.baodautu_crawler import BaoDauTuCrawler
from src.mongodb_exporter import MongoDB
from utils.logger_utils import get_logger

logger = get_logger('SaigonTime Crawler')


class SaigonTimeCrawler(BaoDauTuCrawler):
    def __init__(self, url, tag, start_page, producer: KafkaProducer = None, mongodb: MongoDB = None):
        super().__init__(url, tag, start_page, producer, mongodb)
        self.name = "saigontimes"
        self.save_file = f"../../data"

    @staticmethod
    def get_all_news_url(page_soup):
        result = []
        div_tags = page_soup.find_all("div", class_="td-module-meta-info")
        for tag in div_tags:
            a_tag = tag.find("h3").find("a")
            if a_tag.get("href"):
                result.append(a_tag["href"])
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
            title = page_soup.find("h1", class_="tdb-title-text")
            attract = page_soup.find("div", class_="td-post-content").find("h6")
            author = page_soup.find("a", class_="tdb-author-name")
            author = self.preprocess_data(author)
            date = page_soup.find("time")["datetime"]
            date = str(date).strip()

            main_content = page_soup.find("div", class_="td-post-content")
            contents = main_content.find_all("p")
            news_contents = []
            for content in contents:
                content = self.preprocess_data(content)
                if content:
                    news_contents.append(content)

            # imgs = main_content.find_all("figure")
            # news_imgs = self.get_images(imgs)
            tags = page_soup.find("ul", "tdb-tags")
            news_tags = self.get_tags(tags)
            result = {
                "journal": self.name,
                "type": self.tag,
                "title": self.preprocess_data(title),
                "attract": self.preprocess_data(attract),
                "author": author,
                "date": date,
                "content": news_contents,
                # "image": news_imgs,
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
        file_name = news_url.split("/")[-2]
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
            img_name = img.find("figcaption")

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
            tag = self.preprocess_data(tag)
            if tag:
                news_tags.append(tag)

        return news_tags

    @staticmethod
    def get_crawled_url():
        with open("../../data/crawled_url.json", "r") as f:
            data = json.loads(f.read())
        if not data:
            return []
        return data

    @staticmethod
    def write_crawled_url(data):
        with open("../../data/crawled_url.json", "w") as f:
            json.dump(data, f)

    def export_data(self, limit=None):
        page = self.start_page
        while True:
            if limit and page == limit:
                break
            begin = time.time()
            url = f"{self.url}/page/{page}/"
            news_urls = self.fetch_data(url, self.get_all_news_url)
            if not news_urls or len(news_urls) <= 1:
                break
            for news_url in news_urls:
                logger.info(f"Export page {page}: {news_url}")
                data = self.fetch_data(news_url, self.get_news_info)
                file_name = self.get_file_name(news_url)
                if data:
                    self.write_data(data, file_name)
            page += 1
            logger.info(f"Crawl {len(news_urls)} in {round(time.time() - begin, 2)}s")
            time.sleep(3)


if __name__ == "__main__":
    url = {
        'https://thesaigontimes.vn/tai-chinh-ngan-hang/ngan-hang/': "finance",
        'https://thesaigontimes.vn/tai-chinh-ngan-hang/bao-hiem/': 'finance',
        'https://thesaigontimes.vn/tai-chinh-ngan-hang/chung-khoan/': "stock-market",
        'https://thesaigontimes.vn/doanh-nhan-doanh-nghiep/guong-mat-khoi-nghiep/': "fintech",
        "https://thesaigontimes.vn/kinh-doanh/thuong-mai-dien-tu/": "fintech",
        "https://thesaigontimes.vn/the-gioi/": "market"
    }
