import base64
import json
import time

from bs4 import BeautifulSoup as soup
from ftfy import fix_encoding
from kafka import KafkaProducer

from crawler.baodautu_crawler import BaoDauTuCrawler
from src.mongodb_exporter import MongoDB
from utils.logger_utils import get_logger

logger = get_logger('VnEconomy Crawler')


class VnEconomyCrawler(BaoDauTuCrawler):
    def __init__(self, url, tag, start_page, producer: KafkaProducer = None, mongodb: MongoDB = None):
        super().__init__(url, tag, start_page, producer, mongodb)
        self.name = "vneconomy"
        self.save_file = f"./data"

    @staticmethod
    def get_all_news_url(page_soup: soup):
        result = []
        h3_tags = page_soup.find_all("header", class_="story__header")
        if not h3_tags:
            return result
        for tag in h3_tags:
            a_tag = tag.find("a")
            result.append(f"https://vneconomy.vn{a_tag['href']}")
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
            title = page_soup.find("h1", class_="detail__title")
            attract = page_soup.find("h2", class_="detail__summary")
            author = page_soup.find("div", class_="detail__author")
            author = self.preprocess_data(author)
            if author: author = author.replace(" -", "")
            date = page_soup.find("div", class_="detail__meta")
            date = self.preprocess_data(date).split(" ")
            date = f"{date[1]} {date[0]}"
            main_content = page_soup.find("div", class_="detail__content")
            contents = main_content.find_all("p")
            news_contents = []
            for content in contents:
                news_contents.append(self.preprocess_data(content))
            article = page_soup.find("article")
            imgs = article.find_all("figure")
            news_imgs = self.get_images(imgs)
            tags = page_soup.find("div", class_="detail__tag")
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
            logger.warning(e)
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
            if img_url:
                img_url = img_url["src"]

            if not img_url or "https" not in img_url:
                continue
            if "http" not in img_url:
                img_url = f"{self.img_url_prefix}{img_url}"
            img_content = self.crawl_img(img_url)
            img_content = base64.b64encode(img_content).decode()
            img_name = img.find("figcaption")
            if img_name:
                img_name = self.preprocess_data(img_name)
            news_imgs.append({
                "url": img_url,
                "title": img_name,
                "content": img_content
            })
        return news_imgs

    def get_tags(self, tags):
        news_tags = []
        if not tags:
            return news_tags
        _tags = tags.find_all("a", class_="tag-item")
        if not _tags:
            return news_tags
        for tag in _tags:
            news_tags.append(self.preprocess_data(tag))

        return news_tags

    def export_data(self, limit=None):
        page = self.start_page

        while True:
            if limit and page==limit:
                break
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
                    data["url"] = news_url
                    self.write_data(data, file_name)
            page += 1

            logger.info(f"Crawl {len(news_urls)} in {round(time.time() - begin, 2)}s")
