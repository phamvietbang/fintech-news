import base64
import json
import pickle
import time

from bs4 import BeautifulSoup as soup
from ftfy import fix_encoding
from kafka import KafkaProducer

from constants import ImageUrls
from crawler.base_crawler import BaseCrawler
from src.mongodb_exporter import MongoDB
from utils.logger_utils import get_logger

logger = get_logger('BaoDauTu Crawler')


class BaoDauTuCrawler(BaseCrawler):
    def __init__(self, url, tag, start_page, producer: KafkaProducer = None, mongodb: MongoDB = None):
        super().__init__()
        self.mongodb = mongodb
        self.start_page = start_page
        self.tag = tag
        self.url = url
        self.save_file = f"./data"
        self.producer = producer
        self.name = 'baodautu'
        self.img_url_prefix = ImageUrls.mapping.get(self.name)

    @staticmethod
    def get_all_news_url(page_soup):
        result = []
        div_tags = page_soup.find_all("div", class_="desc_list_news_home")
        if not div_tags:
            return result
        for tag in div_tags:
            tag_a = tag.find("a")
            result.append(tag_a["href"])
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
            title = page_soup.find("div", class_="title-detail")
            attract = page_soup.find("div", class_="sapo_detail")
            author = page_soup.find("a", class_="author")
            date = page_soup.find("span", class_="post-time")
            date = self.preprocess_data(date).replace("- ", "")
            main_content = page_soup.find("div", id="content_detail_news")
            contents = main_content.find_all("p")
            news_contents = []
            for content in contents:
                news_contents.append(self.preprocess_data(content))
            # imgs = main_content.find_all("tbody")
            # news_imgs = self.get_images(imgs)
            tags = page_soup.find("div", "tag_detail")
            news_tags = self.get_tags(tags)
            result = {
                "journal": self.name,
                "type": self.tag,
                "title": self.preprocess_data(title),
                "attract": self.preprocess_data(attract),
                "author": self.preprocess_data(author),
                "date": date,
                "content": news_contents,
                # "image": news_imgs,
                "tags": news_tags
            }
            return result
        except Exception as e:
            logger.warning(e)
            return None

    def get_images(self, imgs):
        news_imgs = []
        for img in imgs:
            img_info = img.find_all("td")
            if img_info:
                img_url = img_info[0].find("img")
                if not img_url:
                    continue
                img_url = img_url['src']
                if "http" not in img_url:
                    img_url = f"{self.img_url_prefix}{img_url}"
                img_content = self.crawl_img(img_url)
                img_content = base64.b64encode(img_content).decode()
                img_name = ""
                if len(img_info) > 1:
                    img_name = self.preprocess_data(img_info[1])
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
        _tags = tags.find_all("a", "tag_detail_item")
        if not _tags:
            return news_tags
        for tag in _tags:
            content_tag = self.preprocess_data(tag)
            if content_tag:
                news_tags.append(content_tag.replace("#", "").strip())

        return news_tags

    def write_to_file(self, data, file_name):
        with open(f"{self.save_file}/{file_name}.json", "w", encoding="utf-8") as f:
            json.dump(data, f, indent=1, ensure_ascii=False)

    def write_to_kafka(self, data, file_name):
        data["id"] = file_name
        self.producer.send(self.name, pickle.dumps(data))

    def write_to_mongodb(self, data, file_name):
        data["_id"] = file_name
        self.mongodb.update_docs(self.name, [data])

    def write_data(self, data, file_name):
        if self.producer:
            self.write_to_kafka(data, file_name)
        elif self.mongodb:
            self.write_to_mongodb(data, file_name)
        else:
            self.write_to_file(data, file_name)

    def get_file_name(self, news_url):
        file_name = news_url.split("/")[-1]
        file_name = file_name.split(".")[0]
        file_name = f"{self.name}_{file_name}"
        return file_name

    def export_data(self, limit=None):
        page = self.start_page

        while True:
            if limit and page == limit:
                break
            begin = time.time()
            url = f"{self.url}/p{page}"
            news_urls = self.fetch_data(url, self.get_all_news_url)
            if not news_urls or len(news_urls) <= 1:
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


if __name__ == "__main__":
    url = "https://baodautu.vn/ngan-hang-d5"
    tag = "finance"

    producer = KafkaProducer(bootstrap_servers=["localhost:29092"])
    print(f"config: {producer.config}")
    print(f"connect: {producer.bootstrap_connected()}")
    job = BaoDauTuCrawler(url, tag, 1, producer, use_kafka=True)
    job.export_data()