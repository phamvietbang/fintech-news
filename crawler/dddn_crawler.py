import json
import time

from bs4 import BeautifulSoup as soup
from ftfy import fix_encoding
from kafka import KafkaProducer

from crawler.baodautu_crawler import BaoDauTuCrawler
from utils.logger_utils import get_logger

logger = get_logger('DDDN Crawler')


class DDDNCrawler(BaoDauTuCrawler):
    def __init__(self, url, tag, start_page, producer: KafkaProducer = None, use_kafka=False):
        super().__init__(url, tag, start_page, producer, use_kafka)
        self.name = "diendandoanhnghiep"
        self.save_file = f"../.data"

    @staticmethod
    def get_all_news_url(page_soup: soup):
        result = []
        div_tag = page_soup.find("div", "danh-sach-bai-viet")
        if not div_tag:
            return result
        h2_tags = div_tag.find_all("h2", class_="post-title")
        for tag in h2_tags:
            a_tag = tag.find("a")
            result.append(a_tag['href'])
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
            title = page_soup.find("h1", class_="post-title")
            attract = page_soup.find("h2", class_="post-sapo")
            author = page_soup.find("div", class_="post-author-share")
            author = self.preprocess_data(author)
            if author:
                author = author.split("|")[0].strip()
            date = page_soup.find("span", "created_time")
            date = self.preprocess_data(date).replace(",", "")

            main_content = page_soup.find("div", class_="post-content")
            contents = main_content.find_all("p")
            news_contents = []
            for content in contents:
                news_contents.append(self.preprocess_data(content))

            imgs = main_content.find_all("div", class_="image_center_wp")
            news_imgs = self.get_images(imgs)
            tags = page_soup.find("div", "block-content")
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
            img_name = img.find("p", class_="image_caption")

            if img_name:
                img_name = self.preprocess_data(img_name)
            else:
                img_name = ""
            news_imgs.append({
                "url": f"https://diendandoanhnghiep.vn{img_url}",
                "title": img_name
            })
        return news_imgs

    def get_tags(self, tags):
        news_tags = []
        if not tags:
            return news_tags
        _tags = tags.find_all("a")
        if not _tags:
            return news_tags
        for tag in _tags:
            news_tags.append(self.preprocess_data(tag))

        return news_tags

    def export_data(self, limit=None):
        page = self.start_page
        while True:
            if limit and page == limit:
                break
            begin = time.time()
            url = f"{self.url}/page-{page}.html"
            news_urls = self.fetch_data(url, self.get_all_news_url)
            if not news_urls:
                break
            for news_url in news_urls:
                logger.info(f"Export page {page}: {news_url}")
                data = self.fetch_data(news_url, self.get_news_info)
                file_name = self.get_file_name(news_url)
                if data:
                    data["url"] = news_url
                    if not self.use_kafka:
                        self.write_to_file(data, file_name)
                    else:
                        self.write_to_kafka(data, file_name)
            page += 1
            logger.info(f"Crawl {len(news_urls)} in {round(time.time() - begin, 2)}s")


if __name__ == "__main__":
    url = {
        # 'https://diendandoanhnghiep.vn/tai-chinh-ngan-hang-c7': "finance",
        'https://diendandoanhnghiep.vn/khoi-nghiep-c27': "fintech",
        # 'https://diendandoanhnghiep.vn/xe-c243': "fintech",
        # 'https://diendandoanhnghiep.vn/quoc-te-c24': "market",
        # 'https://diendandoanhnghiep.vn/dau-tu-chung-khoan-c124': "stock-market",
    }
    for key, value in url.items():
        job = DDDNCrawler(url=key, tag=value, start_page=820)
        job.export_data()
