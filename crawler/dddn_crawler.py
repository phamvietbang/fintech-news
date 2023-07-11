import json
import time

from bs4 import BeautifulSoup as soup
from ftfy import fix_encoding

from crawler.base_crawler import BaseCrawler
from utils.logger_utils import get_logger

logger = get_logger('DDDN Crawler')


class DDDNCrawler(BaseCrawler):
    def __init__(self, url, tag, start_page):
        super().__init__()
        self.start_page = start_page
        self.tag = tag
        self.url = url
        self.save_file = f"../.data/diendandoanhnghiep"

    @staticmethod
    def get_all_news_url(page_soup: soup):
        result = []
        h2_tags = page_soup.find_all("h2", class_="post-title")
        for tag in h2_tags:
            a_tag = tag.find("a")
            if 'href' in a_tag:
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
            attract = page_soup.find("strong", class_="post-sapo-description")
            author = page_soup.find("div", class_="post-author-share")
            author = self.preprocess_data(author)
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

    @staticmethod
    def get_file_name(news_url):
        file_name = news_url.split("/")[-1]
        file_name = file_name.split(".")[0]
        return file_name

    def get_images(self, imgs):
        news_imgs = []
        for img in imgs:
            img_url = img.find("img")["src"]
            img_name = img.find("p", class_="image_caption")
            if not img_url:
                continue
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
        _tags = tags.find_all("a")
        if not _tags:
            return news_tags
        for tag in _tags:
            news_tags.append(self.preprocess_data(tag))

        return news_tags

    def export_data(self):
        page = self.start_page

        while True:
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
                    self.write_to_file(data, file_name)
            page += 1
            logger.info(f"Crawl {len(news_urls)} in {round(time.time() - begin, 2)}s")