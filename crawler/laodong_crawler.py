import json
import time

from bs4 import BeautifulSoup as soup
from ftfy import fix_encoding
from kafka import KafkaProducer
from selenium.webdriver.common.by import By

from crawler.baodautu_crawler import BaoDauTuCrawler
from utils.logger_utils import get_logger

logger = get_logger('LaoDong Crawler')


class LaoDongCrawler(BaoDauTuCrawler):
    def __init__(self, url, tag, start_page, producer: KafkaProducer = None, use_kafka=False):
        super().__init__(url, tag, start_page, producer, use_kafka)
        self.name = "laodong"
        self.save_file = f"./data"

    def get_all_news_url(self, page_soup):
        result = []
        main_div = page_soup.find_element(By.CLASS_NAME, "body-content")
        article_tags = main_div.find_elements(By.TAG_NAME, "article")
        for tag in article_tags:
            a_tag = tag.find_element(By.TAG_NAME, "a")
            result.append(a_tag.get_attribute('href'))
        result = list(set([url for url in result if self.url in url]))
        return result

    @staticmethod
    def preprocess_data(data):
        if data:
            text = data.text
        else:
            return None
        return fix_encoding(text).strip()

    def get_news_info(self, driver):
        try:
            page_soup = driver.find_element(By.TAG_NAME, "article")
            title = page_soup.find_element(By.CLASS_NAME, "title")
            attract = page_soup.find_element(By.CLASS_NAME, "chappeau")
            author = page_soup.find_element(By.TAG_NAME, "span")
            author = self.preprocess_data(author)
            date = page_soup.find_element(By.CLASS_NAME, "time")
            date = self.preprocess_data(date).split(",")[-1]

            contents = page_soup.find_elements(By.TAG_NAME, "p")
            news_contents = []
            for content in contents:
                news_contents.append(self.preprocess_data(content))
            try:
                imgs = page_soup.find_elements(By.XPATH, "//figure[@class='insert-center-image']")
                news_imgs = self.get_images(imgs)

                tags = page_soup.find_element(By.XPATH, "//div[@class='lst-tags']")
                news_tags = self.get_tags(tags)
            except:
                news_tags = []
                news_imgs = []
                pass
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
            img_url = img.find_element(By.TAG_NAME, "img")
            if not img_url:
                continue
            img_url = img_url.get_attribute("src")
            try:
                img_name = img.find_element(By.TAG_NAME, "figcaption")
            except:
                img_name = None

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
        if not tags:
            return news_tags
        _tags = tags.find_elements(By.TAG_NAME, "a")
        if not _tags:
            return news_tags
        for tag in _tags:
            news_tags.append(self.preprocess_data(tag))

        return news_tags

    def export_data(self, limit=None):
        page = self.start_page
        old_urls = []
        while True:
            driver = self.get_driver()
            try:
                if limit and page==limit:
                    break
                begin = time.time()
                url = f"{self.url}?page={page}"
                news_urls = self.use_chrome_driver(driver, url, self.get_all_news_url)

                if not news_urls or old_urls == news_urls:
                    break
                old_urls = news_urls
                for news_url in news_urls:
                    logger.info(f"Export page {page}: {news_url}")
                    data = self.use_chrome_driver(driver, news_url, self.get_news_info)
                    file_name = self.get_file_name(news_url)
                    if data:
                        data["url"] = news_url
                        if not self.use_kafka:
                            self.write_to_file(data, file_name)
                        else:
                            self.write_to_kafka(data, file_name)

                page += 1
                logger.info(f"Crawl {len(news_urls)} in {round(time.time() - begin, 2)}s")
            except Exception as e:
                page += 1
                logger.error(e)
                pass
            finally:
                driver.quit()


if __name__ == "__main__":
    url = {
        'https://laodong.vn/tien-te-dau-tu': "finance",
        # 'https://laodong.vn/thi-truong': "market",
    }
    for key, value in url.items():
        job = LaoDongCrawler(url=key, tag=value, start_page=4)
        job.export_data()
