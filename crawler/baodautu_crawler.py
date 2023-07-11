import json
import time

from crawler.base_crawler import BaseCrawler
from ftfy import fix_encoding
from selenium.webdriver import Chrome
from selenium.webdriver.common.by import By

from utils.logger_utils import get_logger

logger = get_logger('BaoDauTu Crawler')


class BaoDauTuCrawler(BaseCrawler):
    def __init__(self, url, tag, start_page):
        super().__init__()
        self.start_page = start_page
        self.tag = tag
        self.url = url
        self.save_file = f".data/baodautu"

    @staticmethod
    def get_all_news_url(driver: Chrome):
        result = []
        div_tags = driver.find_elements(By.CLASS_NAME, "desc_list_news_home")
        for tag in div_tags:
            tag_a = tag.find_element(By.TAG_NAME, "a")
            result.append(tag_a.get_attribute("href"))
        return result

    @staticmethod
    def preprocess_data(data):
        if data:
            text = data.text
        else:
            return None
        return fix_encoding(text).strip()

    def get_news_info(self, driver: Chrome):
        result = {"tag": self.tag}
        title = driver.find_element(By.CLASS_NAME, "title-detail")
        result["title"] = self.preprocess_data(title)
        attract = driver.find_element(By.CLASS_NAME, "sapo_detail")
        result["attract"] = self.preprocess_data(attract)
        date = driver.find_element(By.CLASS_NAME, "post-time")
        result["date"] = self.preprocess_data(date).replace(" - ", "")
        contents = driver.find_elements(By.TAG_NAME, "p")
        news_contents = []
        for content in contents:
            news_contents.append(self.preprocess_data(content))
        result["content"] = news_contents
        news_imgs = []
        imgs = driver.find_elements(By.TAG_NAME, "tbody")
        for img in imgs:
            img_info = img.find_elements(By.TAG_NAME, "td")
            if img_info:
                img_url = img_info[0].find_element(By.TAG_NAME, "img").get_attribute("src")
                img_name = ""
                if len(img_info) > 1:
                    img_name = self.preprocess_data(img_info[1])
                news_imgs.append({
                    "url": img_url,
                    "title": img_name
                })
        result["image"] = news_imgs
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
            driver = self.get_driver()
            try:
                begin = time.time()
                url = f"{self.url}/p{page}"
                news_urls = self.use_chrome_driver(driver, url, self.get_all_news_url)
                if not news_urls:
                    break
                for news_url in news_urls:
                    logger.info(f"Export page {page}: {news_url}")
                    data = self.use_chrome_driver(driver, news_url, self.get_news_info)
                    file_name = self.get_file_name(news_url)
                    if data:
                        self.write_to_file(data, file_name)
                page += 1
                logger.info(f"Crawl {len(news_urls)} in {round(time.time() - begin, 2)}s")
            except Exception as e:
                logger.error(e)
            finally:
                driver.quit()


if __name__ == "__main__":
    job = BaoDauTuCrawler(url="https://baodautu.vn/ngan-hang-d5", tag="ngan hang", start_page=1)
    job.export_data()
