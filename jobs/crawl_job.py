import time

from kafka import KafkaProducer

from crawler.vtc_crawler import VTCCrawler
from crawler.baodautu_crawler import BaoDauTuCrawler
from crawler.vienamnet_crawler import VietNamNetCrawler
from crawler.dantri_crawler import DanTriCrawler
from crawler.dspl_crawler import PLDSCrawler
from crawler.laodong_crawler import LaoDongCrawler
from crawler.ncdt_crawler import NCDTCrawler
from crawler.vneconomy_crawler import VnEconomyCrawler
from crawler.dddn_crawler import DDDNCrawler
from constants import *
import multiprocessing
from utils.logger_utils import get_logger

logger = get_logger('BaoDauTu Crawler')


class CrawlingJob:
    def __init__(self, job_list, start_page, end_page, use_kafka=False):
        self.use_kafka = use_kafka
        self.end_page = end_page
        self.start_page = start_page
        self.job_list = job_list

    def run(self):
        jobs = []
        for job in self.job_list:
            if job == JobName.baodautu:
                self.add_job(jobs, job, self.crawl_baodautu)
            if job == JobName.dantri:
                self.add_job(jobs, job, self.crawl_dantri)
            if job == JobName.vneconomy:
                self.add_job(jobs, job, self.crawl_vneconomy)
            if job == JobName.vietnamnet:
                self.add_job(jobs, job, self.crawl_vietnamnet)
            if job == JobName.phapluatdoisong:
                self.add_job(jobs, job, self.crawl_phapluatdoisong)
            if job == JobName.diendandoanhnghiep:
                self.add_job(jobs, job, self.crawl_diendandoanhnghiep)
            if job == JobName.nhipcaudautu:
                self.add_job(jobs, job, self.crawl_nhipcaudautu)
            if job == JobName.laodong:
                self.add_job(jobs, job, self.crawl_laodong)
            if job == JobName.vtc:
                self.add_job(jobs, job, self.crawl_vtc)
        if not jobs:
            logger.warning("There is no job to run!")
        else:
            begin = time.time()
            logger.info(f"Start run {len(jobs)} jobs!")
            for job in jobs:
                job.start()

            for job in jobs:
                job.join()
            logger.info(f"Run {len(jobs)} in {time.time() - begin}s")

    @staticmethod
    def add_job(jobs, name, function):
        logger.info(f"Add job crawl {name}!")
        process = multiprocessing.Process(
            target=function,
        )
        jobs.append(process)
        return jobs

    def crawl_baodautu(self):
        for url_, tag_ in Urls.baodautu.items():
            job = BaoDauTuCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)

    def crawl_dantri(self):
        for url_, tag_ in Urls.dantri.items():
            job = DanTriCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)

    def crawl_laodong(self):
        for url_, tag_ in Urls.laodong.items():
            job = LaoDongCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)

    def crawl_vneconomy(self):
        for url_, tag_ in Urls.vneconomy.items():
            job = VnEconomyCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)

    def crawl_nhipcaudautu(self):
        for url_, tag_ in Urls.nhipcaudautu.items():
            job = NCDTCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)

    def crawl_diendandoanhnghiep(self):
        for url_, tag_ in Urls.diendandoanhnghiep.items():
            job = DDDNCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)

    def crawl_phapluatdoisong(self):
        for url_, tag_ in Urls.phapluatdoisong.items():
            job = PLDSCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)

    def crawl_vietnamnet(self):
        for url_, tag_ in Urls.vietnamnet.items():
            job = VietNamNetCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)

    def crawl_vtc(self):
        for url_, tag_ in Urls.vtc.items():
            job = VTCCrawler(url_, tag_, self.start_page)
            job.export_data(self.end_page)


if __name__ == "__main__":
    job = CrawlingJob(['baodautu', 'dantri'], 1, 2)
    job.run()
