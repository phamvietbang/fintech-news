class JobName:
    baodautu = 'baodautu'
    dantri = 'dantri'
    diendandoanhnghiep = 'diendandoanhnghiep'
    phapluatdoisong = 'phapluatdoisong'
    laodong = 'laodong'
    nhipcaudautu = 'nhipcaudautu'
    vietnamnet = 'vietnamnet'
    vneconomy = 'vneconomy'
    vtc = 'vtc'
    all = [vtc, baodautu, vneconomy, vietnamnet, nhipcaudautu, laodong, phapluatdoisong, diendandoanhnghiep, dantri]


class ImageUrls:
    mapping = {
        "baodautu": "https://media.baodautu.vn/",
        "dantri": "https://icdn.dantri.com.vn",
        "doisongphapluat": "https://media.doisongphapluat.com",
        "diendandoanhnghiep": 'https://diendandoanhnghiep.vn',
        "vneconomy": "https://media.vneconomy.vn",
        "vietnamnet": "https://static-images.vnncdn.net",
        "nhipcaudautu": "https://imgst.nhipcaudautu.vn/",
        "vtc": "https://cdn-i.vtcnews.vn"
    }


class Urls:
    diendandoanhnghiep = {
        "https://en.diendandoanhnghiep.vn/business-economics": "economy",
        "https://en.diendandoanhnghiep.vn/investment": "investment",
        "https://en.diendandoanhnghiep.vn/vcci": "fintech"
    }
    nhipcaudautu = {
        "https://e.nhipcaudautu.vn/economy": "economy",
        "https://e.nhipcaudautu.vn/tech": "fintech",
        "https://e.nhipcaudautu.vn/market": "market",
        "https://e.nhipcaudautu.vn/companies": "finance",
        "https://e.nhipcaudautu.vn/real-estate": "real-estate"
    }
    vneconomy = {
        "https://en.vneconomy.vn/investment.htm": "investment",
        "https://en.vneconomy.vn/green-economy.htm": "fintech",
        "https://en.vneconomy.vn/business.htm": "economy",
        "https://en.vneconomy.vn/banking-finance.htm": "finance",
        "https://en.vneconomy.vn/property.htm": "finance",
        "https://en.vneconomy.vn/digital-biz.htm": "fintech"
    }
    vietnamnet = {
            'https://vietnamnet.vn/en/business-news-tag1394942316563964544': "economy",
            'https://vietnamnet.vn/en/vietnams-stock-market-tag16543088519370150224': "market",
            'https://vietnamnet.vn/en/vietnams-real-estate-market-tag10713918583500998948': "real-estate",
            "https://vietnamnet.vn/en/vietnams-monetary-market-tag677031158975677480": "finance"
        }

# class Urls:
#     baodautu = {
#         "https://baodautu.vn/ngan-hang-d5": "finance",
#         "https://baodautu.vn/tai-chinh-chung-khoan-d6/": "stock-market",
#         "https://baodautu.vn/quoc-te-d54/": "market",
#     }
#     dantri = {
#         'https://dantri.com.vn/kinh-doanh/tai-chinh': "finance",
#         'https://dantri.com.vn/kinh-doanh/chung-khoan': "stock-market",
#         'https://dantri.com.vn/kinh-doanh/khoi-nghiep': "fintech",
#         'https://dantri.com.vn/kinh-doanh/thanh-toan-thong-minh': "fintech",
#         "https://dantri.com.vn/kinh-doanh/doanh-nghiep": "market",
#         "https://dantri.com.vn/kinh-doanh/tieu-dung": "market",
#     }
#     diendandoanhnghiep = {
#         'https://diendandoanhnghiep.vn/tai-chinh-ngan-hang-c7': "finance",
#         'https://diendandoanhnghiep.vn/khoi-nghiep-c27': "fintech",
#         'https://diendandoanhnghiep.vn/xe-c243': "fintech",
#         'https://diendandoanhnghiep.vn/quoc-te-c24': "market",
#         'https://diendandoanhnghiep.vn/dau-tu-chung-khoan-c124': "stock-market",
#     }
#     phapluatdoisong = {
#         'https://www.doisongphapluat.com/c/kinh-doanh': "market",
#         'https://www.doisongphapluat.com/c/tai-chinh-4': "finance",
#     }
#     laodong = {
#         'https://laodong.vn/tien-te-dau-tu': "finance",
#         'https://laodong.vn/thi-truong': "market",
#     }
#     nhipcaudautu = {
#         'https://nhipcaudautu.vn/tai-chinh/': "finance",
#         'https://nhipcaudautu.vn/cong-nghe/': "fintech",
#         'https://nhipcaudautu.vn/the-gioi/': "market",
#         'https://nhipcaudautu.vn/kinh-doanh/': "market"
#     }
#     vietnamnet = {
#         'https://vietnamnet.vn/kinh-doanh/tai-chinh': "finance",
#         'https://vietnamnet.vn/kinh-doanh/tu-van-tai-chinh': "finance",
#         'https://vietnamnet.vn/kinh-doanh/dau-tu': "market",
#         'https://vietnamnet.vn/kinh-doanh/thi-truong': "market",
#     }
#     vneconomy = {
#         "https://vneconomy.vn/tai-chinh.htm": "finance",
#         "https://vneconomy.vn/kinh-te-so.htm": "fintech",
#         "https://vneconomy.vn/chung-khoan.htm": "stock-market",
#         "https://vneconomy.vn/kinh-te-the-gioi.htm": "finance",
#     }
#     vtc = {
#         'https://vtc.vn/tai-chinh-200': "finance",
#         'https://vtc.vn/dau-tu-199': "market",
#         'https://vtc.vn/bao-ve-nguoi-tieu-dung-51': "market"
#     }
