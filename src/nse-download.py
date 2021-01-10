from urllib.request import Request, urlopen
import shutil

link = 'https://www1.nseindia.com/content/historical/EQUITIES/2017/NOV/cm03NOV2017bhav.csv.zip'
link = 'https://www1.nseindia.com/content/historical/EQUITIES/2021/JAN/cm01JAN2021bhav.csv.zip'
link = 'https://www1.nseindia.com/content/historical/DERIVATIVES/2021/JAN/fo01JAN2021bhav.csv.zip'
header = {
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4',
    'Host': 'www1.nseindia.com',
    'Referer': 'https://www1.nseindia.com/',
    'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}


def download_file(link, file_name, length):
    try:
        req = Request(link, headers=header)
        with open(file_name, 'wb') as writer:
            request = urlopen(req, timeout=3)
            shutil.copyfileobj(request, writer, length)
    except Exception as e:
        print('File cannot be downloaded:', e)
    finally:
        print('File downloaded with success!')


file_name = 'fo_file.zip'
length = 1024
download_file(link, file_name, length)
