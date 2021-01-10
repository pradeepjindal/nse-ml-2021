import shutil

from urllib.request import Request, urlopen

link = 'https://www1.nseindia.com/content/historical/EQUITIES/2021/JAN/cm01JAN2021bhav.csv.zip'
link = 'https://www1.nseindia.com/content/historical/DERIVATIVES/2021/JAN/fo01JAN2021bhav.csv.zip'
link = 'https://www1.nseindia.com/archives/equities/mto/MTO_01012021.DAT'
header = {
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4',
    'Host': 'www1.nseindia.com',
    'Referer': 'https://www1.nseindia.com/',
    'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}


# def download_file(link, file_name, length):
#     try:
#         req = Request(link, headers=header)
#         with open(file_name, 'wb') as writer:
#             request = urlopen(req, timeout=3)
#             shutil.copyfileobj(request, writer, length)
#     except Exception as e:
#         print('File cannot be downloaded:', e)
#     finally:
#         print('File downloaded with success!')


# file_name = 'fo_file.zip'
# length = 1024
# download_file(link, file_name, length)


def download_historical_main():
    download_historical_mo()


def download_historical_mo():
    link_url = 'https://www1.nseindia.com/archives/equities/mto/MTO_DDMM20YY.DAT'
    file_name = 'MTO_DDMM20YY.DAT'
    month_list = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    for month in month_list:
        print('==============='+month)
        linko = link_url.replace('MM', month)
        namo = file_name.replace('MM', month)
        download_historical_year(linko, namo)


def download_historical_cm():
    link_url = 'https://www1.nseindia.com/content/historical/EQUITIES/20YY/JAN/cmDDJAN20YYbhav.csv.zip'
    file_name = 'cmDDJAN20YYbhav.csv.zip'
    month_list = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    for month in month_list:
        print('===============' + month)
        linko = link_url.replace('JAN', month)
        namo = file_name.replace('JAN', month)
        download_historical_year(linko, namo)


def download_historical_fo():
    link_url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/20YY/JAN/foDDJAN20YYbhav.csv.zip'
    file_name = 'foDDJAN20YYbhav.csv.zip'
    month_list = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    for month in month_list:
        print('===============' + month)
        linko = link_url.replace('JAN', month)
        namo = file_name.replace('JAN', month)
        download_historical_year(linko, namo)


def download_historical_year(link_url, file_name):
    for i in range(2, 16):
        print('----------' + str(i))
        if i < 10:
            yr = '0' + str(i)
            new_url = link_url.replace('YY', yr)
            new_file = file_name.replace('YY', yr)
        else:
            new_url = link_url.replace('YY', str(i))
            new_file = file_name.replace('YY', '' + str(i))
        download_historical_day(new_url, new_file)


def download_historical_day(link_url, file_name):
    for num in range(1, 32):
        print('.....' + str(num))
        if num < 10:
            applicable_url = link_url.replace('DD', '0' + str(num))
            applicable_name = file_name.replace('DD', '0' + str(num))
        else:
            applicable_url = link_url.replace('DD', str(num))
            applicable_name = file_name.replace('DD', str(num))
        download_file(applicable_url, applicable_name, 1024)


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


print('START')
download_historical_main()
