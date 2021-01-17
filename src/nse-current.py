import os
import shutil
import datetime
import logging

import icecream


from urllib.request import Request, urlopen

imp_blog = 'https://mathdatasimplified.com/'

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

day_name_map = {'0': 'monday', '1': 'tuesday', '2': 'wednesday', '3': 'thursday', '4': 'friday', '5': 'SATURDAY', '6': 'SUNDAY', }
nse_data_dir_path = 'D:/nseEnv_2021/nseData2021.pgsql12'
nse_data_dir_path = 'D:/nseEnv-2021/nse-data'
nse_cm_dir_name = 'nse-cm'
nse_dm_dir_name = 'nse-dm'
nse_fm_dir_name = 'nse-fm'

# logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


# def download_main_old():
#     # logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
#
#     for year_num in range(2021, 2022):
#         for month_num in range(1, 13):
#             for day_num in range(1, 32):
#                 # calendar.calendar.month_name(month_num)
#                 date_str_yyyymmdd_template = '{:>4}-{:>2}-{:>2}'
#                 for_date_str_yyyymmdd = str(year_num) + str(month_num) + str(day_num)
#                 for_date_str_yyyymmdd = date_str_yyyymmdd_template.format(year_num, month_num, day_num)
#
#                 for_date_yyyymmdd = datetime.datetime(year_num, month_num, day_num)
#                 for_date_str_yyyymmdd = for_date_yyyymmdd.strftime('%y-%m-%d')
#
#                 print(f'loop for date: {for_date_yyyymmdd}, week day: {for_date_yyyymmdd.weekday()}')
#                 is_future_date = for_date_yyyymmdd > datetime.datetime.now()
#                 if is_future_date or is_week_end(for_date_yyyymmdd):
#                     continue
#                 print(f'---------')
#                 # download_cm(for_date_yyyymmdd)
#                 # download_dm(for_date_yyyymmdd)
#                 # download_fm(for_date_yyyymmdd)
#                 # icecream(for_date_str_yyyymmdd)
#     print(f'COMPLETED')


def download_main():
    # logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    print(datetime.datetime.now())
    for_date_yyyymmdd = datetime.datetime(2021, 1, 1)

    while for_date_yyyymmdd < datetime.datetime.now():
        print('----------------------------------')
        print(f'loop for date: {for_date_yyyymmdd.strftime("%Y-%b-%d")}, {day_name_map.get(str(for_date_yyyymmdd.weekday()))}')
        if is_week_end(for_date_yyyymmdd):
            for_date_yyyymmdd += datetime.timedelta(days=1)
            continue
        download_cm(for_date_yyyymmdd)
        download_dm(for_date_yyyymmdd)
        download_fm(for_date_yyyymmdd)
        for_date_yyyymmdd += datetime.timedelta(days=1)
    print(f'COMPLETED')


def download_dm(for_date_yyyymmdd):
    link_url = 'https://www1.nseindia.com/archives/equities/mto/MTO_DDMMYYYY.DAT'
    file_name = 'MTO_DDMMYYYY.DAT'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = for_date_yyyymmdd.strftime('%m')
    for_day = for_date_yyyymmdd.strftime('%d')
    applicable_url = link_url.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)
    applicable_name = file_name.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    if not file_found(applicable_name, nse_dm_dir_name):
        download_file(applicable_url, applicable_name, 1024, nse_dm_dir_name)
    else:
        print(f'{file_name} | File already downloaded !', applicable_name)
        # logging.debug('%s | File already downloaded !', applicable_name)


def download_cm(for_date_yyyymmdd):
    link_url = 'https://www1.nseindia.com/content/historical/EQUITIES/YYYY/MMM/cmDDMMMYYYYbhav.csv.zip'
    file_name = 'cmDDMMMYYYYbhav.csv.zip'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = str.upper(for_date_yyyymmdd.strftime('%b'))
    for_day = for_date_yyyymmdd.strftime('%d')
    applicable_url = link_url.replace('YYYY', for_year).replace('MMM', for_month).replace('DD', for_day)
    applicable_name = file_name.replace('YYYY', for_year).replace('MMM', for_month).replace('DD', for_day)

    if not file_found(applicable_name, nse_cm_dir_name):
        download_file(applicable_url, applicable_name, 1024, nse_cm_dir_name)
    else:
        print(f'{file_name} | File already downloaded !', applicable_name)
        # logging.debug('%s | File already downloaded !', applicable_name)
        # logging.error('%s raised an error', applicable_url)


def download_fm(for_date_yyyymmdd):
    link_url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/YYYY/MMM/foDDMMMYYYYbhav.csv.zip'
    file_name = 'foDDMMMYYYYbhav.csv.zip'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = str.upper(for_date_yyyymmdd.strftime('%b'))
    for_day = for_date_yyyymmdd.strftime('%d')
    applicable_url = link_url.replace('YYYY', for_year).replace('MMM', for_month).replace('DD', for_day)
    applicable_name = file_name.replace('YYYY', for_year).replace('MMM', for_month).replace('DD', for_day)

    if not file_found(applicable_name, nse_fm_dir_name):
        download_file(applicable_url, applicable_name, 1024, nse_fm_dir_name)
    else:
        print(f'{file_name} | File already downloaded !', applicable_name)
        # logging.debug('%s | File already downloaded !', applicable_name)


def file_found(file_name, cx_dir_name):
    file_name_along_with_full_path = get_file_name_along_with_absolute_path(nse_data_dir_path, cx_dir_name, file_name)
    if os.path.exists(file_name_along_with_full_path):
        file_size = os.path.getsize(file_name_along_with_full_path)
        if file_size > 0:
            return True
    return False


def download_file(linko, file_name, length, cx_dir_name):
    file_name_along_with_full_path = get_file_name_along_with_absolute_path(nse_data_dir_path, cx_dir_name, file_name)
    try:
        req = Request(linko, headers=header)
        with open(file_name_along_with_full_path, 'wb') as writer:
            request = urlopen(req, timeout=3)
            shutil.copyfileobj(request, writer, length)
    except Exception as e:
        print('File cannot be downloaded:', e)
    finally:
        print('File downloaded with success!')


def get_file_name_along_with_absolute_path(base_path, dir_name, file_name):
    file_name_along_with_full_path = base_path + '/' + dir_name + '/' + file_name
    return file_name_along_with_full_path


def is_week_day(for_date):
    day_num_starts_from_monday_and_value_is_0 = for_date.weekday()
    return day_num_starts_from_monday_and_value_is_0 < 5


def is_week_end(for_date):
    day_num_starts_from_monday_and_value_is_0 = for_date.weekday()
    return 4 < day_num_starts_from_monday_and_value_is_0


print('START')
download_main()
