import os
import shutil
import datetime
import csv
import pandas as pd
import logging

# import icecream

from urllib.request import Request, urlopen

print_download_skipped = True
print_download_success = True
print_download_failure = True

cm_download_disabled = False
dm_download_disabled = False
fm_download_disabled = False
idx_download_disabled = False
ls_download_disabled = False
fo_download_disabled = False


imp_blog = 'https://mathdatasimplified.com/'

daily_report_down_laod_when_automatic_fails = 'https://www1.nseindia.com/products/content/all_daily_reports.htm'
oi_limit = 'https://www1.nseindia.com/archives/nsccl/mwpl/nseoi_06122021.zip'

fo_data_having_lot_size = 'https://www1.nseindia.com/archives/fo/mkt/fo07092021.zip'

bhavcopy = 'https://www.indiainx.com/markets/dailymarketdata.aspx'



folots = 'https://archives.nseindia.com/content/fo/fo_mktlots_03122021.csv'
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
# nse_cm_dir_name = 'nse-cm'
# nse_dm_dir_name = 'nse-dm'
# nse_fm_dir_name = 'nse-fm'
# nse_dx_dir_name = 'nse-dx'

# logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')


def download_main():

    # logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    print(datetime.datetime.now())
    # from_date_yyyymmdd = datetime.datetime(2019, 12, 31) # mktlots since
    from_date_yyyymmdd = datetime.datetime(2022, 1, 1)

    to_date = datetime.datetime.now()
    # to_date = datetime.datetime.strptime('2015-12-31', '%Y-%m-%d')

    for_date_yyyymmdd = from_date_yyyymmdd
    while for_date_yyyymmdd <= to_date:
        print('----------------------------------')
        print(f'loop for date: {for_date_yyyymmdd.strftime("%Y-%b-%d")}, {day_name_map.get(str(for_date_yyyymmdd.weekday()))}')
        if is_week_end(for_date_yyyymmdd):
            for_date_yyyymmdd += datetime.timedelta(days=1)
            continue
        #
        download_ls(for_date_yyyymmdd)
        transform_ls_prepare(for_date_yyyymmdd)

        for_date_yyyymmdd += datetime.timedelta(days=1)
    print('----------------------------------')
    print(f'COMPLETED')


def download_ls(for_date_yyyymmdd):
    nse_mktlots_from_date = datetime.datetime(2019, 12, 31)

    if for_date_yyyymmdd < nse_mktlots_from_date:
        print('mktlots:downlaod - pre date then nse begin to provide the file')
        return

    if ls_download_disabled:
        print('mktlots download DISABLED')
        return

    link_url = 'https://archives.nseindia.com/content/fo/fo_mktlots_DDMMYYYY.csv'
    file_name_template = 'fo_mktlots_DDMMYYYY.csv'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = str.upper(for_date_yyyymmdd.strftime('%m'))
    for_day = for_date_yyyymmdd.strftime('%d')

    applicable_url = link_url.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)
    file_name = file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    applicable_dir = 'nse-ls'
    if not file_exist(file_name, applicable_dir):
        req = Request(applicable_url)
        download_file(file_name, 1024, applicable_dir, req)
    else:
        abc = '{:10s} {:3d}  {:7.2f}'.format('xxx', 123, 98)
        # print(f'{applicable_name} | already downloaded !')
        if print_download_skipped:
            print('{:30s} | already downloaded !'.format(file_name))
        # logging.debug('%s | already downloaded !', applicable_name)

    if file_found(file_name, applicable_dir):
        copy_and_rename_file(for_date_yyyymmdd, file_name, applicable_dir)


def copy_and_rename_file(for_date_yyyymmdd, source_file_name, from_dir):
    LS_COPY_TARGET_DIR = 'nse-ls'
    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = for_date_yyyymmdd.strftime('%m')
    for_day = for_date_yyyymmdd.strftime('%d')

    # file name pattern
    file_name_template = 'fo_mktlots_-YYYY-MM-DD.csv'
    target_file_name = file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    # Source path
    source = "/home/User/Documents/file.txt"
    source = get_file_name_along_with_absolute_path(nse_data_dir_path, from_dir, source_file_name)

    # target path
    target = "/home/User/Documents"
    target = get_file_name_along_with_absolute_path(nse_data_dir_path, LS_COPY_TARGET_DIR, target_file_name)

    # Copy the content of source to target
    try:
        shutil.copyfile(source, target)
        print("File copied successfully.")

    # If source and target are same
    except shutil.SameFileError:
        print("Source and target represents the same file.")

    # If target is a directory.
    except IsADirectoryError:
        print("target is a directory.")

    # If there is any permission issue
    except PermissionError:
        print("Permission denied.")

    # For other errors
    except:
        print("Error occurred while copying file.")


def transform_ls_prepare(for_date_yyyymmdd):
    nse_mktlots_from_date = datetime.datetime(2019, 12, 31)
    if for_date_yyyymmdd < nse_mktlots_from_date:
        print('mktlots:transform - pre date then nse begin to provide the file')
        return

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = for_date_yyyymmdd.strftime('%m')
    for_day = for_date_yyyymmdd.strftime('%d')

    # file name pattern
    source_file_name_template = 'fo_mktlots_DDMMYYYY.csv'
    # source_file_name_template = 'fo_mktlots_-YYYY-MM-DD.csv'
    target_file_name_template = 'ls-YYYY-MM-DD.csv'
    # file names
    source_file_name = source_file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)
    target_file_name = target_file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    # dirs
    LS_SOURCE_DIR = 'nse-ls'
    LS_TRANSFORM_TARGET_DIR = 'pra-ls'
    # Source path
    source_file = get_file_name_along_with_absolute_path(nse_data_dir_path, LS_SOURCE_DIR, source_file_name)
    # target path
    target_file = get_file_name_along_with_absolute_path(nse_data_dir_path, LS_TRANSFORM_TARGET_DIR, target_file_name)

    if not file_found(source_file_name, LS_SOURCE_DIR):
        print('mktlots:transform - skipping, file not found')
        return
    transform_ls_copy(for_date_yyyymmdd, source_file, target_file)


def transform_ls_copy(for_date_yyyymmdd, source_file, target_file):
    csv_str_list = []
    csv_str_list.append('symbol,trade_date,expiry_date,lot_size')

    trade_date_str = for_date_yyyymmdd.strftime("%Y-%m-%d")
    expiry_date_t1_str = ''
    expiry_date_t2_str = ''
    expiry_date_t3_str = ''
    with open(source_file) as mktlots_csv_file:
        mktlots_csv_reader = csv.reader(mktlots_csv_file, delimiter=',')
        line_count = 0
        for row in mktlots_csv_reader:
            if line_count == 0:
                expiry_date_t1_str, expiry_date_t2_str, expiry_date_t3_str = extract_exprity_dates(row)
                line_count += 1
            else:
                if line_count > 1 and row[1].strip().upper() == 'SYMBOL':
                    expiry_date_t1_str, expiry_date_t2_str, expiry_date_t3_str = extract_exprity_dates(row)
                    line_count += 1
                else:
                    # print(f'\t{row[1].strip()}, {row[2].strip()}, {row[3].strip()}, {row[4].strip()}')
                    # csv_str = row[1].strip() + ',' + row[2].strip() + ',' + row[3].strip() + ',' + row[4].strip()
                    if row[2].strip():
                        csv_str_t1 = row[1].strip() + ',' + trade_date_str + ',' + expiry_date_t1_str + ',' + row[2].strip()
                        csv_str_list.append(csv_str_t1)
                    if row[3].strip():
                        csv_str_t2 = row[1].strip() + ',' + trade_date_str + ',' + expiry_date_t2_str + ',' + row[3].strip()
                        csv_str_list.append(csv_str_t2)
                    if row[4].strip():
                        csv_str_t3 = row[1].strip() + ',' + trade_date_str + ',' + expiry_date_t3_str + ',' + row[4].strip()
                        csv_str_list.append(csv_str_t3)
                    line_count += 1
        print(f'Processed {line_count} lines.')

    #
    f = open(target_file, 'w')
    for each_csv_str in csv_str_list:
        f.write(each_csv_str + '\n')
    f.close()
    print(f'Saved {1} lines.')
    #
    # with open(target_file, mode='w') as ls_csv_file:
    #     # ls_csv_writer = csv.writer(ls_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    #     ls_csv_writer = csv.writer(ls_csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONE)
    #
    #     # ls_csv_writer.writerow(['John Smith', 'Accounting', 'November'])
    #     # ls_csv_writer.writerow(['Erica Meyers', 'IT', 'March'])
    #     # ls_csv_writer.writerows(csv_str_list)
    #     for each_csv_str in csv_str_list:
    #         ls_csv_writer.writerow(each_csv_str)
    # print(f'Processed {1} lines.')


def extract_exprity_dates(row):
    try:
        expiry_date_t1 = datetime.datetime.strptime(row[2].strip(), '%b-%y')
        expiry_date_t2 = datetime.datetime.strptime(row[3].strip(), '%b-%y')
        expiry_date_t3 = datetime.datetime.strptime(row[4].strip(), '%b-%y')
        regular_date_format = True
    except ValueError:
        print('mktlots:transform - ValueError')
        regular_date_format = False

    if not regular_date_format:
        expiry_date_t1 = datetime.datetime.strptime(row[2].strip(), '%y-%b')
        expiry_date_t2 = datetime.datetime.strptime(row[3].strip(), '%y-%b')
        expiry_date_t3 = datetime.datetime.strptime(row[4].strip(), '%y-%b')

    expiry_date_t1_str = expiry_date_t1.strftime("%Y-%m-%d")
    expiry_date_t2_str = expiry_date_t2.strftime("%Y-%m-%d")
    expiry_date_t3_str = expiry_date_t3.strftime("%Y-%m-%d")

    return expiry_date_t1_str, expiry_date_t2_str, expiry_date_t3_str


def file_found(file_name, cx_dir_name):
    file_name_along_with_full_path = get_file_name_along_with_absolute_path(nse_data_dir_path, cx_dir_name, file_name)
    if os.path.exists(file_name_along_with_full_path):
        file_size = os.path.getsize(file_name_along_with_full_path)
        if file_size > 0:
            return True
    return False


def file_exist(file_name, dir_name):
    file_name_along_with_full_path = get_file_name_along_with_absolute_path(nse_data_dir_path, dir_name, file_name)
    if os.path.exists(file_name_along_with_full_path):
        return True
    return False


def download_file(file_name, length, cx_dir_name, req):
    file_name_along_with_full_path = get_file_name_along_with_absolute_path(nse_data_dir_path, cx_dir_name, file_name)
    try:
        # do not remove these lines
        # req = Request(linko, headers=header)
        # req = Request(linko)
        with open(file_name_along_with_full_path, 'wb') as writer:
            request = urlopen(req, timeout=3)
            shutil.copyfileobj(request, writer, length)
        # print('download succeed! - ' + file_name)
        if print_download_success:
            print('{:30s} | download succeed!'.format(file_name))
    except Exception as e:
        # print(f'{file_name} | downloaded failed:', e)
        if print_download_failure:
            print('{:30s} | download FAILED!    '.format(file_name), e)
    finally:
        pass


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
