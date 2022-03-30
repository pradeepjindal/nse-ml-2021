import os
import shutil
import datetime
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

crosstab = 'https://www.postgresql.org/docs/current/tablefunc.html'

imp_blog = 'https://mathdatasimplified.com/'

daily_report_down_laod_when_automatic_fails = 'https://www1.nseindia.com/products/content/all_daily_reports.htm'
oi_limit = 'https://www1.nseindia.com/archives/nsccl/mwpl/nseoi_06122021.zip'

fo_data_having_lot_size = 'https://www1.nseindia.com/archives/fo/mkt/fo07092021.zip'

bhavcopy = 'https://www.indiainx.com/markets/dailymarketdata.aspx'

delivery_data_alternate = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_14012022.csv'

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
    # from_date_yyyymmdd = datetime.datetime(2019, 12, 31) mktlots since
    # from_date_yyyymmdd = datetime.datetime(2022, 1, 1)
    from_date_yyyymmdd = datetime.datetime(2022, 3, 21)

    to_date = datetime.datetime.now()
    # to_date = datetime.datetime.strptime('2015-12-31', '%Y-%m-%d')

    for_date_yyyymmdd = from_date_yyyymmdd
    while for_date_yyyymmdd <= to_date:
        print('----------------------------------')
        print(f'loop for date: {for_date_yyyymmdd.strftime("%Y-%b-%d")}, {day_name_map.get(str(for_date_yyyymmdd.weekday()))}')
        if is_week_end(for_date_yyyymmdd):
            for_date_yyyymmdd += datetime.timedelta(days=1)
            continue
        download_cm(for_date_yyyymmdd)
        download_dm(for_date_yyyymmdd)
        download_fm(for_date_yyyymmdd)
        download_nx(for_date_yyyymmdd)
        #
        download_mktlots(for_date_yyyymmdd)
        download_fo(for_date_yyyymmdd)
        download_bhav_data_full(for_date_yyyymmdd)
        for_date_yyyymmdd += datetime.timedelta(days=1)
    print('----------------------------------')
    print(f'COMPLETED')


def download_bhav_data_full(for_date_yyyymmdd):
    sec_bhav_data_full_from_date = datetime.datetime(2019, 9, 30)
    if for_date_yyyymmdd < sec_bhav_data_full_from_date:
        print('not available')
        return
    # if idx_download_disabled:
    #     print('idx download DISABLED')
    #     return

    link_url = 'https://archives.nseindia.com/products/content/sec_bhavdata_full_DDMMYYYY.csv'
    file_name_template = 'sec_bhavdata_full_DDMMYYYY.csv'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = str.upper(for_date_yyyymmdd.strftime('%m'))
    for_day = for_date_yyyymmdd.strftime('%d')

    applicable_url = link_url.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)
    file_name = file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    applicable_dir = 'nse-bf'
    if not file_found(file_name, applicable_dir):
        req = Request(applicable_url)
        download_file(file_name, 1024, applicable_dir, req)
    else:
        abc = '{:10s} {:3d}  {:7.2f}'.format('xxx', 123, 98)
        # print(f'{file_name} | already downloaded !')
        if print_download_skipped:
            print('{:30s} | already downloaded !'.format(file_name))
        # logging.debug('%s | already downloaded !', file_name)


def download_fo(for_date_yyyymmdd):
    nse_fo_from_date = datetime.datetime(2015, 1, 1)

    if for_date_yyyymmdd < nse_fo_from_date:
        print('fo - pre date then nse begin to provide the file')
        return

    if fo_download_disabled:
        print('fo download DISABLED')
        return

    link_url = 'https://www1.nseindia.com/archives/fo/mkt/foDDMMYYYY.zip'
    file_name_template = 'foDDMMYYYY.zip'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = str.upper(for_date_yyyymmdd.strftime('%m'))
    for_day = for_date_yyyymmdd.strftime('%d')

    applicable_url = link_url.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)
    file_name = file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    applicable_dir = 'nse-fo'
    if not file_found(file_name, applicable_dir):
        req = Request(applicable_url)
        download_file(file_name, 1024, applicable_dir, req)
    else:
        abc = '{:10s} {:3d}  {:7.2f}'.format('xxx', 123, 98)
        # print(f'{file_name} | already downloaded !')
        if print_download_skipped:
            print('{:30s} | already downloaded !'.format(file_name))
        # logging.debug('%s | already downloaded !', file_name)


def download_mktlots(for_date_yyyymmdd):
    nse_mktlots_from_date = datetime.datetime(2019, 12, 31)

    if for_date_yyyymmdd < nse_mktlots_from_date:
        print('mktlots - pre date then nse begin to provide the file')
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
    if not file_present(file_name, applicable_dir):
        req = Request(applicable_url)
        download_file(file_name, 1024, applicable_dir, req)
    else:
        abc = '{:10s} {:3d}  {:7.2f}'.format('xxx', 123, 98)
        # print(f'{file_name} | already downloaded !')
        if print_download_skipped:
            print('{:30s} | already downloaded !'.format(file_name))
        # logging.debug('%s | already downloaded !', file_name)

    if file_found(file_name, applicable_dir):
        copy_file(for_date_yyyymmdd, file_name, applicable_dir, 'pra-ls')


def download_nx(for_date_yyyymmdd):
    if idx_download_disabled:
        print('idx download DISABLED')
        return

    link_url = 'https://www1.nseindia.com/content/indices/ind_close_all_DDMMYYYY.csv'
    file_name_template = 'ind_close_all_DDMMYYYY.csv'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = str.upper(for_date_yyyymmdd.strftime('%m'))
    for_day = for_date_yyyymmdd.strftime('%d')

    applicable_url = link_url.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)
    file_name = file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    applicable_dir = 'nse-nx'
    if not file_found(file_name, applicable_dir):
        req = Request(applicable_url)
        download_file(file_name, 1024, applicable_dir, req)
    else:
        abc = '{:10s} {:3d}  {:7.2f}'.format('xxx', 123, 98)
        # print(f'{file_name} | already downloaded !')
        if print_download_skipped:
            print('{:30s} | already downloaded !'.format(file_name))
        # logging.debug('%s | already downloaded !', file_name)


# this file is not being download in proper format so skipping it until there is a solution
def download_dm(for_date_yyyymmdd):
    if dm_download_disabled:
        print('dm download DISABLED')
        return

    link_url = 'https://www1.nseindia.com/archives/equities/mto/MTO_DDMMYYYY.DAT'
    file_name_template = 'MTO_DDMMYYYY.DAT'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = for_date_yyyymmdd.strftime('%m')
    for_day = for_date_yyyymmdd.strftime('%d')

    applicable_url = link_url.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)
    file_name = file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    applicable_dir = 'nse-dm'
    if not file_found(file_name, applicable_dir):
        req = Request(applicable_url)
        download_file(file_name, 1024, applicable_dir, req)
    else:
        # print(f'{file_name} | already downloaded !')
        if print_download_skipped:
            print('{:30s} | already downloaded !'.format(file_name))
        # logging.debug('%s | already downloaded !', file_name)


def download_cm(for_date_yyyymmdd):
    if cm_download_disabled:
        print('cm download DISABLED')
        return

    link_url = 'https://www1.nseindia.com/content/historical/EQUITIES/YYYY/MMM/cmDDMMMYYYYbhav.csv.zip'
    file_name_template = 'cmDDMMMYYYYbhav.csv.zip'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = str.upper(for_date_yyyymmdd.strftime('%b'))
    for_day = for_date_yyyymmdd.strftime('%d')

    applicable_url = link_url.replace('YYYY', for_year).replace('MMM', for_month).replace('DD', for_day)
    file_name = file_name_template.replace('YYYY', for_year).replace('MMM', for_month).replace('DD', for_day)

    applicable_dir = 'nse-cm'
    if not file_found(file_name, applicable_dir):
        req = Request(applicable_url, headers=header)
        download_file(file_name, 1024, applicable_dir, req)
    else:
        # print(f'{file_name}    | already downloaded !')
        if print_download_skipped:
            print('{:30s} | already downloaded !'.format(file_name))
        # logging.debug('%s | already downloaded !', file_name)
        # logging.error('%s raised an error', applicable_url)


def download_fm(for_date_yyyymmdd):
    if fm_download_disabled:
        print('fm download DISABLED')
        return

    link_url = 'https://www1.nseindia.com/content/historical/DERIVATIVES/YYYY/MMM/foDDMMMYYYYbhav.csv.zip'
    file_name_template = 'foDDMMMYYYYbhav.csv.zip'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = str.upper(for_date_yyyymmdd.strftime('%b'))
    for_day = for_date_yyyymmdd.strftime('%d')

    applicable_url = link_url.replace('YYYY', for_year).replace('MMM', for_month).replace('DD', for_day)
    file_name = file_name_template.replace('YYYY', for_year).replace('MMM', for_month).replace('DD', for_day)

    applicable_dir = 'nse-fm'
    if not file_found(file_name, applicable_dir):
        req = Request(applicable_url, headers=header)
        download_file( file_name, 1024, applicable_dir, req)
    else:
        # print(f'{file_name}    | already downloaded !')
        if print_download_skipped:
            print('{:30s} | already downloaded !'.format(file_name))
        # logging.debug('%s | already downloaded !', file_name)


def file_found(file_name, cx_dir_name):
    file_name_along_with_full_path = get_file_name_along_with_absolute_path(nse_data_dir_path, cx_dir_name, file_name)
    if os.path.exists(file_name_along_with_full_path):
        file_size = os.path.getsize(file_name_along_with_full_path)
        if file_size > 0:
            return True
    return False


def file_present(file_name, dir_name):
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
            print('{:30s} | succeed DOWNLOAD!'.format(file_name))
    except Exception as e:
        # print(f'{file_name} | downloaded failed:', e)
        if print_download_failure:
            print('{:30s} | FAILED download!    '.format(file_name), e)
    finally:
        pass


def copy_file(for_date_yyyymmdd, source_file_name, from_dir, to_dir):
    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month = for_date_yyyymmdd.strftime('%m')
    for_day = for_date_yyyymmdd.strftime('%d')

    # file name pattern
    file_name_template = 'ls-YYYY-MM-DD.csv'
    target_file_name = file_name_template.replace('YYYY', for_year).replace('MM', for_month).replace('DD', for_day)

    # Source path
    source = "/home/User/Documents/file.txt"
    source = get_file_name_along_with_absolute_path(nse_data_dir_path, from_dir, source_file_name)

    # target path
    target = "/home/User/Documents"
    target = get_file_name_along_with_absolute_path(nse_data_dir_path, to_dir, target_file_name)

    # Copy the content of
    # source to target

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
