import os
import shutil
import datetime
import requests
import pandas as pd

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen


url_link = 'https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Jan312022.html'

header = {
    'Accept-Encoding': 'gzip, deflate, sdch, br',
    'Accept-Language': 'fr-FR,fr;q=0.8,en-US;q=0.6,en;q=0.4',
    'Host': 'www1.nseindia.com',
    'Referer': 'https://www1.nseindia.com/',
    'User-Agent': 'Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest'
}

day_name_map = {'0': 'monday', '1': 'tuesday', '2': 'wednesday', '3': 'thursday', '4': 'friday', '5': 'SATURDAY', '6': 'SUNDAY', }
nse_data_dir_path = 'D:/nseEnv-2021/nse-data'


def download_main():
    print(datetime.datetime.now())
    from_date_yyyymmdd = datetime.datetime(2015, 1, 1)

    to_date = datetime.datetime.now()
    # to_date = datetime.datetime.strptime('2021-06-30', '%Y-%m-%d')

    for_date_yyyymmdd = from_date_yyyymmdd
    while for_date_yyyymmdd <= to_date:
        # print('----------------------------------')
        dt_str = for_date_yyyymmdd.strftime("%Y-%b-%d")
        for_date_yyyymmdd += datetime.timedelta(days=1)

        day_str = for_date_yyyymmdd.strftime("%d")
        if day_str == '15':
            download_fpi(for_date_yyyymmdd)
        if int(day_str) < 27:
            continue
        last_date_of_month = last_day_of_month(for_date_yyyymmdd)
        if for_date_yyyymmdd == last_date_of_month:
            download_fpi(for_date_yyyymmdd)

    print('----------------------------------')
    print(f'COMPLETED')


def download_fpi(for_date_yyyymmdd):
    file_name_template = 'fpi-YYYY-MM-DD.csv'

    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month_num = for_date_yyyymmdd.strftime('%m')
    for_day = for_date_yyyymmdd.strftime('%d')

    file_name = file_name_template.replace('YYYY', for_year).replace('MM', for_month_num).replace('DD', for_day)

    applicable_dir = 'fpi-data'
    file_name_along_with_full_path = get_file_name_along_with_absolute_path(nse_data_dir_path, applicable_dir, file_name)

    final_url = get_final_url_having_short_month_name(for_date_yyyymmdd)
    status = None
    if not file_found(file_name, applicable_dir):
        print('{:30s} | tobe downloaded !, %s'.format(file_name), final_url)
        status = download_it(final_url, file_name_along_with_full_path)
    else:
        print('{:30s} | already downloaded !'.format(file_name))
        return

    if status:
        final_url = get_final_url_having_full_month_name(for_date_yyyymmdd)
        print('{:30s} | tobe downloaded !, %s'.format(file_name), final_url)
        status = download_it(final_url, file_name_along_with_full_path)

    if status:
        final_url = get_final_url_having_full_month_name_without_underscore(for_date_yyyymmdd)
        print('{:30s} | tobe downloaded !, %s'.format(file_name), final_url)
        status = download_it(final_url, file_name_along_with_full_path)

    # special provisions for 2015 data
    if status and int(for_year) < 2016:
        final_url = get_final_url_having_short_month_name_and_char_v_is_missing_from_url(for_date_yyyymmdd)
        print('{:30s} | tobe downloaded !, %s'.format(file_name), final_url)
        status = download_it(final_url, file_name_along_with_full_path)

    if status and int(for_year) < 2016:
        final_url = get_final_url_having_full_month_name_and_char_v_is_missing_from_url(for_date_yyyymmdd)
        print('{:30s} | tobe downloaded !, %s'.format(file_name), final_url)
        status = download_it(final_url, file_name_along_with_full_path)

    if status and int(for_year) < 2016:
        final_url = get_final_url_having_short_month_name_and_char_l_is_missing_from_html(for_date_yyyymmdd)
        print('{:30s} | tobe downloaded !, %s'.format(file_name), final_url)
        status = download_it(final_url, file_name_along_with_full_path)

    return


def get_final_url_having_short_month_name(for_date_yyyymmdd):
    template_url = 'https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_MMMDDYYYY.html'
    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month_str = for_date_yyyymmdd.strftime('%b')
    for_day = for_date_yyyymmdd.strftime('%d')
    final_url = template_url.replace('YYYY', for_year).replace('MMM', for_month_str).replace('DD', for_day)
    return final_url


def get_final_url_having_short_month_name_and_char_v_is_missing_from_url(for_date_yyyymmdd):
    template_url = 'https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInestSector_MMMDDYYYY.html'
    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month_str = for_date_yyyymmdd.strftime('%b')
    for_day = for_date_yyyymmdd.strftime('%d')
    final_url = template_url.replace('YYYY', for_year).replace('MMM', for_month_str).replace('DD', for_day)
    return final_url


def get_final_url_having_short_month_name_and_char_l_is_missing_from_html(for_date_yyyymmdd):
    template_url = 'https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_MMMDDYYYY.htm'
    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month_str = for_date_yyyymmdd.strftime('%b')
    for_day = for_date_yyyymmdd.strftime('%d')
    final_url = template_url.replace('YYYY', for_year).replace('MMM', for_month_str).replace('DD', for_day)
    return final_url


def get_final_url_having_full_month_name(for_date_yyyymmdd):
    url_prefix = 'https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_'
    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month_str = for_date_yyyymmdd.strftime('%B')
    for_day = for_date_yyyymmdd.strftime('%d')
    url_postfix = for_month_str + for_day + for_year + '.html'
    final_url = url_prefix + url_postfix
    return final_url


def get_final_url_having_full_month_name_without_underscore(for_date_yyyymmdd):
    url_prefix = 'https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector'
    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month_str = for_date_yyyymmdd.strftime('%B')
    for_day = for_date_yyyymmdd.strftime('%d')
    url_postfix = for_month_str + for_day + for_year + '.html'
    final_url = url_prefix + url_postfix
    return final_url


def get_final_url_having_full_month_name_and_char_v_is_missing_from_url(for_date_yyyymmdd):
    url_prefix = 'https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInestSector_'
    for_year = for_date_yyyymmdd.strftime('%Y')
    for_month_str = for_date_yyyymmdd.strftime('%B')
    for_day = for_date_yyyymmdd.strftime('%d')
    url_postfix = for_month_str + for_day + for_year + '.html'
    final_url = url_prefix + url_postfix
    return final_url


def download_it(final_url, file_name):
    # pages = requests.get('https://www.fpi.nsdl.co.in/web/StaticReports/Fortnightly_Sector_wise_FII_Investment_Data/FIIInvestSector_Feb282021.html')
    pages = requests.get(final_url)
    if pages.status_code != 200:
        print(pages.status_code)
        return pages.status_code
    txt = pages.text

    index = pages.text.find('ÿþ')
    if index == 0:
        content = txt.encode().decode('utf-16-le')
    else:
        content = pages.text[2:]

    # parser-lxml = Change html to Python friendly format
    # soup = BeautifulSoup(pages.text, 'lxml')
    soup = BeautifulSoup(content, "html.parser")
    soup

    # table1 = soup.find('table', id='main_table_countries_today')
    table1 = soup.find('table')

    if not table1:
        print('reading error, skipping it')
        return 'NotFound'

    # Obtain every title of columns with tag <th>
    headers = []
    # for i in table1.find_all('th'):
    #     title = i.text
    #     headers.append(title)

    for j in table1.find_all('tr')[2:]:
        title = j.text
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        headers = row
        break

    # Create a dataframe
    mydata = pd.DataFrame(columns=headers)

    #
    no = len(headers) - 2
    remainder = no % 8

    i = 0
    header_set_no = 1
    map_of_header_sets = {}
    for each_header in headers:
        i += 1
        if i > 2:
            if header_set_no in map_of_header_sets:
                if each_header in map_of_header_sets[header_set_no]:
                    header_set_no += 1
                    map_of_header_sets[header_set_no] = [each_header]
                else:
                    map_of_header_sets[header_set_no].append(each_header)
            else:
                map_of_header_sets[header_set_no] = [each_header]

    i = 0
    header_set = {}
    for each_header in headers:
        i += 1
        if i > 2:
            header_set[each_header] = each_header

    _prepare_first_row(table1, map_of_header_sets, len(header_set), mydata)

    _prepare_second_row(table1, map_of_header_sets, len(header_set), mydata)

    # Create a for loop to fill mydata
    for j in table1.find_all('tr')[2:]:
        row_data = j.find_all('td')
        row = [i.text.replace(',', '') for i in row_data]
        length = len(mydata)
        mydata.loc[length] = row

    # Export to csv
    mydata.to_csv(file_name, index=False)

    # Try to read csv
    # mydata2 = pd.read_csv(file_name)

    print('downloaded')
    return None


def _prepare_first_row(table1, map_of_header_sets, header_set_length, mydata):
    for j in table1.find_all('tr')[0:1]:
        row_data = j.find_all('td')
        # row = [i.text + ',,,,,' for i in row_data]

        # dummy = []
        # i = 0
        # while i < header_set_length*2-1:
        #     i += 1
        #     dummy.append(',')
        dummy = _prepare_dummy(header_set_length * 2 - 1)

        i = 0
        row = []
        for each_data in row_data:
            td_txt = each_data.text
            row.append(td_txt)
            # if td_txt:
            #     row = row + dummy
            if td_txt:
                length = len(map_of_header_sets[i+1]) + len(map_of_header_sets[i+2]) - 1
                dummy = _prepare_dummy(length)
                row = row + dummy
                i += 2

        length = len(mydata)
        mydata.loc[length] = row


def _prepare_second_row(table1, map_of_header_sets, header_set_length, mydata):
    for j in table1.find_all('tr')[1:2]:
        row_data = j.find_all('td')
        # row = [i.text + ',,,,,' for i in row_data]

        # dummy = []
        # i = 0
        # while i < header_set_length-1:
        #     i += 1
        #     dummy.append(',')
        dummy = _prepare_dummy(header_set_length-1)

        i = 0
        row = []
        for each_data in row_data:
            td_txt = each_data.text
            row.append(td_txt)
            # if td_txt:
            #     row = row + dummy
            if td_txt:
                length = len(map_of_header_sets[i+1]) - 1
                dummy = _prepare_dummy(length)
                row = row + dummy
                i += 1

        length = len(mydata)
        mydata.loc[length] = row


def _prepare_dummy(length):
    dummy = []
    i = 0
    while i < length:
        i += 1
        dummy.append(',')
    return dummy


def last_day_of_month(any_day):
    # this will never fail
    # get close to the end of the month for any day, and add 4 days 'over'
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # subtract the number of remaining 'overage' days to get last day of current month, or said programattically said, the previous day of the first of next month
    return next_month - datetime.timedelta(days=next_month.day)


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


def get_file_name_along_with_absolute_path(base_path, dir_name, file_name):
    file_name_along_with_full_path = base_path + '/' + dir_name + '/' + file_name
    return file_name_along_with_full_path


def is_week_day(for_date):
    day_num_starts_from_monday_and_value_is_0 = for_date.weekday()
    return day_num_starts_from_monday_and_value_is_0 < 5


print('START')
# try:
#     download_main()
# except Exception as e:
#     print(e)
download_main()
