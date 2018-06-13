import datetime
import re
import urllib
import pandas as pd
from functools import reduce
import multiprocessing as mp
from threading import Thread
import logging
import time


def get_stat_url_dict(stat_files, log_url):
    """
    The common func to get stat data dict with index
    :param stat_files:
    :param log_url:
    :return: stat_url_dict
    """
    stat_url_dict = {}

    # index comes from the data collect number at the beginning of each file
    for stat_file in stat_files:
        # remove invalid characters that get from the html source
        # i.e, >3_pfdc_dl_dumpStats_2018-06-08-09-54-17.txt<
        stat_file = stat_file[1:-1]
        stat_file_url = log_url + stat_file
        index = int(re.search(r'\d+', stat_file).group())
        stat_url_dict[index] = stat_file_url

    return stat_url_dict


def get_all_stat_urls(html_info, log_url):
    """
    Only get the valid individual data log file
    :param html_info:
    :param log_url:
    :return: stat urls for every interval collect point
    """
    all_stat_url_dict = {}

    vbm_stat_files = re.findall(r'>\d+_vbm_stats_.*<', html_info)
    # get rid of file 0_xxx, which is used to reset the stat data
    vbm_stat_files.pop(0)

    tdc_stat_files = re.findall(r'>\d+_tdc_dumpStats_.*<', html_info)
    tdc_stat_files.pop(0)

    ilc_stat_files = re.findall(r'>\d+_ilcstat_dumpStats_.*<', html_info)
    ilc_stat_files.pop(0)

    pfdc_stat_files = re.findall(r'>\d+_pfdc_dl_dumpStats_.*<', html_info)
    pfdc_stat_files.pop(0)

    # collect data into dict for each data type
    vbm_stat_url_dict = get_stat_url_dict(vbm_stat_files, log_url)
    tdc_stat_url_dict = get_stat_url_dict(tdc_stat_files, log_url)
    ilc_stat_url_dict = get_stat_url_dict(ilc_stat_files, log_url)
    pfdc_stat_url_dict = get_stat_url_dict(pfdc_stat_files, log_url)

    all_stat_url_dict["VBM"] = vbm_stat_url_dict
    all_stat_url_dict["TDC"] = tdc_stat_url_dict
    all_stat_url_dict["ILC"] = ilc_stat_url_dict
    all_stat_url_dict["PFDC"] = pfdc_stat_url_dict

    return all_stat_url_dict


def read_data_file(data_url, data_source_dict, index):
    """
    Get data source from url and append to the list
    :param data_url: data source html url
    :param data_source_dict: data source list for dataframe
    :param index: index for the dict
    """
    try:
        data_source = urllib.urlopen(data_url).read()
        logging.info("Requested..." + data_url)
        data_source_dict[index] = data_source
    except:
        logging.error('Error with URL check!')


def get_all_data_source(urls_dict):
    """
    Convert the data source url to the data source in parallel
    :param urls_dict: the dict to store data source urls with index
    :return: data_source_dict: the dict to store data source with index
    """
    threads = []
    data_sources = {}
    for i in range(len(urls_dict.keys())):
        process = Thread(target=read_data_file, args=[urls_dict[i+1], data_sources, i+1])
        process.start()
        threads.append(process)

    for process in threads:
        process.join()

    return data_sources


def get_vbm_stat(vbm_urls_dict):
    """
    For each vbm file, grab the ILD total and the ILD match data
    :param vbm_urls_dict:
    :return: ild_stat_dict
    """
    ild_stat_dict = {}

    source_data_dict = get_all_data_source(vbm_urls_dict)

    for index, vbm_source in source_data_dict.items():
        p = re.compile(r"ILD Stats \*+\nTotal Attempts: (\d+)")
        m = p.search(vbm_source)
        ild_total = 0 if m is None else m.group(1)

        p1 = re.compile(r"Total deduped AUs: (\d+)")
        m1 = p1.search(vbm_source)
        ild_match = 0 if m1 is None else m1.group(1)

        ild_stat_dict[index] = [ild_total, ild_match]

    return ild_stat_dict


def get_tdc_stat(tdc_urls_dict):
    new_sha_dict = {}

    source_data_dict = get_all_data_source(tdc_urls_dict)

    for index, tdc_source in source_data_dict.items():

        p = re.compile(r"-\sAddSucceeded\s+(\d+)")
        m = p.search(tdc_source)
        new_sha_dict[index] = 0 if m is None else m.group(1)

    return new_sha_dict


def get_pfdc_stat(pfdc_urls_dict):
    bypass_dict = {}

    source_data_dict = get_all_data_source(pfdc_urls_dict)

    for index, pfdc_source in source_data_dict.items():
        p = re.compile(r"Total bypassOK\s+(\d+)")
        m = p.search(pfdc_source)
        bypass_dict[index] = 0 if m is None else m.group(1)

    return bypass_dict


def get_ilc_ratio_stat(log_source):
    p = re.compile(r"RatioDist::ILCStatDump.*IlcCmprLibConsecutiveFailureDist:", re.DOTALL)
    m = p.search(log_source)
    sub_source = m.group(0)

    ilc_ratio_list = re.findall(r"(\d+)\n", sub_source)

    return ilc_ratio_list


def get_ilc_stat(ilc_urls_dict):
    ilc_stat_dict = {}

    source_data_dict = get_all_data_source(ilc_urls_dict)

    for index, ilc_source in source_data_dict.items():
        p = re.compile(r"IlcTotalAUs::ILCStatDump:\s+(\d+)")
        m = p.search(ilc_source)
        ilc_total = 0 if m is None else m.group(1)

        p1 = re.compile(r"IlcCmprLibTotalAUsFailedNoSaving::ILCStatDump:\s+(\d+)")
        m1 = p1.search(ilc_source)
        ilc_no_saving = 0 if m1 is None else m1.group(1)

        p2 = re.compile(r"ILPDTotalComeIn::ILCStatDump:\s+(\d+)")
        m2 = p2.search(ilc_source)
        ilpd_total = 0 if m2 is None else m2.group(1)

        p3 = re.compile(r"ILPDBitwiseCompareSuccess::ILCStatDump:\s+(\d+)")
        m3 = p3.search(ilc_source)
        ilpd_match = 0 if m3 is None else m3.group(1)

        ratio_list = get_ilc_ratio_stat(ilc_source)

        ilc_stat_dict[index] = [ilc_total, ilc_no_saving, ilpd_total, ilpd_match]
        ilc_stat_dict[index].extend(ratio_list)

    return ilc_stat_dict


def create_stat_dataframe(test_log_url):
    """
    For one stat collect log, there are 4 data types
    vbm, pfdc, tdc and vbm
    The stat data of each type will be collected separately,
    then combine together to have one overall dataframe
    :parameter: test_log_url
    :rtype: Pandas dataframe object
    :return: all_stat_df
    """
    sock = urllib.urlopen(test_log_url)
    html_source = sock.read()
    sock.close()

    # get every collect point file url for all 4 data types
    all_stat_urls = get_all_stat_urls(html_source, test_log_url)

    # get the ILD total data and ILD match data from vbm files
    vbm_df = pd.DataFrame(get_vbm_stat(all_stat_urls['VBM']).items(), columns=['index', 'ILD'])
    vbm_detailed_df = pd.DataFrame(vbm_df.ILD.values.tolist(), index=vbm_df.index)
    vbm_detailed_df.columns = ['ILDTotal', 'ILDMatch']
    vbm_detailed_df['index'] = vbm_df['index']

    # get the ILC total data, compress < 1%, ILPD total, ILPD match, compression ratio from ilc files
    ilc_df = pd.DataFrame(get_ilc_stat((all_stat_urls['ILC'])).items(), columns=['index', 'ILC'])
    ilc_detailed_df = pd.DataFrame(ilc_df.ILC.values.tolist(), index=ilc_df.index)
    ilc_detailed_df.columns = ['ILCTotal', 'ILCNoSaving', 'ILPDTotal', 'ILPDMatch', '0-10', '10-20', '20-30', '30-40',
                               '40-50', '50-60', '60-70', '70-80', '80-90', '90-100']
    ilc_detailed_df['index'] = ilc_df['index']

    # get the New SHA data from tdc file
    tdc_df = pd.DataFrame(get_tdc_stat(all_stat_urls['TDC']).items(), columns=['index', 'NewSHA'])

    # get the pfdc bypass data from the pfdc file
    pfdc_df = pd.DataFrame(get_pfdc_stat(all_stat_urls["PFDC"]).items(), columns=['index', 'Bypass'])

    # merge all dataframe into one
    df_list = [vbm_detailed_df, ilc_detailed_df, tdc_df, pfdc_df]
    all_stat_df = reduce(lambda left, right: pd.merge(left, right, on='index'), df_list)
    all_stat_df.set_index('index', inplace=True)

    return all_stat_df


def read_log_from_excel(file_path):
    """
    Get all the test stat data log urls from the excel file
    :param file_path:
    :return: stat_urls
    """
    df = pd.read_excel(file_path, header=None, sheet_name=0, usecols='C')
    if df.empty:
        print "Nothing in the spreadsheet, please check and run again."
        exit()
    stat_urls = df.values.T[0].tolist()
    return stat_urls


def log_multiprocess(url_item):
    """
    Process the stat data log of a test run
    The stat data will be collected from both spa and spb
    The stat data comes from multiple log files
    """

    # enable sub process creation for daemonic
    curr_proc = mp.current_process()
    curr_proc.daemon = False

    print "Processing the log of: " + url_item

    # get the timestamp of the test run log
    match = re.search('\d{4}_\d{2}_\d{2}_\d{2}-\d{2}-\d{2}', url_item)
    test_date = datetime.datetime.strptime(match.group(), '%Y_%m_%d_%H-%M-%S')
    test_date_str = test_date.strftime('%Y_%m_%d_%H-%M-%S')

    spa_log_url = url_item + 'spa/'
    spb_log_url = url_item + 'spb/'
    sps_logs = [spa_log_url, spb_log_url]

    # create the Pandas dataframe to store all stat data for a single sp
    start_time = time.time()
    pool = mp.Pool(processes=2)
    results = pool.map(create_stat_dataframe, sps_logs)
    end_time = time.time()
    print (end_time - start_time)

    # save the dataframe to excel file with two SPs as the sheet name
    sps = ['spa', 'spb']
    writer = pd.ExcelWriter(test_date_str + '_output.xlsx')
    for sp, data in zip(sps, results):
        data.to_excel(writer, sp)
    writer.save()
    writer.close()


if __name__ == '__main__':
    st = time.time()
    url_list = []
    # multiple test stat log url will be put to the excel file in fixed format
    log_list_file = 'status_log_list.xlsx'
    url_list = read_log_from_excel(log_list_file)

    log_pool = mp.Pool(processes=len(url_list))
    log_pool.map(log_multiprocess, url_list)

    et = time.time()
    total_time = et - st
    print "Total time: " + str(total_time)
