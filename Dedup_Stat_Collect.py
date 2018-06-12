import datetime
import re
import urllib
import pandas as pd
from functools import reduce
import multiprocessing as mp
import Queue
import threading
import time


def get_stat_url_dict(stat_files, log_url):
    stat_url_dict = {}

    for stat_file in stat_files:
        stat_file = stat_file[1:-1]
        stat_file_url = log_url + stat_file
        #        print "\n" + vbm_stat_file_url
        index = int(re.search(r'\d+', stat_file).group())
        stat_url_dict[index] = stat_file_url

    return stat_url_dict


def get_all_stat_urls(html_info, log_url):
    all_stat_url_dict = {}

    vbm_stat_files = re.findall(r'>\d+_vbm_stats_.*<', html_info)
    vbm_stat_files.pop(0)

    tdc_stat_files = re.findall(r'>\d+_tdc_dumpStats_.*<', html_info)
    tdc_stat_files.pop(0)

    ilc_stat_files = re.findall(r'>\d+_ilcstat_dumpStats_.*<', html_info)
    ilc_stat_files.pop(0)

    pfdc_stat_files = re.findall(r'>\d+_pfdc_dl_dumpStats_.*<', html_info)
    pfdc_stat_files.pop(0)

    vbm_stat_url_dict = get_stat_url_dict(vbm_stat_files, log_url)
    tdc_stat_url_dict = get_stat_url_dict(tdc_stat_files, log_url)
    ilc_stat_url_dict = get_stat_url_dict(ilc_stat_files, log_url)
    pfdc_stat_url_dict = get_stat_url_dict(pfdc_stat_files, log_url)

    all_stat_url_dict["VBM"] = vbm_stat_url_dict
    all_stat_url_dict["TDC"] = tdc_stat_url_dict
    all_stat_url_dict["ILC"] = ilc_stat_url_dict
    all_stat_url_dict["PFDC"] = pfdc_stat_url_dict

    return all_stat_url_dict


def get_vbm_stat(vbm_urls_dict):
    ild_stat_dict = {}

    for index, vbm_url in vbm_urls_dict.items():
        temp_sock = urllib.urlopen(vbm_url)
        temp_source = temp_sock.read()
        temp_sock.close()

        p = re.compile(r"ILD Stats \*+\nTotal Attempts: (\d+)")
        m = p.search(temp_source)
        ild_total = 0 if m is None else m.group(1)

        p1 = re.compile(r"Total deduped AUs: (\d+)")
        m1 = p1.search(temp_source)
        ild_match = 0 if m1 is None else m1.group(1)

        ild_stat_dict[index] = [ild_total, ild_match]

    return ild_stat_dict


def get_tdc_stat(tdc_urls_dict):
    new_sha_dict = {}

    for index, tdc_url in tdc_urls_dict.items():
        temp_sock = urllib.urlopen(tdc_url)
        temp_source = temp_sock.read()
        temp_sock.close()

        p = re.compile(r"-\sAddSucceeded\s+(\d+)")
        m = p.search(temp_source)
        new_sha_dict[index] = 0 if m is None else m.group(1)

    return new_sha_dict


def get_pfdc_stat(pfdc_urls_dict):
    bypass_dict = {}

    for index, pfdc_url in pfdc_urls_dict.items():
        temp_sock = urllib.urlopen(pfdc_url)
        temp_source = temp_sock.read()
        temp_sock.close()

        p = re.compile(r"Total bypassOK\s+(\d+)")
        m = p.search(temp_source)
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

    for index, ilc_url in ilc_urls_dict.items():
        temp_sock = urllib.urlopen(ilc_url)
        temp_source = temp_sock.read()
        temp_sock.close()

        p = re.compile(r"IlcTotalAUs::ILCStatDump:\s+(\d+)")
        m = p.search(temp_source)
        ilc_total = 0 if m is None else m.group(1)

        p1 = re.compile(r"IlcCmprLibTotalAUsFailedNoSaving::ILCStatDump:\s+(\d+)")
        m1 = p1.search(temp_source)
        ilc_no_saving = 0 if m1 is None else m1.group(1)

        p2 = re.compile(r"ILPDTotalComeIn::ILCStatDump:\s+(\d+)")
        m2 = p2.search(temp_source)
        ilpd_total = 0 if m2 is None else m2.group(1)

        p3 = re.compile(r"ILPDBitwiseCompareSuccess::ILCStatDump:\s+(\d+)")
        m3 = p3.search(temp_source)
        ilpd_match = 0 if m3 is None else m3.group(1)

        ratio_list = get_ilc_ratio_stat(temp_source)

        ilc_stat_dict[index] = [ilc_total, ilc_no_saving, ilpd_total, ilpd_match]
        ilc_stat_dict[index].extend(ratio_list)

    return ilc_stat_dict


def create_stat_dataframe(test_log_url, queue):
    sock = urllib.urlopen(test_log_url)
    html_source = sock.read()
    sock.close()

    all_stat_urls = get_all_stat_urls(html_source, test_log_url)

    vbm_df = pd.DataFrame(get_vbm_stat(all_stat_urls['VBM']).items(), columns=['index', 'ILD'])
    vbm_detailed_df = pd.DataFrame(vbm_df.ILD.values.tolist(), index=vbm_df.index)
    vbm_detailed_df.columns = ['ILDTotal', 'ILDMatch']
    vbm_detailed_df['index'] = vbm_df['index']

    ilc_df = pd.DataFrame(get_ilc_stat((all_stat_urls['ILC'])).items(), columns=['index', 'ILC'])
    ilc_detailed_df = pd.DataFrame(ilc_df.ILC.values.tolist(), index=ilc_df.index)
    ilc_detailed_df.columns = ['ILCTotal', 'ILCNoSaving', 'ILPDTotal', 'ILPDMatch', '0-10', '10-20', '20-30', '30-40',
                               '40-50', '50-60', '60-70', '70-80', '80-90', '90-100']
    ilc_detailed_df['index'] = ilc_df['index']

    tdc_df = pd.DataFrame(get_tdc_stat(all_stat_urls['TDC']).items(), columns=['index', 'NewSHA'])

    pfdc_df = pd.DataFrame(get_pfdc_stat(all_stat_urls["PFDC"]).items(), columns=['index', 'Bypass'])

    df_list = [vbm_detailed_df, ilc_detailed_df, tdc_df, pfdc_df]
    all_stat_df = reduce(lambda left, right: pd.merge(left, right, on='index'), df_list)
    all_stat_df.set_index('index', inplace=True)

    queue.put(all_stat_df)


def read_log_from_excel(filepath):
    df = pd.read_excel(filepath, header=None, sheet_name=0, usecols='C')
    stat_urls = df.values.T[0].tolist()
    return stat_urls


def log_process(log_item):
    print "Processing the log of: " + log_item

    match = re.search('\d{4}_\d{2}_\d{2}_\d{2}-\d{2}-\d{2}', log_item)
    test_date = datetime.datetime.strptime(match.group(), '%Y_%m_%d_%H-%M-%S')
    test_date_str = test_date.strftime('%Y_%m_%d_%H-%M-%S')

    spa_log_url = log_item + 'spa/'
    spb_log_url = log_item + 'spb/'

    sps_logs = [spa_log_url, spb_log_url]

    thread_list = []
    results = []
    queue = Queue.Queue()

    for sp_log in sps_logs:
        t = threading.Thread(target=create_stat_dataframe, args=[sp_log,queue])
        thread_list.append(t)

    for item in thread_list:
        item.start()
        item.join()
        results.append(queue.get())

    sps = ['spa', 'spb']
    writer = pd.ExcelWriter(test_date_str + '_output.xlsx')
    for sp, data in zip(sps, results):
        data.to_excel(writer, sp)

    writer.save()
    writer.close()


if __name__ == '__main__':
    st = time.time()
    url_list = []
    log_list_file = 'C:\Users\zhaoa7\Documents\TA\Nighthawk\Dedup\DedupAdvanceTest\status_log_list.xlsx'
    url_list = read_log_from_excel(log_list_file)

    log_pool = mp.Pool(processes=len(url_list))
    log_pool.map(log_process, url_list)

    et = time.time()
    total_time = et - st
    print "Total time: " + str(total_time)


