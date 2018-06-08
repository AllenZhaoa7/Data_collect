#!/usr/bin/python
import pyodbc
import threading
#import pandas as pd
import csv
import re
import json
import sys, getopt
import xlrd
import argparse
import os
import traceback

reload(sys)
sys.setdefaultencoding('utf-8')

connectdrive = 'DRIVER={SQL Server};SERVER=CTD-RADAR-STG.corp.emc.com\STAGING;DATABASE=Remedy;UID=reporter;PWD=reporter'
du_list = ['OFFLINE', 'OFF LINE', 'BLOCKED THREAD', 'RECOVERY', 'INACCESSIBLE', ' DU ']
sp_fault_list = ['REBOOT', 'PANIC']

# process to handle the CI list in number of threads
def process(items, cursor, ci_list, start, end):
    for item in items[start:end]:
        try:
            cihandler(item,cursor,ci_list)
        except Exception as ex:
            tb = traceback.format_exc()
            print(ex)
            print(tb)


# split the CI list to sub group for multiprocessing
def split_processing(items, cursor, ci_list, num_splits=10):
    split_size = len(items) // num_splits
    threads = []
    for i in range(num_splits):
        # determine the indices of the list this thread will handle
        start = i * split_size
        # special case on the last chunk to account for uneven splits
        end = None if i + 1 == num_splits else (i + 1) * split_size
        # create the thread
        threads.append(threading.Thread(target=process, args=(items, cursor, ci_list, start, end)))
        threads[-1].start()  # start the thread we just created

        # wait for all threads to finish
        for t in threads:
            t.join()


# get required information for each CI
def cihandler(item, cursor, ci_list):
    cirecord_dict = dict()
    duflag = 0
    spfaultflag = 0

    print ("Processing the CI " + item.ENTRY_ID + "\n")
    summary = str(item.SUMMARY)
    # if ("OFFLINE" in summary.upper()) or ("OFF LINE" in summary.upper()) \
    #        or ("INACCESSIBLE" in summary.upper()
    #        or ("BLOCKED THREAD")):
    #    duflag = 1

    if any(du_word in summary.upper() for du_word in du_list):
        duflag = 1

    if any(sp_fault_word in summary.upper() for sp_fault_word in sp_fault_list):
        spfaultflag = 1

    cirecord_dict = {
        "CI": str(item.ENTRY_ID),
        "AR": "",
        "Score": 1,
        "DU": duflag,
        "Summary": summary,
        "Product_Release": str(item.VERSION_FOUND),
        "Product_Code": mapproductrelease(str(item.VERSION_FOUND)),
        "Priority": str(item.PRIORITY),
        "Status": str(item.STATUS),
        "Parent_AR": "",
        "Major_Area": "",
        "Product_Area": ""
    }

    getrelatedarinfo(cursor, item.ENTRY_ID, cirecord_dict)
    if 1 == cirecord_dict["DU"]:
        cirecord_dict["Score"] *= 50

    if 1 == spfaultflag:
        cirecord_dict["Score"] *= 5

    if cirecord_dict["Major_Area"] == "":
        cirecord_dict["Major_Area"] = item.MAJOR_AREA

    if cirecord_dict["Major_Area"] == "EE Escalations":
        cirecord_dict["Major_Area"] == "EE"

    if cirecord_dict["Product_Area"] == "":
        cirecord_dict["Product_Area"] = item.PRODUCT_AREA

    ci_list.append(cirecord_dict)

# rank the AR provided from the File Tiger team excel file
def rankfromexcel(excelfilepath, cursor):
    file_path = excelfilepath
    if not os.path.exists(str(file_path)):
        print ('The file ' + file_path + ' not exists.')
        cursor.close()
        del cursor
        exit(2)

    workbook = xlrd.open_workbook(file_path)
    sheet = workbook.sheet_by_name('Dave W FS Field Events')

    ARlist = []

    for value in sheet.col_values(2):
        ARlist.append(str(value)[:6])

# remove the first two description lines
    ARlist.pop(0)
    ARlist.pop(0)

    print (ARlist)

    # get all related bug ARs
    query = 'SELECT i.ENTRY_ID, i.PRIORITY, i.SUMMARY, i.VERSION_FOUND, i.MAJOR_AREA, i.PRODUCT_AREA FROM Issue i ' \
    'WHERE i.ENTRY_ID in (%s) ' % ','.join(map(str, ARlist))
    cursor.execute(query)
    items = cursor.fetchall()
    ci_list = []
    split_processing(items, cursor, ci_list)

    keys = ci_list[0].keys()
    with open('../Customer_Incidents_From_Excel.csv', 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(ci_list)

# rank the AR from Remedy
def rankfromremedy(cursor):

    # get all fixed unity customer incidents
    cursor.execute("SELECT i.ENTRY_ID, i.PRIORITY, i.SUMMARY, i.VERSION_FOUND, i.MAJOR_AREA, i.PRODUCT_AREA, i.STATUS "
                   "FROM Issue i WHERE i.TYPE = 'Customer Incident' "
                   "AND i.PRODUCT_RELEASE = 'Unity 4.x'")
    ci_list = []

    split_processing(cursor.fetchall(), cursor, ci_list)

    with open("Fixed_FEAP_Events.json", "w") as f:
        json.dump(ci_list, f)

    keys = ci_list[0].keys()
    with open('../Customer_Incidents.csv', 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(ci_list)


# combine and merge product release number to product release name
# 4.0.x to Thunderbird
# 4.1.x to Falcon
# 4.2.x to Merlin
# 4.3.x to Harrier
def mapproductrelease(product_release):
    pattern = re.compile("^(-)")
    if pattern.match(product_release):
        product_release = product_release[1:]

    pattern_thunderbird = re.compile("^(4.0)")
    pattern_falcon = re.compile("^(4.1)")
    pattern_merlin = re.compile("^(4.2)")
    pattern_harrier = re.compile("^(4.3)")

    if pattern_falcon.match(product_release):
        return "4.1.Falcon"

    if pattern_thunderbird.match(product_release):
        return "4.0.Thunderbird"

    if pattern_merlin.match(product_release):
        return "4.2.Merlin"

    if pattern_harrier.match(product_release):
        return "4.3.Harrier"

# get information for each bug AR based on
# 1. Product Area
# 2. Priority
# 3. Number of duplicates
# calculate the scores to the CI
def getinfosinglear(cursor, arid, ci_dict):
    cursor.execute("SELECT i.PRIORITY, i.CLASSIFICATION_CODES_VALUE, i.A__OF_DUPLICATES, i.MAJOR_AREA, i.PRODUCT_AREA, i.PRIME_BUG__ FROM ISSUE i "
                   "where i.ENTRY_ID = '" + arid + "'")
    for rows in cursor.fetchall():
        marea = str(rows.MAJOR_AREA)
        codevalue = str(rows.CLASSIFICATION_CODES_VALUE)
        parea = str(rows.PRODUCT_AREA) + " "
        parentar = str(rows.PRIME_BUG__)
        priority = float(rows.PRIORITY[-1:])

        if 0 == priority :
            priority = 0.1

        ci_dict["Product_Area"] += parea

        if ("Unique" == codevalue or "Parent" == codevalue):
            ci_dict["Parent_AR"] = arid
        elif ("None" != parentar):
            ci_dict["Parent_AR"] = parentar

        if rows.A__OF_DUPLICATES is None:
            rows.A__OF_DUPLICATES = 0

        ci_dict["Score"] += (float(rows.A__OF_DUPLICATES) / priority)

        if (marea == "") or (marea == "EE"):
            continue
        else:
            ci_dict["Major_Area"] = marea;
            break


# get the required information of the ARs of one customer incident
def getrelatedarinfo(cursor, CIid, CI_dict):
    sqlstate = "SELECT a.REQUEST_ID_2 FROM ASSOCIATION a where a.REQUEST_ID_1 = '" + CIid + "'"
    cursor.execute(sqlstate)
    for arids in cursor.fetchall():
        CI_dict["AR"] += str(arids.REQUEST_ID_2) + " "
        getinfosinglear(cursor, arids.REQUEST_ID_2,CI_dict);



def rankars(source, inputfile):
    # connect to the sql server of remedy
    cnxn = pyodbc.connect(connectdrive)
    cursor = cnxn.cursor()

    if (source == 'Remedy'):
        rankfromremedy(cursor)
    else:
        rankfromexcel(inputfile,cursor)

    # Close and delete cursor
    cursor.close()
    del cursor

    # Close Connection
    cnxn.close()


def main(argv):
    parser = argparse.ArgumentParser(description='Rank the Customer ARs either from Remedy or Excel file.')
    parser.add_argument('-s', '--source', help='Specify the source, Remedy or Excel', required=True)
    parser.add_argument('-i', '--importfile', help='Specify the file path when the source is Excel', required=False)
    args = parser.parse_args()
    if args.source not in ['Remedy', 'Excel']:
        parser.print_help()
        exit(2)
    elif (args.source == 'Excel') and (args.importfile is None):
        print ("Excel file not specified.")
        parser.print_help()
        exit(2)
    else:
        rankars(args.source, args.importfile)


if __name__ == "__main__":
   main(sys.argv[1:])