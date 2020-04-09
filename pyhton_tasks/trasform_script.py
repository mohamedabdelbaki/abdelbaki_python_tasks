import argparse
from pathlib import Path
import os
import fnmatch
import pandas as pd
import numpy as np
import json
import re
from decimal import Decimal
import time
import sys
from subprocess import PIPE, Popen

# -----------------------------------------------------functions----------------------------------------------------

# ----------retrive json files from directory------------

def get_list_of_json_files(dir_name) :
    list_of_files = []
    for element in os.listdir(dir_name):
        if fnmatch.fnmatch(element, '*.json'):
            list_of_files.append(element)

    return list_of_files


# ---------------------------trasformations--------------------------------------

# ---------data frame with unix time stamp (flag) -----------

def transform_json_to_dframe(file_path, flag):
    records = []

    with open(file_path) as f:
        for line in f:
            lineDict = json.loads(line)
            if '_heartbeat_' in lineDict.keys() or 'kw' in lineDict.keys():
                lineDict['_heartbeat_'] = np.nan
                lineDict['kw'] = np.nan
                records.append(lineDict)
            else:
                records.append(lineDict)
    
    initial_frame = pd.DataFrame(records)

    initial_frame.dropna(axis=1,inplace=True,how='all')
    initial_frame.dropna(axis=0,inplace=True,how='all')

    column_names = ['web_browser', 'operating_sys', 'from_url', 'to_url', 'city', 'longitude', 'latitude', 'time_zone',
                    'time_in', 'time_out']
    prepare_frame = pd.DataFrame(columns=column_names)

    prepare_frame['web_browser'] = pd.Series([x.split()[0] for x in initial_frame['a'].values])
    prepare_frame.head()

    def extract_os(s):
        word = re.search(r"\((.*?)\)*;|\((.*?)\)", str(s))
        if word:
            return word.group(0).replace("(", "").replace(";", "").replace(")", "")
        else:
            return None

    prepare_frame['operating_sys'] = initial_frame['a'].apply(extract_os)

    def extract_url(s):
        if s is np.nan:
            return None
        else:
            word = s.split("//")[-1].split("/")[0]
            return word

    prepare_frame['from_url'] = initial_frame['r'].apply(extract_url)
    prepare_frame['to_url'] = initial_frame['u'].apply(extract_url)
    prepare_frame['city'] = initial_frame['cy']

    def extract_long(s):
        word = re.search(r"[^[]*\[([^]]*)\]", str(s))
        if word:
            return Decimal(word.group(1).split(',')[0].strip(' '))
        else:
            return None

    prepare_frame['longitude'] = initial_frame['ll'].apply(extract_long)

    def extract_lat(s):
        word = re.search(r"[^[]*\[([^]]*)\]", str(s))
        if word:
            return Decimal(word.group(1).split(',')[-1].strip(' '))
        else:
            return None

    prepare_frame['latitude'] = initial_frame['ll'].apply(extract_lat)

    prepare_frame['time_zone'] = initial_frame['tz'].apply(lambda x: None if x == "" else x)
    prepare_frame['time_in'] = initial_frame['t'].values
    prepare_frame['time_out'] = initial_frame['hc'].values

    prepare_frame.dropna(axis=0, inplace=True)

    if flag:
        return convert_timestamp(prepare_frame)

    return prepare_frame


# --------------data frame with converted time stamp---------------------
def convert_timestamp(data_frame):
    time_in = []
    time_out = []
    for i, row in data_frame.iterrows():
        stamp_in = pd.to_datetime(row['time_in'], unit='s').tz_localize(row['time_zone']).tz_convert('UTC')
        stamp_out = pd.to_datetime(row['time_out'], unit='s').tz_localize(row['time_zone']).tz_convert('UTC')

        time_in.append(stamp_in)
        time_out.append(stamp_out)

    data_frame['time_in'] = time_in
    data_frame['time_out'] = time_out

    return data_frame


# ------------------------------#---------------------------#-----------------------------#----------------------#-------------------
start = time.time()

checksums = {}

parser = argparse.ArgumentParser()

parser.add_argument("dir_name", help="enter the directory path of json files ")

parser.add_argument("-u", "--convert-unixformat", action="store_true", dest="convert_unix_format", default=False,
                    help="maintain the UNIX format of time stamp (if not passed, time stamp will be converted)")

args = parser.parse_args()

p = Path('target/')
p.mkdir(exist_ok=True)

if os.path.exists(args.dir_name):
    list_of_files = get_list_of_json_files(args.dir_name)
    if len(list_of_files) == 0:
        print('no json files found in this dir {}'.format(args.dir_name))

    else:
        for f in list_of_files:
            file_path = args.dir_name+'/'+f
            #print(file_path)
            with Popen(["md5sum", file_path], stdout=PIPE) as proc:
                checksum = proc.stdout.read().split()[0]

                # ---------- check for dublicate files ----------
                if checksum in checksums:
                    same_file = checksums[checksum]
                    print(f"file >>>>> {f} <<<<< is dublicates of >>>>> {same_file} <<<<< and will not be transformed")

                else:
                    checksums[checksum] = f
                    if args.convert_unix_format:
                        df = transform_json_to_dframe(file_path, True)
                        df.to_csv('target/' + f.split()[0] + '.csv', sep=',',encoding='utf-8', index=False)
                        print('the number of rows transformed is >>>>> {} <<<<< and the path of this file is >> {} <<'.format(len(df.index), 'target/' + f.split()[0] + '.csv'))
                    else:
                        df = transform_json_to_dframe(file_path, False)
                        df.to_csv('target/' + f.split()[0] + '.csv', sep=',',encoding='utf-8', index=False)
                        print('the number of rows transformed is >>>>>> {} <<<<<< and the path of this file is >> {} <<'.format(len(df.index), 'target/' + f.split()[0] + '.csv'))

    execution_time = (time.time() - start)
    print("the total excution time is >> {} << ".format(execution_time))

else:
    print('no such dir {} ,make sure of name and try again'.format(args.dir_name))
    sys.exit()
