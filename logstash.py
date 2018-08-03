import argparse
import json
import os
import sys
import traceback
from glob import glob
from os.path import basename, join

import dateutil.parser
import pandas as pd
from tqdm import tqdm
from urllib.parse import unquote
import gzip


def barcode_records(message, fname, date, request=True):
    recs = []

    body = message.get('body')
    if type(body) != dict:
        return recs
    if request:
        bcs = body.get('barcodes', [])
        for bc in bcs:
            rec = {'file': fname, 'barcode': bc, 'date': date, 'requested': 1}
            recs.append(rec)
    else:
        noms = body.get('nomenclatures', [])
        for nom in noms:
            bcs = nom.get('barcodes')
            for bc in set(bcs):
                rec = {'file': fname, 'barcode': bc, 'date': date, 'found': 1}
                recs.append(rec)
            if len(bcs) == 0:
                rec = {'file': fname, 'date': date, 'found': 1}
                recs.append(rec)

    return recs


def full_stat(record, parent_key=''):
    rec = {}

    def construct_key(keys):
        if keys[0] == '':
            keys = keys[1:]
        keys = [k for k in keys if k != 'body']
        return '_'.join(keys)

    for k, v in record.items():
        new_key = construct_key([parent_key, k])
        if isinstance(v, dict):
            rec.update(full_stat(v, new_key))
        elif isinstance(v, list):
            if k == 'barcodes':
                if len(v) == 1:
                    rec[new_key] = v[0]
                else:
                    rec[new_key] = v
            else:
                rec[new_key + '_' + 'len'] = len(v)
        else:
            if k == 'query' and type(v) == str:
                v = unquote(v)
            if v:
                rec[new_key] = v
    return rec


def lookup_master(barcodes):
    from pydev import utils

    master = utils.load_master()
    db = master['Database']
    if type(db) == list:
        db = db[0]

    master_barcodes = {bc for et in db['etalons'] for bc in
                       et.get('barcodes', [])}
    not_in_master = set(barcodes) - set(master_barcodes)
    return not_in_master


def parse_line(line, lnumber):
    recs = []
    try:
        record = json.loads(line, encoding='utf8')
    except:
        print('on %s' % fname)
        traceback.print_exc(file=sys.stdout)
        print('line number %d' % (lnumber + 1))
        return

    ts = dateutil.parser.parse(record['@timestamp'])
    date = str(ts.date())
    base_name = basename(fname)

    if args.full:
        try:
            rec = full_stat(record)
            rec.update({'date': date})
            for k in ['@timestamp', 'appName', 'request_time',
                      'response_time', '@version', 'host',
                      'geoip_continent_code', 'geoip_ip',
                      'geoip_country_code2', 'geoip_latitude',
                      'geoip_country_code3',
                      'geoip_location_lat',
                      'geoip_location_lon', 'geoip_longitude',
                      'geoip_postal_code', 'geoip_region_code',
                      'geoip_region_name', 'geoip_timezone',
                      'user_ip']:
                rec.pop(k, None)
            recs.append(rec)
        except:
            traceback.print_exc(file=sys.stdout)
            print('on %s' % fname)
            print('line number %d' % (lnumber + 1))
            sys.exit(1)
    else:
        req_recs = barcode_records(record['request'],
                                   base_name, date,
                                   request=True)
        recs += req_recs
        if len(req_recs):
            res_recs = barcode_records(
                record['response'], base_name, date,
                request=False)
            if len(req_recs) == 1:
                bc = req_recs[0]['barcode']
                for rec in res_recs:
                    if 'barcode' not in rec:
                        rec['barcode'] = bc
            recs += res_recs

    return recs


def parse_file(fname):
    recs = []
    with gzip.open(fname, 'r') as f:
        for lnumber, line in enumerate(f):
            recs += parse_line(line, lnumber)
    return recs


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Extract statistics from logs.')
    parser.add_argument('log_dir', metavar='log_dir', type=str,
                        help='directory with *.log files')
    parser.add_argument('--save_dir', metavar='save_dir', type=str,
                        help='directory to save statistics in, '
                             '"log_dir" by default',
                        default='')
    parser.add_argument('--full', action='store_true')

    args = parser.parse_args()
    log_dir = args.log_dir
    save_dir = args.save_dir
    if not save_dir:
        save_dir = log_dir

    error = False
    if not os.path.isdir(log_dir):
        print('"%s" is not a directory' % log_dir, file=sys.stderr)
        error = True

    if log_dir != save_dir and not os.path.isdir(save_dir):
        print('"%s" is not a directory' % save_dir, file=sys.stderr)
        error = True

    if error:
        sys.exit(1)

    files = glob(join(log_dir, '*.log.gz'))
    if len(files) == 0:
        print("no *.log.gz files found in %s" % log_dir)
        error = True

    if error:
        sys.exit(1)

    save_name = 'stat' + ('_full' if args.full else '')

    excel_file = join(save_dir, save_name + '.xlsx')
    csv_file = join(save_dir, save_name + '.csv')

    if os.path.isfile(excel_file):
        try:
            with open(excel_file, "r+") as f:
                pass
        except IOError:
            print('could not open %s, please close it' % excel_file)
            error = True

    recs = []
    for fname in tqdm(files):
        try:
            recs += parse_file(fname)
        except EOFError:
            print('on %s' % fname)
            traceback.print_exc(file=sys.stdout)

    df = pd.DataFrame.from_records(recs)
    print(df.shape)

    if not len(df):
        print('no data found')
        sys.exit(0)

    if args.full:
        index = False
    else:
        df = df.groupby(['date', 'file', 'barcode']).sum()
        index = True
    try:
        df.to_excel(excel_file, engine='xlsxwriter',
                    encoding='utf8', index=index)
    except:
        print('could not save %s, please close it' % excel_file)
        traceback.print_exc(file=sys.stdout)
        sys.exit(1)

    df.to_csv(csv_file, encoding='utf8', sep='\t', index=index)

    print('files created:')
    print(excel_file)
    print(csv_file)

    # master_file = '../data/master/master-export.json'
    # barcodes = df['barcode'].unique()
    # not_in_master = lookup_master(df['barcodes'].unique(), master_file)

    # sub_df = df[df['barcode'].isin(not_in_master)]
    # grouped_master = sub_df.groupby(['date', 'file', 'barcode']).sum()
    # grouped_master.to_csv(data_dir + 'stat_master.csv', encoding='utf8')
