#!/usr/bin/env python3
"""Transcode python streams

Usage:
  transcode.py inout <inpath> <outpath>
  transcode.py glob_base <glob_expr> <out_base>

Options:
  -h --help     Show this screen.
"""
from docopt import docopt

import pandas as pd
import os.path
import os
import struct
from glob import glob
from jsonlinesreader import JsonLinesReader
from fact.credentials import create_factdb_engine
from tqdm import tqdm

db = create_factdb_engine()
event_integer_value_order = [
    'Night',
    'Run',
    'Event',
    'Trigger',
]

NAMES = None

event_double_value_order = ['Az_deg', 'Zd_deg']


def main(inpath, outpath):
    night, run = night_run_from_inpath(inpath)
    runinfo = get_runinfo(night, run)

    os.makedirs(os.path.split(outpath)[0], exist_ok=True)
    with open(outpath, 'wb') as file:
        put_version_header(file)
        dump_runinfo_binary(runinfo, file)
        with JsonLinesReader(inpath) as infile:
            for event in infile:
                dump_event_binary(event, file)


def get_runinfo(night, run):
    return pd.read_sql(
        '''
        SELECT *
        FROM RunInfo
        WHERE
            fNight={night} AND
            fRunID={run}
        '''.format(night=night, run=run),
        con=db)


def put_version_header(file):
    file.write(bytes([0xfa, 0xc7, 0x01]))


def dump_runinfo_binary(runinfo, file):
    ''' write all entries from runinfo into file as 64bit 'd'oubles'''
    d = {}
    for n in runinfo.columns:
        x = runinfo.iloc[0][n]

        if isinstance(x, pd.tslib.Timestamp):
            x = x.asm8.astype('f8')
        try:
            d[n] = float(x)
        except:
            pass

    names = sorted(d.keys())

    global NAMES
    if NAMES is None:
        NAMES = set(names)
    else:
        if not set(names) == NAMES:
            raise "OMG"

    values = [d[name] for name in names]

    _bytes = struct.pack('d'*len(values), *values)
    file.write(_bytes)


def dump_event_binary(event, file):
    integer_values = [event[name] for name in event_integer_value_order]
    file.write(struct.pack('i'*len(integer_values), *integer_values))

    double_values = [event[name] for name in event_double_value_order]
    file.write(struct.pack('d'*len(double_values), *double_values))

    unixtime = event['UnixTime_s_us'][0] + event['UnixTime_s_us'][1]/1e6
    file.write(struct.pack('d', unixtime))

    dump_arrival_times(event['PhotonArrivals_500ps'], file)
    dump_saturated_pixel(event['SaturatedPixels'], file)


def dump_arrival_times(s, file):
    total_number_of_entries = sum(len(x) for x in s)  # 1byte per entry
    number_of_sublists = len(s)   # 2bytes before each sublist
    total_legth = (
        total_number_of_entries * 1 +
        number_of_sublists * 2
    )
    file.write(struct.pack('I', total_legth))
    for sublist in s:
        file.write(struct.pack('H', len(sublist)))
        file.write(struct.pack('B'*len(sublist), *sublist))


def dump_saturated_pixel(s, file):
    file.write(struct.pack('H', len(s)))
    file.write(struct.pack('H'*len(s), *s))


def make_path(night, run, base, ext):
    year = night[0:4]
    month = night[4:6]
    day = night[6:8]
    filename = "{night}_{run}{ext}".format(night=night, run=run, ext=ext)
    return os.path.join(base, year, month, day, filename)


def night_run_from_inpath(p):
    filename = os.path.split(p)[1]
    basename = filename.split('.')[0]
    night = basename[0:8]
    run = basename[9:12]
    return night, run

if __name__ == '__main__':
    arguments = docopt(__doc__, version='Transcode')
    print(arguments)
    if arguments['inout']:
        main(arguments['<inpath>'], arguments['<outpath>'])
    elif arguments['glob_base']:
        for inpath in sorted(glob(arguments['<glob_expr>'])):
            night, run = night_run_from_inpath(inpath)
            outpath = make_path(
                night, run, arguments['<out_base>'], ext='.phs.bin')
            main(inpath, outpath)


