#!/usr/bin/env python3

import sys
import argparse
import numpy as np
import pandas as pd

# Units
second = 1
millisecond = 1e-3*second
microsecond = 1e-6*second
nanosecond = 1e-9*second
hour = 3600*second

byte = 1
kilobyte = 1024*byte
megabyte = 1024*kilobyte
gigabyte = 1024*megabyte

watt = 1
joule = watt*second

def read_cmdline():
    parser = argparse.ArgumentParser()
    parser.add_argument('log', help="log file from fiobatch")
    parser.add_argument('fio', help="fio output CSV file")
    parser.add_argument('hobo', help="HOBO measurements CSV file")
    parser.add_argument('--output', '-o', required=True, help="write aggregation (e.g. mean of replicates) CSV to file")
    parser.add_argument('--all', '-a', help="write joined CSV to file")
    parser.add_argument('--hoboshift', metavar='SECONDS', type=float, default=None, help='shift HOBO data by adding SECONDS to HOBO timestamps')
    return parser.parse_args()

verbose = lambda *args, **kwargs: print(*args, file=sys.stderr, **kwargs)

def read_hobo(hobo_csv):
    '''
    Read power measurements from HOBO CSV file (not to be confused
    with .hobo proprietary binary format)
    '''
    if hobo_csv is None:
        verbose('No HOBO CSV file given, skipping')
        return None

    # Read only headers to get time zone information
    verbose('Checking headers in', hobo_csv)
    hobo_headers = pd.read_csv(hobo_csv, index_col=0, skiprows=1, nrows=0)
    try:
        dt_column = [c for c in hobo_headers.columns if c.startswith('Date Time')][0]
    except IndexError:
        print('No "Date Time" column found in HOBO data: ' + str(hobo_csv),
              *hobo_headers, sep='\n\t', file=sys.stderr)
        sys.exit(3)
    try:
        tz = dt_column.split(', ')[1]
        prefix = 'GMT'
        if tz.startswith(prefix): tz = tz[len(prefix):]
        verbose('\tFound HOBO time zone:', tz)
    except IndexError:
        tz = ''

    verbose('HOBO features:', sep='\n\t', *list(hobo_headers.columns))

    # Read entire CSV, parsing "Date Time" column as datetime timestamp
    verbose("Reading", hobo_csv)
    hobo = pd.read_csv(hobo_csv, index_col=dt_column, skiprows=1,
                       usecols=lambda c: c !='#',
                       parse_dates=[dt_column], date_parser=lambda c: pd.to_datetime(c + ' ' + tz, utc=True))

    # Simplify column names (drops serial number)
    #print('Using index:', hobo.index.name, 'as', hobo_time)
    hobo.index.name = 'time'
    hobo.columns = [col.split(',', 1)[0] for col in hobo.columns]
    if cmdline.hoboshift:
        hobo.index += pd.Timedelta(cmdline.hoboshift, unit='second')
    return hobo


def read_log(logfile):
    verbose("Reading", logfile)
    data = {'batchid': [], 'start': [], 'stop': [], 'params': []}
    with open(logfile, 'rt') as log:
        batchid = None
        start_time = None
        start_params = None
        for line in log.readlines():
            fields = line.split('|')
            if len(fields) == 5:
                timestamp = pd.Timestamp(fields[0])
                batchid = int(fields[3].split('=')[1])
                params = fields[4].strip()
                if fields[2] == 'start':
                    if start_time is None or start_params is None or batchid is None:
                        start_time = timestamp
                        start_params = params
                    else:
                        verbose("Unexpected 'start' after", str(params))
                elif fields[2] == 'stop':
                    if params == start_params:
                        data['batchid'].append(batchid)
                        data['start'].append(start_time)
                        data['stop'].append(timestamp)
                        data['params'].append(params)
                        batchid = None
                        start_time = None
                        start_params = None
                    else:
                        verbose("Mismatched 'stop' line for", str(params))
                else:
                    verbose("Unrecognized event \"" + fields[2] + "\", skipping")
    parsed = []
    for p in data['params']:
        d = { k: v for k, v in [pair.split('=', 1) for pair in p.split(', ')] }
        parsed.append(d)
    data['params'] = parsed
    return pd.DataFrame(data)


def same(col):
    ''''''
    if col.nunique() > 1:
        warning('Expected same values, but found different values in', col.name)
    return col.iloc[0]

fio_agg = {
    **dict.fromkeys([
        'terse_version_3',
        'fio_version',
        'groupid',
        'disk_name',
        'disk_read_iops',
        'disk_write_iops',
        'disk_read_merges',
        'disk_write_merges',
        'disk_read_ticks',
        'write_ticks',
        'disk_queue_time',
        'disk_util',
        'device',
        'iodepth',
        'bs',
        'rw',
        'stopcond',
        'numjobs',
    ], lambda c: same(c)),
    **dict.fromkeys([
        'error',
        'read_kb',
        'read_bandwidth_kb',
        'read_iops',
        'write_kb',
        'write_bandwidth_kb',
        'write_iops',
        'cpu_user',
        'cpu_sys',
        'cpu_csw',
        'cpu_mjf',
        'cpu_minf',
        'iodepth_1',
        'iodepth_2',
        'iodepth_4',
        'iodepth_8',
        'iodepth_16',
        'iodepth_32',
        'iodepth_64',
        'lat_2us',
        'lat_4us',
        'lat_10us',
        'lat_20us',
        'lat_50us',
        'lat_100us',
        'lat_250us',
        'lat_500us',
        'lat_750us',
        'lat_1000us',
        'lat_2ms',
        'lat_4ms',
        'lat_10ms',
        'lat_20ms',
        'lat_50ms',
        'lat_100ms',
        'lat_250ms',
        'lat_500ms',
        'lat_750ms',
        'lat_1000ms',
        'lat_2000ms',
        'lat_over_2000ms',
    ], 'sum'),
    **dict.fromkeys([
        'read_runtime_ms',
        'read_slat_mean',
        'read_slat_dev',
        'read_clat_mean',
        'read_clat_dev',
        'read_lat_mean',
        'read_lat_dev',
        'read_bw_agg_pct',
        'read_bw_mean',
        'read_bw_dev',
        'write_runtime_ms',
        'write_slat_mean',
        'write_slat_dev',
        'write_clat_mean',
        'write_clat_dev',
        'write_lat_mean',
        'write_lat_dev',
        'write_bw_agg_pct',
        'write_bw_mean',
        'write_bw_dev',
    ], 'mean'),
    **dict.fromkeys([
        'read_slat_min',
        'read_clat_min',
        'read_tlat_min',
        'read_bw_min',
        'write_slat_min',
        'write_clat_min',
        'write_tlat_min',
        'write_bw_min',
    ], 'min'),
    **dict.fromkeys([
        'read_slat_max',
        'read_clat_max',
        'read_lat_max',
        'read_bw_max',
        'write_slat_max',
        'write_clat_max',
        'write_lat_max',
        'write_bw_max',
    ], 'max'),
    **dict.fromkeys([
        'read_clat_pct01',
        'read_clat_pct02',
        'read_clat_pct03',
        'read_clat_pct04',
        'read_clat_pct05',
        'read_clat_pct06',
        'read_clat_pct07',
        'read_clat_pct08',
        'read_clat_pct09',
        'read_clat_pct10',
        'read_clat_pct11',
        'read_clat_pct12',
        'read_clat_pct13',
        'read_clat_pct14',
        'read_clat_pct15',
        'read_clat_pct16',
        'read_clat_pct17',
        'read_clat_pct18',
        'read_clat_pct19',
        'read_clat_pct20',
        'write_clat_pct01',
        'write_clat_pct02',
        'write_clat_pct03',
        'write_clat_pct04',
        'write_clat_pct05',
        'write_clat_pct06',
        'write_clat_pct07',
        'write_clat_pct08',
        'write_clat_pct09',
        'write_clat_pct10',
        'write_clat_pct11',
        'write_clat_pct12',
        'write_clat_pct13',
        'write_clat_pct14',
        'write_clat_pct15',
        'write_clat_pct16',
        'write_clat_pct17',
        'write_clat_pct18',
        'write_clat_pct19',
        'write_clat_pct20',
    ], 'sum'),
}


def convert_percentages(df):
    '''
    Looks for columns that appear to be percentages and converts them
    to floats
    '''
    for col in df.dtypes[df.dtypes == object].index:
        if not all(df[col].str.endswith('%')):
            continue
        try:
            df[col] = df[col].str.rstrip('%').astype(float)/100.0
            verbose('Converted', col, 'from percentage to float')
        except:
            warning('Cannot convert column', col, 'from percentage to float')
            continue
    return df

def convert_histogram(df):
    '''
    Looks for columns of the format "percentage = count" and converts
    them to tuples, parsing the numeric values
    '''
    import re
    for col in df.dtypes[df.dtypes == object].index:
        if not all(df[col].str.contains('=')):
            continue

        # Split into left and right fields
        fields = df[col].str.split('=', 1, expand=True)

        # Convert left percentage
        if all(fields[0].str.endswith('%')):
            fields[0] = fields[0].str.rstrip('%').astype(float)/100.0
        else:
            fields[0] = fields[0].astype(float)

        # Right expected to be a count
        fields[1] = fields[1].astype(int)

        # Drop column if both sides are all zeros
        if all(fields[0] == 0) and all(fields[1] == 0):
            df = df.drop(col, axis=1)
            #del fio_agg[col]
            verbose('Dropped column', col)
            continue
            
        # If left side is the same, we can drop it, appending to column name
        if fields[0].nunique() == 1:
            df[col] = fields[1]
            match = re.search(r'^(.+)_pct\d\d$', col)
            newname = '%s_%gp' % (match.group(1) if match else col, 100*fields[0].iloc[0])
            if newname not in df.columns:
                fio_agg[newname] = fio_agg[col]
                #del fio_agg[col]
                df = df.rename(columns={col: newname})
                col += ' -> ' + newname
            verbose('Converted', col, 'from histogram to int')
        else:
            df[col] = list(zip(fields[0], fields[1]))
            verbose('Converted', col, 'from histogram to tuple')
    return df


def read_fio(filename):
    verbose("Reading", filename)
    df = pd.read_csv(cmdline.fio, sep=';')
    df = convert_percentages(df)
    df = convert_histogram(df)
    return df


def agg_rows(df, key_cols):
    keys = key_cols if 'jobname' in key_cols else key_cols + ['jobname']
    not_key_cols = [c for c in df.columns if c not in keys]
    return df.groupby(keys).agg({k: v for k, v in fio_agg.items() if k in not_key_cols}).reset_index()


def main():
    global cmdline
    events = read_log(cmdline.log)
    hobo = read_hobo(cmdline.hobo)
    fio = read_fio(cmdline.fio)

    data = []
    key_cols = set()
    for row in events.itertuples():
        x = row.params
        key_cols.update(x.keys())

        start = row.start
        if 'ramp_time' in x:
             start += pd.Timedelta(float(x['ramp_time']), 's')

        interval = hobo.loc[start:row.stop]

        x['batchid'] = row.batchid
        x['start'] = row.start
        x['stop'] = row.stop
        x['duration'] = row.stop - row.start

        # Number of HOBO measurements taken
        x['num_samples'] = len(interval)

        # 'Active Power' is in watts, mean over interval
        x['watts_mean'] = interval['Active Power'].mean()*watt

        # 'Active Energy' is in Wh, sum and convert to joules
        # (This HOBO column can be troublesome; do not use)
        #x['joules'] = 3600*interval['Active Energy'].sum()

        data.append(x)

    if not key_cols.issubset(set(fio.columns)):
        raise ValueError("Key columns not found in fio data")
    
    df = pd.DataFrame(data)
    if 'ramp_time' in df:
        print("Using ramp_time of", *df['ramp_time'].unique(), "seconds", file=sys.stderr)

    key_cols = list(key_cols)
    verbose("Key columns are:\n", key_cols)
    df[key_cols] = df[key_cols].astype(fio[key_cols].dtypes)

    fio = agg_rows(fio, key_cols)
    joined = df.join(fio.set_index(key_cols), on=key_cols)

    # Calculated columns 
    joined['iops'] = (joined['read_iops'] + joined['write_iops'])*(1/second)
    joined['bandwidth'] = (joined['read_bandwidth_kb'] + joined['write_bandwidth_kb'])*kilobyte
    joined['iopj'] = joined['iops']/joined['watts_mean']
    joined['bpj'] = joined['bandwidth']/joined['watts_mean']

    if cmdline.all:
        print("Writing", cmdline.all, file=sys.stderr)
        joined.to_csv(cmdline.all, index=False)

    if cmdline.output:
        key_cols.remove('replicate')
        grouping = joined.groupby(key_cols)
        means = grouping.mean().add_suffix('__mean')
        medians = grouping.median().add_suffix('__median')
        stds = grouping.std().add_suffix('__std')
        a = pd.concat([means, medians, stds], axis=1)
        print("Writing", cmdline.output)
        a.to_csv(cmdline.output)


if __name__ == '__main__':
    global cmdline
    cmdline = read_cmdline()
    main()
#EOF
