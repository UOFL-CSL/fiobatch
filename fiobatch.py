#!/usr/bin/env python3

import sys
import json
import logging
import argparse

from subprocess import Popen, PIPE, STDOUT, TimeoutExpired
from time import sleep

from string import Template
from itertools import product

class FioJob(object):
    fio_cmd = 'fio'
    fio_headers = 'terse_version_3;fio_version;jobname;groupid;error;read_kb;read_bandwidth_kb;read_iops;read_runtime_ms;read_slat_min_us;read_slat_max_us;read_slat_mean_us;read_slat_dev_us;read_clat_min_us;read_clat_max_us;read_clat_mean_us;read_clat_dev_us;read_clat_pct01;read_clat_pct02;read_clat_pct03;read_clat_pct04;read_clat_pct05;read_clat_pct06;read_clat_pct07;read_clat_pct08;read_clat_pct09;read_clat_pct10;read_clat_pct11;read_clat_pct12;read_clat_pct13;read_clat_pct14;read_clat_pct15;read_clat_pct16;read_clat_pct17;read_clat_pct18;read_clat_pct19;read_clat_pct20;read_tlat_min_us;read_lat_max_us;read_lat_mean_us;read_lat_dev_us;read_bw_min_kb;read_bw_max_kb;read_bw_agg_pct;read_bw_mean_kb;read_bw_dev_kb;write_kb;write_bandwidth_kb;write_iops;write_runtime_ms;write_slat_min_us;write_slat_max_us;write_slat_mean_us;write_slat_dev_us;write_clat_min_us;write_clat_max_us;write_clat_mean_us;write_clat_dev_us;write_clat_pct01;write_clat_pct02;write_clat_pct03;write_clat_pct04;write_clat_pct05;write_clat_pct06;write_clat_pct07;write_clat_pct08;write_clat_pct09;write_clat_pct10;write_clat_pct11;write_clat_pct12;write_clat_pct13;write_clat_pct14;write_clat_pct15;write_clat_pct16;write_clat_pct17;write_clat_pct18;write_clat_pct19;write_clat_pct20;write_tlat_min_us;write_lat_max_us;write_lat_mean_us;write_lat_dev_us;write_bw_min_kb;write_bw_max_kb;write_bw_agg_pct;write_bw_mean_kb;write_bw_dev_kb;cpu_user;cpu_sys;cpu_csw;cpu_mjf;cpu_minf;iodepth_1;iodepth_2;iodepth_4;iodepth_8;iodepth_16;iodepth_32;iodepth_64;lat_2us;lat_4us;lat_10us;lat_20us;lat_50us;lat_100us;lat_250us;lat_500us;lat_750us;lat_1000us;lat_2ms;lat_4ms;lat_10ms;lat_20ms;lat_50ms;lat_100ms;lat_250ms;lat_500ms;lat_750ms;lat_1000ms;lat_2000ms;lat_over_2000ms;disk_name;disk_read_iops;disk_write_iops;disk_read_merges;disk_write_merges;disk_read_ticks;write_ticks;disk_queue_time;disk_util'


    def __init__(self, fio_script, **kwargs):
        self.fio = fio_script
        self.cmd = [self.fio_cmd, '--output-format=terse', '-']
        self.success = None
        for k, v in kwargs.items():
            self.__dict__[k] = v


    def __str__(self):
        if 'mapping' in self.__dict__:
            return ', '.join('%s=%s' % (str(k), str(v)) for k, v in self.mapping.items())


    def run(self, extra=''):
        logger.debug("Fio input:\n" + self.fio)
        logger.info("start " + str(self))
        self.proc = Popen(self.cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, text=True)
        try:
            self.result = self.proc.communicate(input=self.fio, timeout=cmdline.timeout)
            logger.info("stop " + str(self))
        except TimeoutExpired:
            logger.error("Timeout waiting for " + str(self))
            self.success = False
            return

        if self.proc.returncode != 0:
            text = "Job return code is %d" % self.proc.returncode
            if self.result[0].strip() != '':
                text += '\n' + self.result[0]
            if self.result[1].strip() != '':
                text += '\n' + self.result[1]
            logger.error(text)
            self.success = False
            return

        text = self.result[0].rstrip('\n')
        text += extra + ';' + ';'.join(str(v) for v in self.mapping.values()) + '\n'
        cmdline.output.write(text)
        cmdline.output.flush()
        self.success = True


def read_params(filename):
    with open(filename, 'rt') as infile:
        params = json.load(infile)

    # JSON object must be dict
    if type(params) is not dict:
        logger.fatal("Dictionary expected for JSON parameter file, but found " + str(type(params)))

    fio_cols = set(FioJob.fio_headers.split(';'))

    if 'replicates' in params:
        if 'replicate' in params:
            logger.warning("Both 'replicates' and 'replicate' specified. Using 'replicates' value")
        if type(params['replicates']) is not int:
            logger.fatal("'replicates' must be an integer")
        params['replicate'] = list(range(1, params['replicates'] + 1))
        del params['replicates']

    elif 'replicate' in params:
        if type(params['replicate']) is not list:
            params['replicate'] = list(params['replicate'])

    if 'fio' in params and type(params['fio']) is dict:
        # Replace single values with singleton lists
        for k, v in params['fio'].items():
            if type(v) is not list:
                params['fio'][k] = [v]
            if k in fio_cols:
                logger.warning("Parameter %s conflicts with fio column. Consider renaming" % str(k))
    else:
        logger.fatal("Missing fio dict in parameter file")
    return params
        

def parse_cmdline():
    parser = argparse.ArgumentParser()
    parser.add_argument('parameters', metavar='JSON')
    parser.add_argument('fiofiles', metavar='FIO', nargs='+')
    parser.add_argument('--output', '-o', type=argparse.FileType('wt'), default=sys.stdout)
    parser.add_argument('--log', '-l')
    parser.add_argument('--verbose', '-v', action='count', default=0)
    parser.add_argument('--timeout', type=float)
    parser.add_argument('--cooldown', metavar='SECONDS', type=float, default=2, help="cool down time between jobs")
    parsed = parser.parse_args()

    parsed.parameters = read_params(parsed.parameters)
    parsed.fio_params = parsed.parameters['fio']

    templates = []
    for fiofile in parsed.fiofiles:
        with open(fiofile, 'rt') as f:
            templates.append(f.read())
    parsed.__dict__['templates'] = templates
    return parsed


def setup_logger(cmdline):
    logger = logging.getLogger(__name__)
    log_props = {
        'level': logging.DEBUG if cmdline.verbose > 0 else logging.INFO,
        'style': '{',
        'format': '|'.join(['{asctime}', '{levelname}', '{message}']),
    }
    if cmdline.log:
        logging.basicConfig(filename=cmdline.log, filemode='w', **log_props)
    else:
        logging.basicConfig(stream=sys.stderr, **log_props)
    return logger


def apply_template(template, params):
    t = Template(template)
    t.substitute(params)


def main():
    jobs = []
    param_keys = list(cmdline.fio_params.keys())
    for template in cmdline.templates:
        param_values = product(*[cmdline.fio_params[k] for k in param_keys])

        t = Template(template)
        for values in param_values:
            mapping = { k: values[i] for i, k in enumerate(param_keys) }
            job = FioJob(fio_script=t.substitute(mapping), mapping=mapping)
            jobs.append(job)
        
    logger.info("Number of jobs: %d" % len(jobs))

    cmdline.output.write(FioJob.fio_headers + ';replicate;' + ';'.join(param_keys) + '\n')
    cmdline.output.flush()

    for replicate in cmdline.parameters['replicate']:
        logger.info("Starting replicate %s" % str(replicate))

        for job_num, job in enumerate(jobs):
            logger.info("Starting job %d of %d" % (job_num + 1, len(jobs)))
            if cmdline.cooldown > 0:
                logger.info("cooldown")
                sleep(cmdline.cooldown)
            job.run(extra=';' + str(replicate))
        logger.info("Finished replicate %s" % str(replicate))

    successes = sum(job.success == True for job in jobs)
    fails = sum(job.success == False for job in jobs)
    logger.info("Jobs: %d completed, %d failed" % (successes, fails))


if __name__ == '__main__':
    global cmdline, logger
    cmdline = parse_cmdline()
    logger = setup_logger(cmdline)

    logger.info('Started')
    main()
    logger.info('All done')
#EOF
