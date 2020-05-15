
from mgr_module import MgrModule
import logging
from logging import handlers
import socket
import os, sys
from multiprocessing import Process
from threading import Timer
import threading
import time


class Module(MgrModule):
    COMMANDS = [
        {
            "cmd": "msller",
            "desc": "Get Max Stable Line",
            "perm": "r",
            "poll": "true"
        },
    ]


    def __init__(self, *args, **kwargs):
        super(Module, self).__init__(*args, **kwargs)
        self.msl_logger = logging.getLogger(__name__)
        self.msl_logger.setLevel(logging.INFO)
        self.msl_threshold = 0.0
        self.log_name = '/var/log/ceph/ceph-mgr.msller.' + socket.gethostname() + '.log'
        #th = handlers.TimedRotatingFileHandler(filename=self.logname, when='D', backupCount=3, encoding='utf-8')
        th = handlers.TimedRotatingFileHandler(filename=self.log_name, when='M', backupCount=1, encoding='utf-8')
        fmt = '%(asctime)s.%(msecs)03d|%(message)s'
        th.setFormatter(logging.Formatter(fmt))
        self.msl_logger.addHandler(th)
        self.shutdown_event = threading.Event()

    def self_test(self):
        r = self.get('io_rate')
        assert('pg_stats_delta' in r)
        assert('stamp_delta' in r['pg_stats_delta'])
        assert('stat_sum' in r['pg_stats_delta'])
        assert('num_read_kb' in r['pg_stats_delta']['stat_sum'])
        assert('num_write_kb' in r['pg_stats_delta']['stat_sum'])
        assert('num_write' in r['pg_stats_delta']['stat_sum'])
        assert('num_read' in r['pg_stats_delta']['stat_sum'])

    def serve(self):
        # init parameters of msller
        self.init_serve()

        # start logging thread
        self.msl_logging()

        # start analyze thread
        self.msl_analyze()

        # wait for the shutdown event
        self.shutdown_event.wait()
        self.shutdown_event.clear()

    def shutdown(self):
        super(Module, self).shutdown()
        self.shutdown_event.set()

    def msl_logging(self):
        rd = 0
        wr = 0
        total = 0
        rd_ops = 0
        wr_ops = 0
        total_ops = 0
        ret = ''

        time.sleep(1)
        r = self.get('io_rate')

        stamp_delta = float(r['pg_stats_delta']['stamp_delta'])
        if (stamp_delta > 0):
            rd = int(r['pg_stats_delta']['stat_sum']['num_read_kb']) / stamp_delta
            wr = int(r['pg_stats_delta']['stat_sum']['num_write_kb']) / stamp_delta
            # The values are in kB, but to_pretty_iec() requires them to be in bytes
            rd = int(rd) << 10
            wr = int(wr) << 10
            total = rd + wr

            rd_ops = int(r['pg_stats_delta']['stat_sum']['num_read']) / stamp_delta
            wr_ops = int(r['pg_stats_delta']['stat_sum']['num_write']) / stamp_delta
            total_ops = rd_ops + wr_ops

        self.msl_logger.info(str(rd_ops) + ',' + str(wr_ops) + ',' + str(total_ops))

        Timer(1, Module.msl_logging, (self,)).start()

    def msl_analyze(self):
        with open(self.log_name, 'r') as reader:
            min_value = float("inf")
            logs = reader.readlines()
            # print(log_content)
            for log in logs:
                strs = log.split('|')
                timestamp = strs[0]
                data = strs[1]
                strs = data.split(',')
                value = float(strs[2].strip())
                if value < min_value:
                    min_value = value

        if min_value < self.msl_threshold:
            min_value = self.msl_threshold

        # update mgr/msller/msl_value
        self.set_store('msl_value', str(min_value))

        Timer(60, Module.msl_analyze, (self,)).start()

    def handle_command(self, inbuf, command):
        rd = 0
        wr = 0
        total = 0
        rd_ops = 0
        wr_ops = 0
        total_ops = 0
        msl_ops = 0
        ret = ''

        if command['prefix'] == 'msller':
            r = self.get('io_rate')
            msl_value = self.get_store('msl_value')
            msl_ops = float(msl_value)

            stamp_delta = float(r['pg_stats_delta']['stamp_delta'])
            if (stamp_delta > 0):
                rd = int(r['pg_stats_delta']['stat_sum']['num_read_kb']) / stamp_delta
                wr = int(r['pg_stats_delta']['stat_sum']['num_write_kb']) / stamp_delta
                # The values are in kB, but to_pretty_iec() requires them to be in bytes
                rd = int(rd) << 10
                wr = int(wr) << 10
                total = rd + wr

                rd_ops = int(r['pg_stats_delta']['stat_sum']['num_read']) / stamp_delta
                wr_ops = int(r['pg_stats_delta']['stat_sum']['num_write']) / stamp_delta
                total_ops = rd_ops + wr_ops

            if 'width' in command:
                width = command['width']
            else:
                width = 90

            if command.get('print_header', False):
                elems = ['Read', 'Write', 'Total', 'Read IOPS', 'Write IOPS', 'Total IOPS', 'MSL']
                ret += self.get_pretty_header(elems, width)

            elems = [
                self.to_pretty_iec(rd) + 'B/s',
                self.to_pretty_iec(wr) + 'B/s',
                self.to_pretty_iec(total) + 'B/s',
                int(rd_ops),
                int(wr_ops),
                int(total_ops),
                int(msl_ops)
            ]
            ret += self.get_pretty_row(elems, width)

        return 0, '', ret

    def init_serve(self):
        str_value = self.get_store('msl_threshold')
        if str_value is None:
            print("msl threshold parameter are not set!")
            print("use default threshold")
            self.msl_threshold = 100.0
            self.set_store('msl_threshold', str(self.msl_threshold))
        else:
            self.msl_threshold = float(str_value)

        str_value = self.get_store('msl_value')

        if str_value is None or float(str_value) < self.msl_threshold:
            self.set_store('msl_value', str(self.msl_threshold))
