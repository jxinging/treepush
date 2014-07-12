# coding: utf8
__author__ = 'JinXing'


class Connection(object):
    def __init__(self, src_ip, dst_ip, process, logfile):
        self.src_ip = src_ip
        self.dst_ip = dst_ip
        self.ssh_process = process
        self.logfile = logfile

    def __str__(self):
        return "%s -> %s" % (self.src_ip, self.dst_ip)