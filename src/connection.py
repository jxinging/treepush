# coding: utf8
__author__ = 'JinXing'


class Connection(object):
    def __init__(self, src_ip, dest_ip, process, logfile):
        self.src_ip = src_ip
        self.dest_ip = dest_ip
        self.ssh_process = process
        self.logfile = logfile

    def __str__(self):
        return "%s -> %s" % (self.src_ip, self.dest_ip)