#!/usr/bin/env python2
# coding: utf8

__author__ = 'JinXing'
__VERSION__ = 0.2

import sys
import time
from sourcepool import SourcePool
from manager import TPushManager
from helper import *
from globals import *

EPILOG = (
"TIPS:\n"
"1. 命令中可用的 shell 变量: TPUSH_SRC, TPUSH_DEST, TPUSH_DEST_PORT, \n"
"""2. rsync 命令参考: rsync -avz -e "ssh -o StrictHostKeyChecking=no"\n"""
"3. scp 命令参考: scp -o StrictHostKeyChecking=no\n"
"4. 执行过程中可输入命令进行交互控制, 输入 help 查看帮助\n"
"5. 完整的调用命令参考:\n"
"""python treepush.py 'rsync -avz -e "ssh -o StrictHostKeyChecking=no -p TPUSH_DEST_PORT" """
"""/data/datafile TPUSH_DEST:/data/datafile' -r 3 -m 4 -s 1.1.1.1 -l dst_hosts.txt\n"""
"""python treepush.py 'cd /data/rsync/ && sh push_files_to_remote_rsync_dir.sh """
"""TPUSH_DEST TPUSH_DEST_PORT >/dev/null' -r 3 -m 4 -s 1.1.1.1 -l dst_hosts.txt\n"""
).decode("utf8")

if __name__ == '__main__':
    from optparse import OptionParser
    OptionParser.format_epilog = lambda self, epilog: self.epilog   # 重写 format_epilog(), 默认的方法会自动去掉换行
    parser = OptionParser(usage=u"Usage: %prog -l <listfile> -s <source_hosts> [-r|-m|-d] cmd",
                                version="version %s" % __VERSION__,
                                epilog=EPILOG)
    parser.add_option('-d', '--debug', action='store_true', dest='debug', default=False,
                            help=u'开启 debug')
    parser.add_option('-l', '--list', dest='listfile',
                            help=u'目标服务器列表')
    parser.add_option('-s', '--source', dest='source',
                            help=u'版本源机器列表(1.1.1.1,2.2.2.2)')
    parser.add_option('-m', '--max', dest='max_conn', type='int', default=4,
                            help=u'单IP并发连接数限制(default:%default)')
    parser.add_option('-r', '--retry', dest='retry', type='int', default=0,
                            help=u'单个IP出错后的重试次数(default:%default)')
    parser.add_option('--logdir', dest='logdir', type='str', default='logs',
                            help=u'日志输出目录(default:%default)')
    #parser.add_option('--smart', dest='smart', action='store_true', default=False,
    #        help=u'开启智能重连(如果发现源机与目录机不在同一网段则尝试查找同一网段的源并重连)')

    options, other_args = parser.parse_args(sys.argv[1:])
    print_help = False
    if options.listfile is None:
        print >> sys.stderr, u"* 请指定目标服务器列表(-l/--list)"
        print_help = True
    if options.source is None:
        print >> sys.stderr, u"* 请指定源机器列表(-s/--source)"
        print_help = True
    if len(other_args) == 0:
        print >> sys.stderr, u"* 请给出要执行的命令"
        print_help = True
    if options.max_conn > 10:
        print >> sys.stderr, u"sshd_config MaxSessions 默认配置为 10。单IP的并发连接数不可大于该值"
        print_help = True
    if print_help:
        print "="*60
        parser.print_help()
        sys.exit(127)

    G_OPTIONS = options
    options.source_ips = []
    options.sshport_map = {}
    for host in options.source.split(','):
        if host.find('@') >= 0:
            user, host = host.split('@', 1)
        if host.find(':') >= 0:
            ip, port = host.split(':', 1)
        else:
            ip, port = host, 22
        options.source_ips.append(ip)
        options.sshport_map[ip] = port

    command = ' '.join(other_args)

    if options.debug:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)

    if os.path.exists(options.logdir):
        os.rename(options.logdir, "%s.%d" % (options.logdir, int(time.time())))
    os.mkdir(options.logdir)

    dst_optlist = get_optlist_by_listfile(options.listfile)

    for dst_host in dst_optlist[:]:
        options.sshport_map[dst_host[1]] = dst_host[2]

    logger.info("Command: %s", command)
    src_pool = SourcePool(options.max_conn, options.source_ips)
    mgr = TPushManager(src_pool, [x[1] for x in dst_optlist], options.sshport_map, command, options.retry)
    try:
        mgr.main_loop(0.1)
    except KeyboardInterrupt, e:
        logger.fail('#### 操作中断')
    finally:
        mgr.finish()
