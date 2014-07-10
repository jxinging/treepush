#!/usr/bin/env python2
# coding: utf8


# 层级推送文件脚本, by jinxing
# 131109, 第一版
# 131112, 改判断返回源IP的方式，根据IP地址的距离及源IP的可用连接数计算 rank 值
# 131113, 增加执行期间的指令支持, 可通过 stdin 输入指令

__author__ = 'JinXing'

import sys
import os
import time
import subprocess
import logging
import socket
import struct
import select
import tempfile
from multiprocessing import Lock

__VERSION__ = 0.2

### 全局变量
# 同一台源机器最大并发连接数
DEFAULT_MAX_CONN = 4

LOG_SUCCESS = logging.ERROR+1
LOG_FAIL = logging.ERROR+2

G_LOGGER = None
g_Pool = None
g_options = None
G_LOG_DIR = 'logs'
G_SSH_MASTER_LOCK = None


class HostPool(object):
    def __init__(self, max_conn, *ip_list):
        self.pool = {}
        self.max_conn = max_conn
        for ip in ip_list:
            self.pool[ip] = {'ip': ip, 'conn': max_conn}     # conn表示可接受的连接数
            G_LOGGER.debug("%s 可用连接: %s", self.pool[ip]['ip'], self.pool[ip]['conn'])

    def __len__(self):
        return len(self.pool)

    def _print(self):
        for ip in self.pool:
            print ip, self.pool[ip]['conn']

    def has_ip(self, _ip):
        return str(_ip) in self.pool

    def get_ip(self, target_ip=None):
        return self.new_do_get_ip(target_ip)

        # for i in xrange(self.max_conn/2, 0, -1):  # 尽量选择可用连接数多的源IP
        #     source_ip = self._do_get_ip(target_ip, i)
        #     if source_ip:
        #         return source_ip

    def new_do_get_ip(self, target_ip=None, min_conn=1):
        """
        target_ip, 目标机器的IP(用于搜索同一段的源IP)
        min_conn, 最少可用连接
        """
        logging.debug('new_do_get_ip(%s, %s)', target_ip, min_conn)
        free_ip_list = [x['ip'] for x in self.pool.itervalues() if x['conn'] >= min_conn]
        if len(free_ip_list) == 0:
            return None
        if target_ip is None:
            self.del_conn(free_ip_list[0])
            return free_ip_list[0]

        def get_rank(ip):
            int_ip = ip2long(ip)
            distance = abs(target_ip_int-int_ip)
            if ip in self.pool:
                available_conn = self.pool[ip]['conn']
            else:
                available_conn = 0
            rank = distance + (available_conn-self.max_conn)
            return rank

        # 指定了 target_ip, 下面尝试获取一个IP段相近的IP
        target_ip_int = ip2long(target_ip)
        min_rank_ip = None
        min_rank = get_rank('255.255.255.255')
        for ip in free_ip_list:
            if get_rank(ip) < min_rank:
                min_rank_ip = ip
                min_rank = get_rank(min_rank_ip)
        if min_rank_ip:
            self.del_conn(min_rank_ip)
            return min_rank_ip
        else:
            return None

    def add_ip(self, ip):
        if ip in self.pool:
            return None
        self.pool[ip] = {'ip': ip, 'conn': self.max_conn}
        G_LOGGER.debug("%s 可用连接: %s", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return ip

    def _del_ip(self, ip):
        if ip in self.pool:
            self.pool.pop(ip)
            G_LOGGER.debug("%s 可用连接: %s", self.pool[ip]['ip'], self.pool[ip]['conn'])

    def add_conn(self, ip, num=1):
        if ip not in self.pool:
            G_LOGGER.debug('%s pool中不存在 add_conn()', ip)
            return
        if self.pool[ip]['conn'] >= self.max_conn:
            G_LOGGER.debug('%s 可用连接已达最大值 add_conn()', ip)
            return

        self.pool[ip]['conn'] += num
        G_LOGGER.debug("%s 可用连接: %s", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return self.pool[ip]['conn']

    def del_conn(self, ip, num=1):
        if ip not in self.pool:
            G_LOGGER.debug('del_conn(), pool中不存在 %s', ip)
            return
        if self.pool[ip]['conn'] <= 0:
            G_LOGGER.debug('%s 无可用连接 add_conn()', ip)
            return

        # print self.pool[ip]['conn'], num
        self.pool[ip]['conn'] -= num
        G_LOGGER.debug("%s 可用连接: %s", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return self.pool[ip]['conn']


class CommandSession(object):
    def __init__(self, from_ip, to_ip, p, logfile):
        self.from_ip = from_ip
        self.to_ip = to_ip
        self.process = p
        self.logfile = logfile

    def __str__(self):
        return "%s -> %s" % (self.from_ip, self.to_ip)


class DsyncManager(object):
    def __init__(self, opt_list, run_cmd):
        # optlist, [server_name, server_ip, server_sshport]
        self.optlist = opt_list
        self.to_ip_list = [x[1] for x in optlist]
        self.to_ip_list = list(set(self.to_ip_list)) # 去除重复IP

        self.total_node = len(self.to_ip_list)
        self.retry_count = {}
        self.max_retry = g_options.retry
        self.run_cmd = run_cmd
        self.running_list = []
        self.done_list = []
        self.error_list = []
        self.session_list = []

        self.active = False

        ## 下面对要操作的IP进行重排，在不同的IP段选择一个IP移到列表前面
        ##  扩大第一轮更新时的目标机器网段范围, 下一轮更新就会有更大的几率选择到在同一段的源IP
        subnet_list = []
        for ip in self.to_ip_list[:]:
            subnet = get_subnet(ip)
            if subnet not in subnet_list:
                subnet_list.append(subnet)
                self.to_ip_list.remove(ip)
                self.to_ip_list.insert(0, ip)

    def __str__(self):
        info = list()
        info.append("source:%s, total:%s, done:%s, error:%s, running:%s, pending:%s" % (
            len(g_Pool), self.total_node, len(self.done_list),
            len(self.error_list), len(self.session_list), len(self.to_ip_list)
        ))

        #if len(self.to_ip_list) <= len(self.optlist)/10 and \
        #   len(self.session_list) <= len(self.optlist)/10:
        #    if len(self.to_ip_list) > 0:
        #        info.append("pending hosts: ")
        #        for ip in self.to_ip_list:
        #            info.append(str(ip))
        #    if len(self.session_list) > 0:
        #        info.append("running sessions: ")
        #        for s in self.session_list:
        #            info.append(str(s))

        return '\n'.join(info)

    def main_loop(self, interval=0.1):
        self.active = True
        while self.active:
            self.wait_command(timeout=0.1)
            if self.do_loop() == -1:
                self.active = False
                break
            time.sleep(interval)
            #self.smart_reconnect()

    def smart_reconnect(self):
        source_ip_subnet_list = [x['ip'] for x in g_Pool.pool.itervalues() if x['conn'] >= 2]
        for session in self.session_list[:]:
            to_ip_subnet = get_subnet(session.to_ip)
            if get_subnet(session.from_ip) != to_ip_subnet and to_ip_subnet in source_ip_subnet_list:
                G_LOGGER.info('smart_reconnect %s -> %s', session.from_ip, session.to_ip)
                if session.process.poll() is None: # 如果进程没有结束就发送 terminate 信号
                    session.process.terminate() # 这里不能发送 kill 信号, kill 会使进程直接退出, 不会清理 control socket 文件
                    session.process.wait()

                self.session_list.remove(session)
                self.running_list.remove(session.to_ip)
                self.to_ip_list.append(session.to_ip)
                g_Pool.add_conn(session.from_ip)

    def wait_command(self, timeout=0.01):
        r, w, _ = select.select([sys.stdin], [], [], timeout)
        if len(r) == 0:
            return
        input_str_list = sys.stdin.readline().strip().split()
        if len(input_str_list) == 0:
            input_str_list = ['help']

        if len(input_str_list) > 1:
            cmd, args = input_str_list[0], input_str_list[1:]
        else:
            cmd = input_str_list[0]
            args = ['']
        G_LOGGER.info("*** run command [%s]", cmd)
        method_name = "do_cmd_"+cmd
        if not hasattr(self, method_name):
            G_LOGGER.error("command [%s] not defined", cmd)
            cmd = "help"
            method_name = "do_cmd_help"

        getattr(self, method_name)(cmd, args)
        # G_LOGGER.info('*** command [%s] done', cmd)

    def do_cmd_help(self, cmd_name, args):
        print \
u"""支持的命令:
help                        显示此帮助
reconnect <slow|all>        断开在执行的连接, 把目标IP重新放入等待执行队列
show                        显示正在执行的IP, 等待执行的IP
"""

    def do_cmd_show(self, cmd_name, args):
        info = []
        if len(self.to_ip_list) > 0:
            info.append("pending hosts: ")
            for ip in self.to_ip_list:
                info.append(str(ip))
        if len(self.session_list) > 0:
            info.append("running sessions: ")
            for s in self.session_list:
                info.append(str(s))
        print '\n'.join(info)

    def do_cmd_reconnect(self, cmd_name, args):
        if not args:
            args = ['slow']
        type = args[0]
        if type == 'slow':
            reconnect_session_list = []
            for session in self.session_list:
                if get_subnet(session.from_ip) != get_subnet(session.to_ip):
                    reconnect_session_list.append(session)
        elif type == 'all':
            reconnect_session_list = self.session_list[:]
        else:
            G_LOGGER.error(u"unknown reconnect args [%s]", type)
            return
        if not reconnect_session_list:
            return
        for session in reconnect_session_list:
            if session.process.poll() is None:  # 如果进程没有结束就发送 terminate 信号
                # 这里不能发送 kill 信号, kill 会使进程直接退出, 不会清理 control socket 文件
                session.process.terminate()

        for session in reconnect_session_list[:]:
            if session.process.poll() is None:  # 等待进程结束
                session.process.wait()
            self.session_list.remove(session)
            self.running_list.remove(session.to_ip)
            self.to_ip_list.append(session.to_ip)
            g_Pool.add_conn(session.from_ip)

    def do_loop(self):
        show_info = False
        for to_ip in self.to_ip_list[:]:
            s_ip = g_Pool.get_ip(to_ip)
            logfile = "%s/%s_from_%s.log" % (G_LOG_DIR, to_ip, s_ip)
            if s_ip is None:    # 源IP池中没有IP可用
                G_LOGGER.debug('源IP池中没有可用IP')
                break
            try:
                G_SSH_MASTER_LOCK.acquire()
                sshport = get_sshport_by_ip(s_ip)
                process = subprocess_ssh(s_ip, self.run_cmd, port=sshport, logfile=logfile,
                                                env={
                                                    "DSYNC_TO_IP": to_ip,
                                                    "DSYNC_FROM_IP": s_ip,
                                                    "DSYNC_TO_SSHPORT": sshport
                                                })
            except Exception,e:
                logging.log(LOG_FAIL, "start cmd error, source_ip: %s, target_ip: %s", s_ip, to_ip, exc_info=True)
                g_Pool.add_conn(s_ip)   # 调用 get_ip() 时连接数自动减1，所以这里要加回去
                self.error_list.append(to_ip)
                continue
            finally:
                G_SSH_MASTER_LOCK.release()

            self.to_ip_list.remove(to_ip)
            G_LOGGER.info('### [RUN] %s -> %s', s_ip, to_ip)
            show_info = True
            self.running_list.append(to_ip)
            session = CommandSession(s_ip, to_ip, process, logfile)
            self.session_list.append(session)

        for session in self.session_list[:]:
            if session.process.poll() is not None:
                self.session_list.remove(session)
                self.running_list.remove(session.to_ip)
                if session.process.returncode != 0:
                    if session.to_ip not in self.retry_count:
                        self.retry_count[session.to_ip] = 0
                    if self.retry_count[session.to_ip] >= self.max_retry:
                        G_LOGGER.log(LOG_FAIL, '### [ERROR] %s -> %s, tail log: %s\n%s',
                                        session.from_ip, session.to_ip, session.logfile,
                                        '\n'.join(open(session.logfile).readlines()[-2:]))
                        show_info = True
                        self.error_list.append(session.to_ip)
                    else:
                        self.retry_count[session.to_ip] += 1
                        G_LOGGER.info('### [RETRY]#%s %s, tail log: %s\n%s', self.retry_count[session.to_ip],
                                        session.to_ip, session.logfile,
                                        '\n'.join(open(session.logfile).readlines()[-2:]) )
                        self.to_ip_list.append(session.to_ip)
                    g_Pool.add_conn(session.from_ip)
                else:
                    G_LOGGER.log(LOG_SUCCESS, '### [DONE] %s -> %s', session.from_ip, session.to_ip)
                    show_info = True
                    self.done_list.append(session.to_ip)
                    g_Pool.add_conn(session.from_ip)
                    if not g_Pool.has_ip(session.to_ip):
                        g_Pool.add_ip(session.to_ip) # 添加新的IP到更新源池

        if show_info:
            G_LOGGER.info(str(self))
        if len(self.to_ip_list) == 0 and len(self.running_list) == 0:
            G_LOGGER.info('#### 操作结束')
            return -1

    def __del__(self):
        G_LOGGER.info('结束操作中...')
        for session in self.session_list:
            if session.process.poll() is None: # 如果进程没有结束就发送 terminate 信号
                session.process.terminate() # 这里不能发送 kill 信号, kill 会使进程直接退出, 不会清理 control socket 文件

        for session in self.session_list[:]:
            if session.process.poll() is None: # 等待进程结束
                session.process.wait()

            self.session_list.remove(session)
            self.running_list.remove(session.to_ip)
            if session.process.returncode != 0:
                G_LOGGER.log(LOG_FAIL, '### [ERROR] %s -> %s', session.from_ip, session.to_ip)
                self.error_list.append(session.to_ip)
            else:
                G_LOGGER.log(LOG_SUCCESS, '### [DONE] %s -> %s', session.from_ip, session.to_ip)
                self.done_list.append(session.to_ip)

        print '################## 成功列表(%s) ##################' % len(self.done_list)
        print ' '.join(self.done_list)
        print '################## 失败列表(%s) ##################' % len(self.error_list)
        print ' '.join(self.error_list)
        print '################## 未处理列表(%s) ##################' % len(self.to_ip_list)
        print ' '.join(self.to_ip_list)


def subprocess_test(host, cmd, env=None, logfile=None, port=22):
    p = subprocess.Popen('sleep 2s', shell=True, close_fds=True,
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p


def subprocess_ssh(host, cmd, env=None, logfile=None, port=22):
    port = int(port)
    ssh_control_dir = os.path.join(tempfile.gettempdir(), '.dsync_ssh_control')

    if not os.path.exists(ssh_control_dir): os.mkdir(ssh_control_dir)
    ssh_control_socketfile = os.path.join(ssh_control_dir, '%s_%d.sock' % (host, port) )

    ssh_args = []
    if not os.path.exists(ssh_control_socketfile):
        ssh_args.append('-M')
    ssh_args.extend(['-o', 'ControlPath="%s"' % (ssh_control_socketfile)])
    ssh_args.extend(['-p', str(port)])
    ssh_args.extend(['-A', '-T'])
    ssh_args.extend(['-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    '-o', 'ConnectTimeout=4'])
    ssh_args.append(host)
    if env:
        for name, value in env.items():
            ssh_args.append("export %s=%s;" % (name, value))
    ssh_args.append("echo '%s:%s ssh connection success';" % (host, port))  # 用于判断连接在哪一层被断开
    ssh_args.append("sleep $((RANDOM%10+5));")  # 如果命令执行过快会出错(连接过快，被拒绝), 这里添加一个随机延时
    ssh_args.append(cmd)

    if logfile is not None:
        r_stdout = open(logfile, 'ab')
    else:
        r_stdout = subprocess.PIPE
    r_stdout.write('ssh '+' '.join(ssh_args)+'\n')
    p = subprocess.Popen(['/usr/bin/ssh']+ssh_args, shell=False, close_fds=True,
                         stdin=subprocess.PIPE, stdout=r_stdout, stderr=subprocess.STDOUT)
    return p


def get_optlist_by_listfile(list_file):
    optlist = []
    fp = open(list_file, 'rb')
    for server in fp:
        if server.find('#') >= 0:
            continue    # 注释行
        t_list = map(lambda x: x.strip(""""'"""), server.split())
        server_name = t_list[5]
        server_ip = t_list[7]
        server_sshport = t_list[6]
        optlist.append((server_name, server_ip, server_sshport))
    return optlist


def create_listfile_by_optlist():
    raise


def ip2long(ip_string):
    return struct.unpack('!I',socket.inet_aton(ip_string))[0]


def long2ip(ip_int):
    return socket.inet_ntoa(struct.pack('!I', ip_int))


def get_subnet(ip):
    ip_split = ip.split('.')
    if len(ip_split) != 4:
        raise ValueError(u"Invalid ip address %s" % ip)
    subnet = '.'.join(ip_split[:3])    # 以IP的前3段做为网段
    return subnet


def get_sshport_by_ip(ip):
    if ip in g_options.sshport_map:
        return int(g_options.sshport_map[ip])
    else:
        return 22


class Logger(object):
    pass


EPILOG = (
"TIPS:\n"
"1. 命令中可用的 shell 变量: DSYNC_TO_IP, DSYNC_TO_SSHPORT, DSYNC_FROM_IP\n"
"""2. rsync 命令参考: rsync -avz -e "ssh -o StrictHostKeyChecking=no"\n"""
"3. scp 命令参考: scp -o StrictHostKeyChecking=no\n"
"4. 执行过程中可输入命令进行交互控制, 输入 help 查看帮助\n"
"5. 完整的调用命令参考:\n"
"""python dsync.py 'rsync -avz -e "ssh -o StrictHostKeyChecking=no -p $DSYNC_TO_SSHPORT" """
"""/data/datafile $DSYNC_TO_IP:/data/datafile' -r 3 -m 4 -s 1.1.1.1 -l iplist.txt\n"""
"""python dsync.py 'cd /data/rsync/ && sh push_files_to_remote_rsync_dir.sh """
"""$DSYNC_TO_IP $DSYNC_TO_SSHPORT >/dev/null' -r 3 -m 4 -s 1.1.1.1 -l iplist.txt\n"""
)

if __name__ == '__main__':
    log_format = '%(asctime)s [%(levelname)s] # %(message)s'
    logging.basicConfig(format=log_format, datefmt='%m/%d %H:%M:%S')
    logging.addLevelName(LOG_SUCCESS, "\033[1;32mSUCCESS\033[0m")
    logging.addLevelName(LOG_FAIL, "\033[5;31mFAIL\033[0m")
    G_LOGGER = logging.getLogger()

    from optparse import OptionParser
    OptionParser.format_epilog = lambda self, epilog: self.epilog   # 重写 format_epilog(), 默认的方法会自动去掉换行
    parser = OptionParser(usage=u"Usage: %prog -l <listfile> -s <source_hosts> [-r|-m|-d] cmd",
                                version="version %s" % __VERSION__,
                                epilog=EPILOG)
    parser.add_option('-d', '--debug', action='store_true', dest='debug', default=False,
                            help=u'开启 debug')
    parser.add_option('-l', '--list', dest='listfile',
                            help=u'目标服务器列表(gatfile format)')
    parser.add_option('-s', '--source', dest='source',
                            help=u'版本源机器列表(1.1.1.1,2.2.2.2)')
    parser.add_option('-m', '--max', dest='max_conn', type='int', default=DEFAULT_MAX_CONN,
                            help=u'单IP并发连接数限制(default:%default)')
    parser.add_option('-r', '--retry', dest='retry', type='int', default=0,
                            help=u'单个IP出错后的重试次数(default:%default)')
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

    g_options = options
    g_options.source_ip_list = []
    g_options.sshport_map = {}
    for host in options.source.split(','):
        if host.find('@') >= 0:
            user, host = host.split('@', 1)
        if host.find(':') >= 0:
            ip, port = host.split(':', 1)
        else:
            ip, port = host, 22
        g_options.source_ip_list.append(ip)
        g_options.sshport_map[ip] = port

    run_cmd = ' '.join(other_args)

    if g_options.debug:
        G_LOGGER.setLevel(logging.DEBUG)
    else:
        G_LOGGER.setLevel(logging.INFO)

    if os.path.exists(G_LOG_DIR):
        os.rename(G_LOG_DIR, "%s.%d" % (G_LOG_DIR, int(time.time()) ))
    os.mkdir(G_LOG_DIR)

    optlist = get_optlist_by_listfile(g_options.listfile)

    for opt_host in optlist[:]:
        g_options.sshport_map[opt_host[1]] = opt_host[2]

    g_Pool = HostPool(g_options.max_conn, *g_options.source_ip_list)
    mgr = DsyncManager(optlist, run_cmd)
    G_SSH_MASTER_LOCK = Lock()  # 启动 ssh Master 时的锁， 防止同一IP启动多个 ssh Master
    try:
        mgr.main_loop(0.1)
    except KeyboardInterrupt, e:
        G_LOGGER.log(LOG_FAIL, '#### 操作中断')
    finally:
        del mgr
