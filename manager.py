# coding: utf8
__author__ = 'JinXing'

import time
import select
import sys
from multiprocessing import Lock

from connection import Connection
from globals import *
from helper import *


class TPushManager(object):
    def __init__(self, src_pool, dst_hosts, command, options):
        # optlist: [server_name, server_ip, server_sshport]
        self.src_pool = src_pool
        self.dst_hosts = dst_hosts
        self.command = command
        self.options = options

        self.max_retry = options.retry
        self.sshport_map = options.sshport_map
        self.all_dst_hosts = dst_hosts
        self.src_ips = self.src_pool.src_ips()
        self.total_dsts = len(dst_hosts)

        self.retry_count = {}
        self.running_hosts = []
        self.done_hosts = []
        self.error_hosts = []
        self.connections = []

        self.is_active = False
        self.ssh_master_lock = Lock()

        ## 下面对要操作的IP进行重排，在不同的IP段选择一个IP移到列表前面
        ## 扩大第一轮更新时的目标机器网段范围, 下一轮更新就会有更大的几率选择到在同一段的源IP
        subnet_set = set()
        for ip in self.dst_hosts[:]:
            subnet = get_subnet(ip)
            if subnet not in subnet_set:
                subnet_set.add(subnet)
                self.dst_hosts.remove(ip)
                self.dst_hosts.insert(0, ip)

    def __str__(self):
        return "source:%s, total:%s, done:%s, error:%s, running:%s, pending:%s" % (
            len(self.src_pool), self.total_dsts, len(self.done_hosts),
            len(self.error_hosts), len(self.running_hosts), len(self.dst_hosts))

    def main_loop(self, interval=0.1):
        self.is_active = True
        while self.is_active:
            self.read_command(timeout=0.1)
            if not self.run_loop():
                self.is_active = False
                break
            time.sleep(interval)
            #self.smart_reconnect()

    def run_loop(self):
        show_info = False
        for dst_ip in self.dst_hosts[:]:
            src_ip = self.src_pool.get_src(dst_ip)
            logfile = "%s/%s_from_%s.log" % (self.options.logdir, dst_ip, src_ip)
            if src_ip is None:
                logger.debug('源IP池中没有可用IP')
                break

            try:
                self.ssh_master_lock.acquire()
                sshport = self.get_sshport_by_ip(src_ip)
                env = {"TPUSH_SRC": src_ip, "TPUSH_DEST": dst_ip, "TPUSH_DEST_PORT": sshport}
                ssh_process = subprocess_ssh(src_ip, self.command, port=sshport, logfile=logfile, env=env)
            except Exception, _:
                logger.fail("start cmd error, source_ip: %s, target_ip: %s", src_ip, dst_ip, exc_info=True)
                ssh_process = fail_popen
            else:
                logger.info('### [RUN] %s -> %s', src_ip, dst_ip)
            finally:
                self.ssh_master_lock.release()

            self.dst_hosts.remove(dst_ip)
            show_info = True
            self.running_hosts.append(dst_ip)
            conn = Connection(src_ip, dst_ip, ssh_process, logfile)
            self.connections.append(conn)

        logger.info("%s", "\n".join(str(x.ssh_process.poll()) for x in self.connections))
        for conn in [x for x in self.connections if x.ssh_process.poll() is not None]:
            self.connections.remove(conn)
            self.running_hosts.remove(conn.dst_ip)
            self.src_pool.add_src_conn(conn.src_ip)
            if conn.ssh_process.returncode != 0:
                if conn.dst_ip not in self.retry_count:
                    self.retry_count[conn.dst_ip] = 0
                if self.retry_count[conn.dst_ip] >= self.max_retry:
                    logger.fail('### [ERROR] %s -> %s, tail log: %s\n%s',
                                    conn.src_ip, conn.dst_ip, conn.logfile,
                                    '\n'.join(open(conn.logfile).readlines()[-2:]))
                    show_info = True
                    self.error_hosts.append(conn.dst_ip)
                else:
                    self.retry_count[conn.dst_ip] += 1
                    logger.info('### [RETRY]#%s %s, tail log: %s\n%s', self.retry_count[conn.dst_ip],
                                    conn.dst_ip, conn.logfile, '\n'.join(open(conn.logfile).readlines()[-2:]))
                    self.dst_hosts.append(conn.dst_ip)
            else:
                logger.success('### [DONE] %s -> %s', conn.src_ip, conn.dst_ip)
                show_info = True
                self.done_hosts.append(conn.dst_ip)
                if not self.src_pool.has_ip(conn.dst_ip):
                    self.src_pool.add_src(conn.dst_ip)    # 添加新的IP到更新源池

        if show_info:
            logger.info(str(self))

        if len(self.dst_hosts) == 0 and len(self.running_hosts) == 0:
            logger.info('#### 操作结束')
            return False

        return True

    def smart_reconnect(self):
        src_subnets = [x['ip'] for x in self.src_pool.pool.itervalues() if x['conn'] >= 2]
        for conn in self.connections[:]:
            dst_subnet = get_subnet(conn.dst_ip)
            if get_subnet(conn.src_ip) != dst_subnet and dst_subnet in src_subnets:
                logger.info('smart_reconnect %s -> %s', conn.src_ip, conn.dst_ip)
                if conn.ssh_process.poll() is None:  # 如果进程没有结束就发送 terminate 信号
                    conn.ssh_process.terminate()     # 这里不能用 kill(), kill() 会使进程直接退出, 导致没有清理 control socket
                    conn.ssh_process.wait()

                self.connections.remove(conn)
                self.running_hosts.remove(conn.dst_ip)
                self.dst_hosts.append(conn.dst_ip)
                self.src_pool.add_src_conn(conn.src_ip)

    def read_command(self, timeout=0.01):
        r, w, _ = select.select([sys.stdin], [], [], timeout)
        if len(r) == 0:
            return
        input_str_list = sys.stdin.readline().strip().split()
        if len(input_str_list) == 0:
            input_str_list = ['help']

        if len(input_str_list) == 1:
            cmd = input_str_list[0]
            args = ['']
        else:
            cmd, args = input_str_list[0], input_str_list[1:]
        logger.info("*** run command [%s]", cmd)
        method_name = "do_cmd_"+cmd
        if not hasattr(self, method_name):
            logger.error("command [%s] not defined", cmd)
            cmd = "help"
            method_name = "do_cmd_help"

        getattr(self, method_name)(cmd, args)
        # logger.info('*** command [%s] done', cmd)

    def do_cmd_help(self, *_):
        print u"""支持的命令:
help                        显示此帮助
show                        显示运行进度信息
reconnect <slow|all>        断开连接, 把目标IP重新放入等待执行队列
"""

    def do_cmd_show(self, *_):
        info = []
        if len(self.dst_hosts) > 0:
            info.append("pending hosts: ")
            for ip in self.dst_hosts:
                info.append(str(ip))
        if len(self.connections) > 0:
            info.append("running connections: ")
            for s in self.connections:
                info.append(str(s))
        print '\n'.join(info)

    def do_cmd_reconnect(self, _, args):
        if not args:
            args = ['slow']
        conn_type = args[0]
        if conn_type == 'slow':
            reconnect_session_list = []
            for conn in self.connections:
                if get_subnet(conn.src_ip) != get_subnet(conn.dst_ip):
                    reconnect_session_list.append(conn)
        elif conn_type == 'all':
            reconnect_session_list = self.connections[:]
        else:
            logger.error(u"unknown reconnect args [%s]", type)
            return
        if not reconnect_session_list:
            return
        for conn in reconnect_session_list:
            if conn.ssh_process.poll() is None:  # 如果进程没有结束就发送 terminate 信号
                # 这里不能发送 kill 信号, kill 会使进程直接退出, 不会清理 control socket 文件
                conn.ssh_process.terminate()

        for conn in reconnect_session_list[:]:
            if conn.ssh_process.poll() is None:  # 等待进程结束
                conn.ssh_process.wait()
            self.connections.remove(conn)
            self.running_hosts.remove(conn.dst_ip)
            self.dst_hosts.append(conn.dst_ip)
            self.src_pool.add_src_conn(conn.src_ip)

    def finish(self):
        logger.info('结束操作中...')
        for conn in self.connections:
            if conn.ssh_process.poll() is None:  # 如果进程没有结束就发送 terminate 信号
                # 这里不能发送 kill 信号, kill 会使进程直接退出, 不会清理 control socket 文件
                conn.ssh_process.terminate()

        for conn in self.connections[:]:
            if conn.ssh_process.poll() is None:  # 等待进程结束
                conn.ssh_process.wait()

            self.connections.remove(conn)
            self.running_hosts.remove(conn.dst_ip)
            if conn.ssh_process.returncode != 0:
                logger.fail('### [ERROR] %s -> %s', conn.src_ip, conn.dst_ip)
                self.error_hosts.append(conn.dst_ip)
            else:
                logger.fail('### [DONE] %s -> %s', conn.src_ip, conn.dst_ip)
                self.done_hosts.append(conn.dst_ip)

        print '################## 成功列表(%s) ##################' % len(self.done_hosts)
        print ' '.join(self.done_hosts)
        print '################## 失败列表(%s) ##################' % len(self.error_hosts)
        print ' '.join(self.error_hosts)
        print '################## 未处理列表(%s) ##################' % len(self.dst_hosts)
        print ' '.join(self.dst_hosts)

    def get_sshport_by_ip(self, ip):
        return self.sshport_map.get(ip, 22)