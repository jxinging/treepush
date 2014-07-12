# coding: utf8
__author__ = 'JinXing'

from globals import G_LOGGER
from helper import *


class SourcePool(object):
    """源主机池"""
    def __init__(self, max_conn, src_ips):
        self.pool = {}
        self.max_conn = max_conn    # 每个源的最大连接数
        for ip in src_ips:
            self.pool[ip] = {'ip': ip, 'conn': max_conn}    # conn 表示还可以建立的连接数
            G_LOGGER.debug("%s 可用连接: %s", self.pool[ip]['ip'], self.pool[ip]['conn'])

    def __len__(self):
        return len(self.pool)

    def src_ips(self):
        return self.pool.keys()
    # def _print(self):
    #     for ip in self.pool:
    #         print ip, self.pool[ip]['conn']

    def has_ip(self, ip):
        return str(ip) in self.pool

    def get_src(self,  dst_ip=None, need_conn=1):
        """dst, 目标机器的IP(用于搜索同一段的源IP)
        need_conn, 最少可用连接
        """
        G_LOGGER.debug('get_src(%s, %d)', dst_ip, need_conn)

        available_src_ips = [x['ip'] for x in self.pool.itervalues() if x['conn'] >= need_conn]

        if len(available_src_ips) == 0:
            return None

        if dst_ip is None:
            self.del_src_conn(available_src_ips[0])
            return available_src_ips[0]

        dst_ip_int = ip2long(dst_ip)

        def get_src_ip_rank(src_ip):
            int_ip = ip2long(src_ip)
            distance = abs(dst_ip_int-int_ip)
            if src_ip in self.pool:
                available_conn = self.pool[ip]['conn']
            else:
                available_conn = 0
            rank = distance + (available_conn-self.max_conn)
            return rank

        # 指定了目标IP, 下面尝试获取一个IP段相近的IP
        min_rank_ip = None
        min_rank = get_src_ip_rank('255.255.255.255')
        for ip in available_src_ips:
            if get_src_ip_rank(ip) < min_rank:
                min_rank_ip = ip
                min_rank = get_src_ip_rank(min_rank_ip)
        if min_rank_ip:
            self.del_src_conn(min_rank_ip)
            return min_rank_ip
        else:
            return None

    def add_src(self, ip):
        if ip in self.pool:
            return None
        self.pool[ip] = {'ip': ip, 'conn': self.max_conn}
        G_LOGGER.debug("%s 可用连接: %d", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return ip

    def del_src(self, ip):
        if ip in self.pool:
            self.pool.pop(ip)
            G_LOGGER.debug("%s 可用连接: %d", self.pool[ip]['ip'], self.pool[ip]['conn'])

    def add_src_conn(self, ip, num=1):
        if ip not in self.pool:
            G_LOGGER.debug('%s pool中不存在', ip)
            return
        if self.pool[ip]['conn'] >= self.max_conn:
            G_LOGGER.debug('%s 可用连接数已达最大值', ip)
            return

        self.pool[ip]['conn'] += num
        G_LOGGER.debug("%s 可用连接: %d", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return self.pool[ip]['conn']

    def del_src_conn(self, ip, num=1):
        if ip not in self.pool:
            G_LOGGER.debug('pool中不存在 %s', ip)
            return
        if self.pool[ip]['conn'] <= 0:
            G_LOGGER.debug('%s 无可用连接', ip)
            return

        # print self.pool[ip]['conn'], num
        self.pool[ip]['conn'] -= num
        G_LOGGER.debug("%s 可用连接: %d", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return self.pool[ip]['conn']
