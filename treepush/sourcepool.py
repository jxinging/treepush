# coding: utf8
__author__ = 'JinXing'

from helper import *


class SourcePool(object):
    """源主机池"""
    def __init__(self, max_conn, src_ips):
        self.pool = {}
        self.max_conn = max_conn    # 每个源的最大连接数
        for ip in src_ips:
            self.pool[ip] = {'ip': ip, 'conn': max_conn}    # conn 表示还可以建立的连接数
            logger.debug("%s 可用连接: %s", self.pool[ip]['ip'], self.pool[ip]['conn'])

    def __len__(self):
        return len(self.pool)

    def src_ips(self):
        return self.pool.keys()

    def has_ip(self, ip):
        return str(ip) in self.pool

    def get_src(self,  dest_ip=None, need_conn=1):
        """dest_ip, 目标机器的IP(用于搜索同一段的源IP)
        need_conn, 需要的连接数
        """
        available_src_ips = [x['ip'] for x in self.pool.itervalues() if x['conn'] >= need_conn]
        if len(available_src_ips) == 0:
            return None

        if dest_ip is None:
            ret_ip = available_src_ips[0]
            for ip in available_src_ips:
                if self.pool[ip]['conn'] > self.pool[ret_ip]['conn']:
                    ret_ip = ip
            self.sub_src_conn(ret_ip)
            return available_src_ips[0]

        dest_ip_int = ip2long(dest_ip)

        def get_src_ip_distance(src_ip):
            src_ip_int = ip2long(src_ip)
            distance = abs(dest_ip_int-src_ip_int)/1000  # /1000 忽略掉最后三位的影响
            return distance

        # 指定了目标IP, 下面尝试获取一个IP段相近的IP
        ret_ip = None
        min_distance = get_src_ip_distance('255.255.255.255')
        for ip in available_src_ips:
            tmp_distance = get_src_ip_distance(ip)
            if tmp_distance < min_distance:
                ret_ip = ip
                min_distance = tmp_distance
        if ret_ip:
            self.sub_src_conn(ret_ip)
            return ret_ip
        else:
            return None

    def add_src(self, ip):
        if ip in self.pool:
            return None
        self.pool[ip] = {'ip': ip, 'conn': self.max_conn}
        logger.debug("%s 可用连接: %d", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return ip

    def del_src(self, ip):
        if ip in self.pool:
            self.pool.pop(ip)
            logger.debug("%s 可用连接: %d", self.pool[ip]['ip'], self.pool[ip]['conn'])

    def add_src_conn(self, ip, num=1):
        if ip not in self.pool:
            logger.debug('%s pool中不存在', ip)
            return
        if self.pool[ip]['conn'] >= self.max_conn:
            logger.debug('%s 可用连接数已达最大值', ip)
            return

        self.pool[ip]['conn'] += num
        logger.debug("%s 可用连接: %d", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return self.pool[ip]['conn']

    def sub_src_conn(self, ip, num=1):
        if ip not in self.pool:
            logger.debug('pool中不存在 %s', ip)
            return
        if self.pool[ip]['conn'] <= 0:
            logger.debug('%s 无可用连接', ip)
            return

        # print self.pool[ip]['conn'], num
        self.pool[ip]['conn'] -= num
        logger.debug("%s 可用连接: %d", self.pool[ip]['ip'], self.pool[ip]['conn'])
        return self.pool[ip]['conn']


if __name__ == "__main__":
    pool = SourcePool(4, ["1.1.1.1", "1.1.1.254", "1.1.2.10", "2.2.3.1"])
    print pool.get_src("1.1.2.254")