# coding: utf8
__author__ = 'JinXing'

import os
import struct
import socket
import tempfile
import subprocess
import logging


def subprocess_ssh(host, cmd, env=None, logfile=None, user=None, port=None):
    ssh_control_dir = os.path.join(tempfile.gettempdir(), '.tpush_ssh_control')
    if not os.path.exists(ssh_control_dir):
        os.mkdir(ssh_control_dir)
    ssh_control_socket_file = os.path.join(ssh_control_dir, '%s_%s.sock' % (host, str(port)))

    ssh_args = []
    if not os.path.exists(ssh_control_socket_file):
        ssh_args.append('-M')
    if port is not None:
        ssh_args.extend(['-p', str(port)])
    if user is not None:
        ssh_args.extend(['-l', user])
    ssh_args.append("-A")
    # ssh_args.append("-T")
    ssh_args.extend(['-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    '-o', 'ConnectTimeout=4',
                    '-o', 'ControlPath="%s"' % ssh_control_socket_file])
    ssh_args.append(host)

    remote_cmds = []
    if env:
        export_env = []
        for name, value in env.iteritems():
            export_env.append("%s=%s" % (name, value))
        # 环境变量最后要加一个 ";" 与命令分隔，否则命令中访问不到定义的环境变量
        remote_cmds.append(";".join(export_env)+";")
    # ssh_args.append("echo '%s:%s ssh connection success';" % (host, port))  # 用于判断连接在哪一层被断开
    # ssh_args.append("sleep $((RANDOM%10+5));")  # 如果命令执行过快会出错(连接过快，被拒绝), 这里添加一个随机延时
    remote_cmds.append(cmd)

    if logfile is not None:
        r_stdout = open(logfile, 'ab')
    else:
        r_stdout = subprocess.PIPE
    remote_cmd_str = ' '.join(remote_cmds)
    r_stdout.write('ssh '+' '.join(ssh_args)+remote_cmd_str + "\n")
    p = subprocess.Popen(['/usr/bin/ssh']+ssh_args+remote_cmds, shell=False, close_fds=True,
                         stdin=subprocess.PIPE, stdout=r_stdout, stderr=subprocess.STDOUT)
    return p


def create_listfile_by_optlist():
    raise


def ip2long(ip_string):
    return struct.unpack('!I', socket.inet_aton(ip_string))[0]


def long2ip(ip_int):
    return socket.inet_ntoa(struct.pack('!I', ip_int))


def get_subnet(ip):
    ip_split = ip.split('.')
    if len(ip_split) != 4:
        raise ValueError(u"Invalid ip address %s" % ip)
    subnet = '.'.join(ip_split[:3])    # 以IP的前3段做为网段
    return subnet


class Logger(logging.Logger):
    LOG_SUCCESS = logging.ERROR+1
    LOG_FAIL = logging.ERROR+2

    def __init__(self, name, level=logging.WARNING):
        logging.Logger.__init__(self, name, level)
        logging.addLevelName(self.LOG_SUCCESS, "\033[1;32mSUCCESS\033[0m")
        logging.addLevelName(self.LOG_FAIL, "\033[5;31mFAIL\033[0m")
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] # %(message)s')
        handler.setFormatter(formatter)
        self.addHandler(handler)

    def success(self, msg, *args, **kwargs):
        return self.log(self.LOG_SUCCESS, msg, *args, **kwargs)

    def fail(self, msg, *args, **kwargs):
        return self.log(self.LOG_FAIL, msg, *args, **kwargs)

logging.setLoggerClass(Logger)
logger = logging.getLogger("TPush")
logger.setLevel(logging.INFO)


def _get_format_dict(format_str):
    format_d = dict()
    num = 0
    for s in format_str.split(","):
        ss = s.split(":")
        if len(ss) == 1:
            name = ss[0]
            num += 1
        elif len(ss) == 2 and ss[0].isdigit():
            name = ss[1]
            num = int(ss[0])
        else:
            raise ValueError(u"Invalid format: %s", s)
        if len(name) == 0:
            continue
        assert name not in format_d
        format_d[name] = num-1
    return format_d


def parse_listfile(list_file, format_str):
    format_dict = _get_format_dict(format_str)
    env_dict = dict()
    with open(list_file, 'rb') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            td = dict()
            sl = line.split()
            for name, num in format_dict.iteritems():
                if sl[num].isdigit():
                    td[name] = int(sl[num])
                else:
                    td[name] = sl[num]
            host = sl[format_dict["host"]]
            env_dict[host] = td
    return env_dict


def tail_lines(filename, n=1, strip=True):
    with(open(filename, "r")) as f:
        r = []
        for line in f:
            if strip and not line.strip():
                continue
            if len(r) >= n:
                r.pop(0)
            r.append(line)
    return "".join(r)

if __name__ == "__main__":
    print parse_listfile("hosts.txt", "name,host,port")
    print tail_lines("hosts.txt", 2, False),
