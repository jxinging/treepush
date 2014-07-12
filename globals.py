# coding: utf8
### 全局变量

__author__ = 'JinXing'

import logging

# 同一台源机器最大并发连接数
DEFAULT_MAX_CONN = 4

G_OPTIONS = None
G_LOG_DIR = 'logs'

LOG_SUCCESS = logging.ERROR+1
LOG_FAIL = logging.ERROR+2
__log_format = '%(asctime)s [%(levelname)s] # %(message)s'
logging.basicConfig(format=__log_format, datefmt='%m/%d %H:%M:%S')
logging.addLevelName(LOG_SUCCESS, "\033[1;32mSUCCESS\033[0m")
logging.addLevelName(LOG_FAIL, "\033[5;31mFAIL\033[0m")
G_LOGGER = logging.getLogger()
G_LOGGER = None