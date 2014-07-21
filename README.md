TreePush
===========
一个用于向大量服务器同步文件的推送工具

--------------------

## 实现

1. treepush 根据参数初始化源节点及目标节点列表
2. treepush 从目标节点列表及源节点列表中各取出一个IP(使用 dip, sip 表示)
3. 连接到 sip, 执行推送文件到 dip 的命令
4. 文件推送完成，将 sip, dip 重新添加到源节点列表(推送过程多节点并行进行)
5. 重复 2~4，直到目标节点列表为空

## 优势

- 随着源节点的增加，更新过程会越来越快，实现更新速度的指数级增涨
- 使用自定义的文件推送指令，可基于 scp、rsync 进行文件的推送
- 不需要在所有节点上部署，只需要任意找一台作为控制机安装即可

## 使用示例

```Shell
# 以 127.0.0.1 为源，使用 rsync 向 hosts.txt 中的目标节点推送文件(已部署 ssh 密钥认证)
treepush 'rsync -avz -e "ssh -o StrictHostKeyChecking=no -p $TPUSH_PORT" /data/datafile $TPUSH_USER@$TPUSH_HOST:/data/datafile' -s 127.0.0.1 -l hosts.txt

# 指定 hosts.txt 第 2,3,4 列分别为 host,port,user(默认格式为第 1,2,3 列分别对应 host,port,user)
treepush 'rsync -avz -e "ssh -o StrictHostKeyChecking=no -p $TPUSH_PORT" /data/datafile $TPUSH_USER@$TPUSH_HOST:/data/datafile' -s 127.0.0.1 -l hosts.txt -f 2:host,port,user

# 指定第 4,1,3 列分别为 port,user,host
treepush 'rsync -avz -e "ssh -o StrictHostKeyChecking=no -p $TPUSH_PORT" /data/datafile $TPUSH_USER@$TPUSH_HOST:/data/datafile' -s 127.0.0.1 -l hosts.txt -f 4:port,1:user,3:host

# 额外命名第 5 列为 path(会生成一个对应的环境变量 TPUSH_PATH)，并根据此变量推送文件
treepush 'rsync -avz -e "ssh -o StrictHostKeyChecking=no -p $TPUSH_PORT" /data/datafile $TPUSH_USER@$TPUSH_HOST:$TPASH_PATH/datafile' -s 127.0.0.1 -l hosts.txt -f 4:port,1:user,3:host,5:path

# 使用 scp 推送文件
treepush 'scp -o StrictHostKeyChecking=no -P $TPUSH_PORT /data/datafile $TPUSH_USER@$TPUSH_HOST:/data/datafile' -s 127.0.0.1 -l hosts.txt

# 指定重试次数(-r)和一个源节点的并发连接数(-m)
treepush 'scp -o StrictHostKeyChecking=no -P $TPUSH_PORT /data/datafile $TPUSH_USER@$TPUSH_HOST:/data/datafile' -s 127.0.0.1 -l hosts.txt -r 3 -m 4

```

### 各选项的说明:
```text
  -h, --help            show this help message and exit
  -d, --debug           开启 debug
  -l LISTFILE, --list=LISTFILE
                        目标服务器列表
  -f FORMAT, --format=FORMAT
                        指定列表文件各字段的名字，使用 "," 分隔字段名,
                        连接多个逗号可用于跳过字段指定的字段名会被变成全大写并加上
                        "TPUSH_"前缀添加到环境变量中。字段名支持使用 "#:<name>"
                        直接指定第#个字段对应的名字。(host,port,user 这三个字段固定对应: 主机,端口,用户名)
  -s SOURCE, --source=SOURCE
                        版本源机器列表(1.1.1.1,2.2.2.2)
  -m MAX_CONN, --max=MAX_CONN
                        单IP并发连接数限制(default:4)
  -r RETRY, --retry=RETRY
                        单个IP出错后的重试次数(default:0)
  --logdir=LOGDIR       日志输出目录(default:logs)

```

## 其他

- 测试环境: CentOS 6.3, Python2.7
