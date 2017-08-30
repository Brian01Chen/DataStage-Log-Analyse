#!/usr/bin/env  python
# coding:utf-8
# Author Summer
# Create date:2015-07-29
import datetime
import time
from fabric.context_managers import *
from fabric.contrib.console import confirm
from fabric.colors import *
from fabric.api import *

# 本地开发服务器信息
env.local_package_dir = '/web'
env.time = time.strftime("%Y%m%d")
env.local_bakcup_dir = '/tmp'
env.local_package_name = 'target'
# 远程服务器信息
env.hosts = ['192.168.0.133', '192.168.0.134', '192.168.0.135']
env.port = '3217'
env.user = 'root'
env.password = "123456"
env.remote_package_dir = '/web'


# 备份函数
@task
@runs_once
def backup_task():
    print
    yellow("<--------------开始备份新包-------------->")
    with lcd("%s" % env.local_package_dir):
        local("tar zcvf %s-%s.tar.gz %s" % (env.local_package_name, env.time, env.local_package_name))
    print
    blue("<------------新包备份创建成功------------->")


# 上传函数
@task
def put_task():
    print
    yellow("<-------------开始停止服务--------->")
    run('%s/%s/APIServer stop' % (env.local_package_dir, env.local_package_name))
    print
    blue("<-------------停止服务成功--------->")
    print
    yellow("<-------------删除旧包------------>")
    run('rm -fr %s/%s*' % (env.remote_package_dir, env.local_package_name))
    print
    blue("<-----------旧包删除成功----  ---->")

    print
    yellow("<-----------开始上传新包---  ----->")
    # run ("mkdir %s" %env.remote_package_dir)
    with lcd("%s" % env.local_package_dir):
        put('%s-%s.tar.gz' % (env.local_package_name, env.time),
            '%s/%s-%s.tar.gz' % (env.local_package_dir, env.local_package_name, env.time))
    print   ('<-----------新包上传成功---------->')

    run('tar zxf %s/%s-%s.tar.gz -C %s' % (
    env.local_package_dir, env.local_package_name, env.time, env.local_package_dir))
    print blue('<-------------解压成功------------>')

    with cd('%s/%s' % (env.remote_package_dir, env.local_package_name)):
        run('./APIServer start')
    print   blue('<-------APIServer服务运行成功------->')


@task
def start():
    backup_task()
    put_task()