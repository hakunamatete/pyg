# 电商模拟PVUV的数据测试（hadoop hbase flink zookeeper kafka）
## 本地虚拟机和客户机安装
### 所需软件链接：https://pan.baidu.com/s/1RbmWAmts9vyuz76oMTVw3A 提取码：8b4p
1.安装虚拟机：VMware-workstation-full-11.1.2-windows-x86_644.rar
傻瓜式安装：略。

2.安装客户机：CentOS-7-x86_64-Minimal-1511.iso
     vware文件菜单-->新建虚拟机->典型安装
     安装程序光盘镜像文件，指定CentOS-7-x86_64-Minimal-1511.iso
     设置账号密码。

全名：hadoop
账号密码：hadoop/hadoop

3.设置网络：/etc/sysconfig/network-scripts/ifcfg-eno16777736

$>vi /etc/sysconfig/network-scripts/ifcfg-eno16777736

TYPE=Ethernet
BOOTPROTO=none
DEFROUTE=yes
IPV4_FAILURE_FATAL=no
IPV6INIT=no
IPV6_AUTOCONF=no
IPV6_DEFROUTE=no
IPV6_FAILURE_FATAL=no
NAME=eno16777736
UUID=474067f4-5497-4ee6-90bf-3858fc567963
DEVICE=eno16777736
ONBOOT=yes
PEERDNS=yes
PEERROUTES=yes
IPV6_PEERDNS=no
IPV6_PEERROUTES=no
IPADDR=192.168.198.150
GATEWAY=192.168.198.2
DNS=192.168.198.2

Esc键
$>wq!


4.修改域名文件：[/etc/resolv.conf]
$>vi /etc/resolv.conf
    nameserver 192.168.242.2  桥接段：202.106.0.20

5.重启网络服务，使用root权限
$>sudo service network restart
$>ping www.baidu.com

6.yum安装。root权限
$>su root
$>yum -y install nano net-tools nc rsync

7.修改配置文件，
1.centos用户可以执行sudo命令：[/etc/sudoers]
$>sudo nano /etc/sudoers
root      ALL=(ALL)       ALL
centos  ALL=(ALL)       ALL

2.修改主机名:[/etc/hostname]
$>sudo nano /etc/hostname
s150

3.nano行数显示:[/etc/nanorc]
$>sudo nano /etc/nanorc
set const  去掉#

4.修改hosts[/etc/hosts]
$>sudo nano /etc/hosts
127.0.0.1 localhost
192.168.240.150 s150
192.168.240.151 s151
192.168.240.152 s152
192.168.240.153 s153
192.168.240.154 s154


8.安装WinSCP(windows)
$>sudo mkdir /soft        创建soft目录。
$>sudo chown centos:centos /soft            修改目录拥有者。centos:当前用户
通过WinSCP将jdk和hadoop放到soft目录下。

9.解压缩gz文件。
$>cd /soft
$>tar -xzvf jdk-8u65-linux-x64.tar.gz
$>tar -xzvf hadoop-2.7.3.tar.gz
$>ln -s jdk1.8.0_65 jdk                    //创建软连接
$>ln -s hadoop-2.7.3 hadoop

10.修改hadoop配置文件，手动指定JAVA_HOME环境变量
[${hadoop_home}/etc/hadoop/hadoop-env.sh]
$> nano /soft/hadoop/etc/hadoop/hadoop-env.sh
export JAVA_HOME=/soft/jdk
$>rm -rf *.gz

11.配置环境变量[ /etc/profile]
在末尾追加：
#设置当前路径显示完全路径.【可选】
export PS1='[\u@\h `pwd`]\$'
#jdk
export JAVA_HOME=/soft/jdk
export PATH=$PATH:$JAVA_HOME/bin
#hadoop
export HADOOP_HOME=/soft/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

环境变量立即生效
$>source /etc/profile
验证是否安装成功
$>java -version
$>hadoop version

12.关闭防火墙：
$>sudo chkconfig firewalld    on                       //"开启自启"启用
$>sudo chkconfig firewalld    off                          //"开启自启"禁用
$>sudo systemctl enable firewalld.service         //"开机启动"启用
$>sudo systemctl disable firewalld.service        //"开机自启"禁用
$>sudo systemctl start firewalld.service            //启动防火墙
$>sudo systemctl stop firewalld.service            //停止防火墙
$>sudo systemctl status firewalld.service         //查看防火墙状态

13.克隆之前最好是安装好了hadoop。不过也可以先克隆，后面安装之后分发即可。
