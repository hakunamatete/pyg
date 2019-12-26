package com.itheima.realprocess.bean

case class Message (count:Long, // 点击次数
                    timestamp:Long,//事件时间
                    clicklog:ClickLog)// 点击流日志样例类