package com.itheima.report.bean;

/**
 * 对前端接收到的数据进行封装
 * 统一的使用FastJSON转换为JSON字符串传递给kafka
 */
public class Message {

    // 消息体，JSON数据
    private String message;
    // 对点击的次数进行计数
    private Long count;
    // 表示事件时间
    private Long timestamp;

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Message{" +
                "message='" + message + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }
}
