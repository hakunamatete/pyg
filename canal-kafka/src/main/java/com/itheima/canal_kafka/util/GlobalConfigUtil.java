package com.itheima.canal_kafka.util;

import java.util.ResourceBundle;

public class GlobalConfigUtil {
    // 获取一个资源加载器
    // 资源加载器会自动去加载CLASSPAT中的application.properties配置文件
    public static ResourceBundle bundle = ResourceBundle.getBundle("application");
    public static String canalHost = bundle.getString("canal.host");
    public static String canalPort = bundle.getString("canal.port");
    public static String canalInstance = bundle.getString("canal.instance");
    public static String mysqlUsername = bundle.getString("mysql.username");
    public static String mysqlPassword = bundle.getString("mysql.password");
    public static String kafkaBootstrapServers = bundle.getString("kafka.bootstrap.servers");
    public static String kafkaZookeeperConnect = bundle.getString("kafka.zookeeper.connect");
    public static String kafkaInputTopic = bundle.getString("kafka.input.topic");

    public static void main(String[] args) {
        System.out.println(canalHost);
        System.out.println(canalPort);
        System.out.println(canalInstance);
        System.out.println(mysqlUsername);
        System.out.println(mysqlPassword);
        System.out.println(kafkaBootstrapServers);
        System.out.println(kafkaZookeeperConnect);
        System.out.println(kafkaInputTopic);
    }
}
