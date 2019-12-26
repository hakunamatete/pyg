package com.lxl.hadoop.cm;

import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiCluster;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.v18.RootResourceV18;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;

public class ClouderaManagerCluster {

    static RootResourceV18 apiRoot;

    static {

        ClouderaManagerClientBuilder clientBuilder = new ClouderaManagerClientBuilder();
        clientBuilder.withHost("cdh01");
        clientBuilder.withPort(7180);
        clientBuilder.withUsernamePassword("admin","admin");
        apiRoot = clientBuilder.build().getRootV18();
    }

    public static void main(String[] args) {
        getAllCluster();
    }

    public static void getAllCluster(){
        System.out.println("开始测试的时间为{},**************开始测试获取ClouderaManager集群信息**************"+now());
        ApiClusterList apiClusterList = apiRoot.getClustersResource().readClusters(DataView.FULL);
        System.out.println("ClouderaManager 共管理了{}个集群"+apiClusterList.getClusters().size());
        for(ApiCluster apiCluster : apiClusterList){
            ApiCluster apiCluster1 = apiRoot.getClustersResource().readCluster(apiCluster.getName());
            System.out.println("集群名称："+apiCluster1.getName());
            System.out.println("集群显示名称："+apiCluster1.getDisplayName());
            System.out.println("CDH 版本："+apiCluster.getFullVersion());
            System.out.println("ClusterUrl："+apiCluster1.getClusterUrl());
            System.out.println("HostUrl："+apiCluster1.getHostsUrl());
            System.out.println("Cluster Uuid："+apiCluster1.getUuid());
            System.out.println("集群运行状态："+apiCluster1.getEntityStatus());
        }
        System.out.println("结束测试的时间为{},**************结束测试获取ClouderaManager集群信息**************"+now());
    }

    public static String now() {
        GregorianCalendar calenda = new GregorianCalendar();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(calenda.getTime());
    }

}
