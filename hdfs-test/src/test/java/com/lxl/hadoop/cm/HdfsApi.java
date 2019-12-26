package com.lxl.hadoop.cm;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class HdfsApi {

    String dfs_path = "hdfs://cdh01:8020";

    @Test
    public void mkdir() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", dfs_path);
        FileSystem fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/user/hadoop/myhadoop"));
    }

    @Test
    public void putFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", dfs_path);
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream out = fs.create(new Path("/user/hadoop/myhadoop/a.txt"));
        out.write("helloworld".getBytes());
        out.close();
    }

    @Test
    public void outText() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS",dfs_path);
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path("hdfs://cdh01:8020/user/hadoop/myhadoop/a.txt"));
        IOUtils.copyBytes(in,System.out,1024);
        IOUtils.closeStream(in);
    }

    @Test
    public void listFS() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", dfs_path);
        FileSystem fs = FileSystem.get(conf);
        printlnPath(fs,new Path("/user"));
    }

    /**
     * 递归输出hdfs目录结构
     */
    private void printlnPath(FileSystem fs , Path p){
        try {
            //输出路径
            System.out.println(p.toUri().toString());
            if(fs.isDirectory(p)){
                FileStatus[] files = fs.listStatus(p);
                if(files != null && files.length > 0){
                    for(FileStatus f : files){
                        Path p0 = f.getPath();
                        printlnPath(fs,p0);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void removeFile() throws Exception{
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", dfs_path);
        FileSystem fs = FileSystem.get(conf) ;
        Path p = new Path("/user/hadoop/myhadoop");
        fs.delete(p, true);
    }
}
