package cn.skyliuyang.iHadoop.matrix;

import org.apache.hadoop.mapred.JobConf;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by liuyang on 14-3-16.
 */
public class Main {
    public static final String HDFS = "hdfs://h10:9000";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) {
        martrixMultiply();
    }

    public static void martrixMultiply() {
        Map<String, String> path = new HashMap<String, String>();
        path.put("ma", "logfile/matrix/ma.csv");// 本地的数据文件
        path.put("mb", "logfile/matrix/mb.csv");
        path.put("input", HDFS + "/user/hadoop/matrix");// HDFS的目录
        path.put("input1", HDFS + "/user/hadoop/matrix/ma");
        path.put("input2", HDFS + "/user/hadoop/matrix/mb");
        path.put("output", HDFS + "/user/hadoop/matrix/output");

        try {
            MatrixMultiply.run(path);// 启动程序
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

    public static JobConf config() {// Hadoop集群的远程配置信息
        JobConf conf = new JobConf(Main.class);
        conf.setJobName("MatrixMultiply");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/master");
        conf.addResource("classpath:/hadoop/slaves");
        return conf;
    }
}
