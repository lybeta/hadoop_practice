package cn.skyliuyang.iHadoop.kpi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import cn.skyliuyang.iHadoop.util.UserAgent;

public class KPIUA { 

    public static class KPIUAMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
    	private Text userAgent = new Text();
        private IntWritable one = new IntWritable(1);
        String uaInfo = "";

        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterPVs(value.toString());
            if (kpi.isValid()) {
            	uaInfo = kpi.getHttp_user_agent();
            	UserAgent ua = new UserAgent(uaInfo.toLowerCase());
            	if(ua.detectMSIE6()){ userAgent.set("IE6"); }
            	if(ua.detectMSIE7()){ userAgent.set("IE7"); }
            	if(ua.detectMSIE8()){ userAgent.set("IE8"); }
            	if(ua.detectMSIE9()){ userAgent.set("IE9"); }
            	if(ua.detectMSIE10()){ userAgent.set("IE10"); }
            	if(ua.detectFirefox()){ userAgent.set("FireFox"); }
            	if(ua.detectChrome()){ userAgent.set("Chrome"); }
            	if(ua.detectOpera()){ userAgent.set("Opera"); }
            	if(ua.detectSafari()){ userAgent.set("Safari"); }
            	if(userAgent.getLength()<1){ userAgent.set("Other"); }
                output.collect(userAgent, one);
            }
        }
    }

    public static class KPIUAReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            result.set(sum);
            output.collect(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        String input = "hdfs://h10:9000/user/hdfs/log_kpi/";
        String output = "hdfs://h10:9000/user/hdfs/log_kpi/ua";

        JobConf conf = new JobConf(KPIUA.class);
        conf.setJobName("KPIUA");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");
        conf.addResource("classpath:/hadoop/master");
        conf.addResource("classpath:/hadoop/slaves");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(KPIUAMapper.class);
        conf.setCombinerClass(KPIUAReducer.class);
        conf.setReducerClass(KPIUAReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);
    }

}