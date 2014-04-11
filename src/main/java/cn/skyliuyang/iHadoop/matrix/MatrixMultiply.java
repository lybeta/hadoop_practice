package cn.skyliuyang.iHadoop.matrix;

import cn.skyliuyang.iHadoop.util.HdfsDAO;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by liuyang on 14-3-16.
 */
public class MatrixMultiply {


    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String dataFlag;//ma or mb

        private int rowNum = 4;
        private int colNum = 2;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            dataFlag = split.getPath().getName();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = Main.DELIMITER.split(value.toString());

            if("ma".equals(dataFlag)){
                String row = tokens[0];
                String col = tokens[1];
                String val = tokens[2];

                for(int i = 1; i <= colNum; i++){
                    Text k = new Text(row + "," +i);
                    Text v = new Text("A#" + col + "," + val);
                    context.write(k, v);
                    System.out.println(k.toString() + " " + v.toString());
                }
            }

            if("mb".equals(dataFlag)){
                String row = tokens[0];
                String col = tokens[1];
                String val = tokens[2];

                for(int i = 1; i <= rowNum; i++){
                    Text k = new Text(i + "," + col);
                    Text v = new Text("B#" + row + "," + val);
                    context.write(k, v);
                    System.out.println(k.toString() + " " + v.toString());
                }

            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<String, String> mapA = new HashMap<String, String>();
            Map<String, String> mapB = new HashMap<String, String>();

            System.out.print(key.toString() + ":");

            for(Text line : values){
                String value = line.toString();
                System.out.print("(" + value + ")");

                if(value.startsWith("A#")){
                    String[] kv = Main.DELIMITER.split(value.substring(2));
                    mapA.put(kv[0], kv[1]);
                }

                if(value.startsWith("B#")){
                    String[] kv = Main.DELIMITER.split(value.substring(2));
                    mapB.put(kv[0], kv[1]);
                }
            }

            int result = 0;
            Iterator<Map.Entry<String,String>> iterator = mapA.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String,String> map = iterator.next();
                String aKey = map.getKey();
                String aVal = map.getValue();
                String bVal = mapB.containsKey(aKey) ? mapB.get(aKey) : "0";
                result += Integer.parseInt(aVal) * Integer.parseInt(bVal);
            }

            context.write(key, new IntWritable(result));
            System.out.println();
        }
    }

    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
        JobConf conf = Main.config();

        String input = path.get("input");
        String input1 = path.get("input1");
        String input2 = path.get("input2");
        String output = path.get("output");

        HdfsDAO hdfs = new HdfsDAO(Main.HDFS, conf);
        hdfs.rmr(input);
        hdfs.mkdirs(input);
        hdfs.copyFile(path.get("ma"), input1);
        hdfs.copyFile(path.get("mb"), input2);

        Job job = new Job(conf);
        job.setJarByClass(MatrixMultiply.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));// 加载2个输入数据集
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }

}
