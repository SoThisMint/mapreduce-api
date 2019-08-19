package com.mint.mr;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @program: mapreduce-api
 * @description:
 * @author: mint
 * @create: 2019-08-19 23:42
 **/
public class WordCountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //设置Job方法入口的去doing类
        job.setJarByClass(WordCountDriver.class);
        //设置Mapper组件类
        job.setMapperClass(WordCountMapper.class);
        //设置Mapper的输出key类型
        job.setMapOutputKeyClass(Text.class);
        //设置Mapper的输出value类型
        job.setMapOutputKeyClass(IntWritable.class);
        //设置reduce组件类
        job.setReducerClass(ScoreReduce.class);
        //设置reduce的输出key类型
        job.setOutputKeyClass(Text.class);
        //设置reduce的输出value类型
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job,
                new Path("hdfs://192.168.32.168:9000/score"));
        //设置输出结果路径，要求路径事先不存在
        FileOutputFormat.setOutputPath(job,new Path("hdfs://192.168.32.168:9000/score/result"));

        //提交job
        job.waitForCompletion(true);
    }
}
