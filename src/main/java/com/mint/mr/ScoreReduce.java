package com.mint.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @program: mapreduce-api
 * @description:
 * @author: mint
 * @create: 2019-08-19 23:48
 **/
public class ScoreReduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int score = 0;
        int count = 0;
        for (IntWritable score1 : values) {
            score += Integer.valueOf(score1.toString());
            count++;
        }
        double averageScore = score / count;
        context.write(key, new IntWritable(Integer.valueOf(String.valueOf(averageScore))));
    }
}
