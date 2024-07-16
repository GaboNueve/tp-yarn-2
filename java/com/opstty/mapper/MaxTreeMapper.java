package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxTreeMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private Text districtCount = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        districtCount.set(value);
        context.write(NullWritable.get(), districtCount);
    }
}