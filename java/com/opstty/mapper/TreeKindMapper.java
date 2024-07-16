package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeKindMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text treeKind = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().contains("header_column_name")) {
            return;
        }

        // Assuming CSV format and tree kind is in the 4th column (adjust index as needed)
        String[] fields = value.toString().split(",");
        if (fields.length > 3) {
            treeKind.set(fields[3]);
            context.write(treeKind, one);
        }
    }
}
