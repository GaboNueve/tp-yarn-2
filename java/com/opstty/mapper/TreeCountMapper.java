package com.opstty.mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TreeCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text district = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().contains("header_column_name")) {
            return;
        }

        // Assuming CSV format and district is in the 2nd column (adjust index as needed)
        String[] fields = value.toString().split(",");
        if (fields.length > 1) {
            district.set(fields[1]);
            context.write(district, one);
        }
    }
}
