package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MaxHeightMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    private Text treeKind = new Text();
    private DoubleWritable treeHeight = new DoubleWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().contains("header_column_name")) {
            return;
        }

        // Assuming CSV format and tree kind is in the 4th column and height in the 5th column (adjust index as needed)
        String[] fields = value.toString().split(",");
        if (fields.length > 4) {
            treeKind.set(fields[3]);
            try {
                treeHeight.set(Double.parseDouble(fields[4]));
                context.write(treeKind, treeHeight);
            } catch (NumberFormatException e) {
                // Handle parse exception
            }
        }
    }
}
