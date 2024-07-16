package com.opstty.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    private DoubleWritable treeHeight = new DoubleWritable();
    private Text record = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().contains("header_column_name")) {
            return;
        }

        // Assuming CSV format and height in the 5th column (adjust index as needed)
        String[] fields = value.toString().split(",");
        if (fields.length > 4) {
            try {
                double height = Double.parseDouble(fields[4]);
                treeHeight.set(height);
                record.set(value);
                context.write(treeHeight, record);
            } catch (NumberFormatException e) {
                // Handle parse exception
            }
        }
    }
}
