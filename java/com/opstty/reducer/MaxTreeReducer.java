package com.opstty.reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxTreeReducer extends Reducer<NullWritable, Text, Text, IntWritable> {
    private Text maxDistrict = new Text();
    private IntWritable maxCount = new IntWritable();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        String district = "";

        for (Text val : values) {
            String[] fields = val.toString().split("\t");
            if (fields.length == 2) {
                int count = Integer.parseInt(fields[1]);
                if (count > max) {
                    max = count;
                    district = fields[0];
                }
            }
        }

        maxDistrict.set(district);
        maxCount.set(max);
        context.write(maxDistrict, maxCount);
    }
}
