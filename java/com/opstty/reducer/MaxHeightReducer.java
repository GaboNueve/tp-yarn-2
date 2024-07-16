package com.opstty.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxHeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private DoubleWritable maxHeight = new DoubleWritable();

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double max = Double.NEGATIVE_INFINITY;
        for (DoubleWritable val : values) {
            if (val.get() > max) {
                max = val.get();
            }
        }
        maxHeight.set(max);
        context.write(key, maxHeight);
    }
}
