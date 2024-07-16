package com.opstty.reducer;
import Writable.AgeDistrictWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OldestTreeReducer extends Reducer<NullWritable, AgeDistrictWritable, Text, IntWritable> {
    private Text resultDistrict = new Text();
    private IntWritable resultAge = new IntWritable();

    @Override
    protected void reduce(NullWritable key, Iterable<AgeDistrictWritable> values, Context context) throws IOException, InterruptedException {
        int maxAge = Integer.MIN_VALUE;
        String district = "";

        for (AgeDistrictWritable val : values) {
            if (val.getAge().get() > maxAge) {
                maxAge = val.getAge().get();
                district = val.getDistrict().toString();
            }
        }

        resultDistrict.set(district);
        resultAge.set(maxAge);
        context.write(resultDistrict, resultAge);
    }
}
