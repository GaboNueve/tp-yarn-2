package com.opstty.mapper;
import Writable.AgeDistrictWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OldestTreeMapper extends Mapper<LongWritable, Text, NullWritable, AgeDistrictWritable> {
    private AgeDistrictWritable ageDistrict = new AgeDistrictWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().contains("header_column_name")) {
            return;
        }

        // Assuming CSV format and age in the 6th column and district in the 2nd column (adjust index as needed)
        String[] fields = value.toString().split(",");
        if (fields.length > 5) {
            try {
                int age = Integer.parseInt(fields[5]);
                String district = fields[1];
                ageDistrict = new AgeDistrictWritable(age, district);
                context.write(NullWritable.get(), ageDistrict);
            } catch (NumberFormatException e) {
                // Handle parse exception
            }
        }
    }
}

