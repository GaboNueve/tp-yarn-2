package com.opstty.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpeciesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    private Text species = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header row
        if (key.get() == 0 && value.toString().contains("header_column_name")) {
            return;
        }

        // Assuming CSV format and species name is in the 3rd column (adjust index as needed)
        String[] fields = value.toString().split(",");
        if (fields.length > 2) {
            species.set(fields[2]);
            context.write(species, NullWritable.get());
        }
    }
}
