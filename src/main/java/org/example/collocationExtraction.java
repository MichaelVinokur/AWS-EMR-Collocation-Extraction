package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class collocationExtraction {

    public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text outWord = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t"); // splits by tab
            if (parts.length < 4) return;

            String ngram = parts[0];        // bigram
            String [] words= ngram.split(" ");
            if (words.length != 2) return; // only bigrams.
            int year = Integer.parseInt(parts[1]);      // year
            int matchCount = Integer.parseInt(parts[2]);     // count

            int decadeStart = (year / 10) * 10; // 1997/10=199*10=1990

            context.write(new Text(decadeStart + "|" + words[0] + "|" + words[1]), new IntWritable(matchCount));

        }
    }

    public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable outCount = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            outCount.set(sum);
            context.write(key, outCount);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: collocationExtraction <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        // FORCE LOCAL MODE
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.jobtracker.address", "local"); // helps on some setups

        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(collocationExtraction.class);

        // also force it on the job config (so it 100% sticks)
        job.getConfiguration().set("mapreduce.framework.name", "local");
        job.getConfiguration().set("fs.defaultFS", "file:///");
        job.getConfiguration().set("mapreduce.jobtracker.address", "local");

        System.out.println("framework=" + job.getConfiguration().get("mapreduce.framework.name"));
        System.out.println("fs=" + job.getConfiguration().get("fs.defaultFS"));

        job.setMapperClass(MapperClass.class);
        job.setReducerClass(ReducerClass.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

// The dataset you are using is already a 2-gram dataset So every record’s ngram field is exactly two words.