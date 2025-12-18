package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;


public class collocationExtraction {

    public static class MapperBasic extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private final Text outWord = new Text();
        private final Set<String> stop = new HashSet<>(); // hash for stop words
        private String language;

        protected void setup(Context context) throws IOException {
            language = context.getConfiguration().get("language", "unknown");
            String stopPath = context.getConfiguration().get("stopwords");
            if (stopPath == null) return;

            Path p = new Path(stopPath);
            FileSystem fs = p.getFileSystem(context.getConfiguration());

            try (FSDataInputStream in = fs.open(p);                                     //Open this file (on fs - disk / HDFS / S3 ) and give me a stream to read its bytes. FSDataInputStream-a Hadoop input stream you can read line-by-line
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (!line.isEmpty()) stop.add(line);
                }
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\t"); // splits by tab
            if (parts.length < 4) return;
            String ngram = parts[0];        // bigram
            String [] words= ngram.split(" ");
            if (words.length != 2 || stop.contains(words[1]) || stop.contains(words[0])) return; // only bigrams.

            int year = Integer.parseInt(parts[1]);      // year
            int matchCount = Integer.parseInt(parts[2]);     // count
            int decadeStart = (year / 10) * 10; // 1997/10=199*10=1990

            context.write(new Text(language + "|" + decadeStart + "|" + words[0] + "|" + words[1]), new IntWritable(matchCount)); //c12
            context.write(new Text(language + "|" + decadeStart + "|" + words[0] + "|" + "*"), new IntWritable(matchCount)); // c1
            context.write(new Text(language + "|" + decadeStart + "|" + "*" + "|" + words[1]), new IntWritable(matchCount)); //c2
            context.write(new Text(language + "|" + decadeStart + "|" + "*" + "|" + "*"), new IntWritable(matchCount)); //N

        }
    }

    public static class ReducerBasic extends Reducer<Text, IntWritable, Text, IntWritable> {
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



    public static class MapperLLR extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim(); // trim -removes whitespace from both ends of this string and returns a new string, without modifying the original string
            if (line.isEmpty()) return;
            String[] keyVal = line.split("\t");
            if (keyVal.length != 2) return;
            String keyStr = keyVal[0]; // eng|1990|w1|w2
            String countStr = keyVal[1]; // count of w1w2,w1,w2,N

            String[] parts = keyStr.split("\\|"); // split by |
            if (parts.length != 4) return;
            String language = parts[0];
            String decade   = parts[1];
            String w1        = parts[2];  // w1 or *
            String w2       = parts[3];  // w2 or *
            context.write(new Text(language + "|" + decade), new Text(w1 + "|" + w2 + "|" + countStr));
        }
    }

    public static class ReducerLLR extends Reducer<Text, Text, Text, Text> {
        private final HashMap<String,Integer> bigramCount = new HashMap<>();
        private final HashMap<String,Integer> leftCount = new HashMap<>();
        private final HashMap<String,Integer> rightCount = new HashMap<>();
        private final IntWritable Ncount = new IntWritable();

        @Override
        protected void reduce(Text key, Text value, Context context)
                throws IOException, InterruptedException {
            String [] val=value.toString().split("\\|");
            String w1=val[0];
            String w2=val[1];
            int count=Integer.parseInt(val[2]);

            if(w1.equals("*")){
                if(w2.equals("*")){Ncount.set(Ncount.get()+count);}
                else{
                    rightCount.put(w2,count);
                }
            }
            else if(w2.equals("*")){leftCount.put(w1,count);}
            else{
                bigramCount.put(w1 + "|" + w2,count);
            }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: collocationExtraction <input> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        //stop.txt configuration
        String sp = System.getProperty("stopwords");
        if (sp != null) conf.set("stopwords", sp);

        //language configuration
        String language = System.getProperty("language");
        if (language != null) conf.set("language", language);

        // FORCE LOCAL MODE
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.jobtracker.address", "local"); // helps on some setups

        Job job = Job.getInstance(conf, "WordCount"); // todo- change name of job
        job.setJarByClass(collocationExtraction.class);

        // also force it on the job config (so it 100% sticks)
        job.getConfiguration().set("mapreduce.framework.name", "local");
        job.getConfiguration().set("fs.defaultFS", "file:///");
        job.getConfiguration().set("mapreduce.jobtracker.address", "local");

        System.out.println("framework=" + job.getConfiguration().get("mapreduce.framework.name"));
        System.out.println("fs=" + job.getConfiguration().get("fs.defaultFS"));

        job.setMapperClass(MapperBasic.class);
        job.setReducerClass(ReducerBasic.class);

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