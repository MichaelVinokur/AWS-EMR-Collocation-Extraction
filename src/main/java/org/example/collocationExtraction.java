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
import java.util.*;


public class collocationExtraction {

    //this is the same formula calculation as for the classic Dunning collocation LLR but because when using Math.pow for large numbers it can become 0 this method will keep the actual numbers.
    static double xlogx(long x) {
        return (x == 0) ? 0.0 : x * Math.log(x);
    }

    static double llr(long k11, long k12, long k21, long k22) {
        long k1_ = k11 + k12;
        long k2_ = k21 + k22;
        long _k1 = k11 + k21;
        long _k2 = k12 + k22;
        long N   = k1_ + k2_;

        double res =
                xlogx(k11) + xlogx(k12) + xlogx(k21) + xlogx(k22)
                        - xlogx(k1_) - xlogx(k2_) - xlogx(_k1) - xlogx(_k2)
                        + xlogx(N);

        return 2.0 * res;
    }

    public static class MapperBasic extends Mapper<LongWritable, Text, Text, IntWritable> {
//        private static final IntWritable ONE = new IntWritable(1);
//        private final Text outWord = new Text();
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
    //key"eng|1990|w1|w2<tab>value:50 -> key: eng|1990 value: w1|w2|count


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
    // (key: language|decade) (value: w1|w2|count)
    public static class ReducerLLR extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashMap<String, Integer> bigramCount = new HashMap<>();
            HashMap<String, Integer> leftCount = new HashMap<>();
            HashMap<String, Integer> rightCount = new HashMap<>();
            long N = 0;

            for (Text t : values) {
                String[] val = t.toString().split("\\|");
                String w1 = val[0];   // w1 or *
                String w2 = val[1];   // w2 or *
                int c = Integer.parseInt(val[2]);

                if (w1.equals("*") && w2.equals("*")) N = c;
                else if (w2.equals("*")) leftCount.put(w1, c);
                else if (w1.equals("*")) rightCount.put(w2, c);
                else bigramCount.put(w1 + "|" + w2, c);
            }
            if (N == 0) return;

            for (var e : bigramCount.entrySet()) {
                String[] ab = e.getKey().split("\\|");
                String w1 = ab[0], w2 = ab[1];

                long k11 = e.getValue();  //k11=c1c2 the number of occurrences of w1w2 in the corpus
                long c1  = leftCount.getOrDefault(w1, 0); //c1 the number of occurrences of w1 in the corpus
                long c2  = rightCount.getOrDefault(w2, 0); ////c2 the number of occurrences of w2 in the corpus
                long k12 = c1 - k11;
                long k21 = c2 - k11;
                long k22 = N - k11 - k12 - k21;

                double score = llr(k11, k12, k21, k22);
                context.write(new Text(key.toString() + "|" + w1 + "|" + w2) , new Text(String.valueOf(score)));
            }
        }
    }


    public static class Mapper100 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] keyVal = line.split("\t"); // tab or space
            if (keyVal.length != 2) return;

            String[] parts = keyVal[0].split("\\|");
            if (parts.length != 4) return;
            String language = parts[0];
            String decade   = parts[1];
            String w1       = parts[2];
            String w2       = parts[3];
            String llr      = keyVal[1]; // 0.323... the log ratio

            context.write(new Text(language + "|" + decade),new Text(w1 + "|" + w2 + "|" + llr));
        }
    }
    // (key: language|decade) (value: w1|w2|llr)
    public static class Reducer100 extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            PriorityQueue<String> minHeap = new PriorityQueue<>(
                    100,
                    (a, b) -> {
                        double la = Double.parseDouble(a.split("\\|")[2]);
                        double lb = Double.parseDouble(b.split("\\|")[2]);
                        return Double.compare(la, lb);
                    }
            );//including a costum comperator that will be able to break down the value and comapre by LLR
            for (Text t : values) {
                String[] parts = t.toString().split("\\|");
                if (parts.length != 3) continue;

                double llr = Double.parseDouble(parts[2]);

                if (minHeap.size() < 100) {
                    minHeap.add(t.toString());
                } else {
                    double minLlr = Double.parseDouble(minHeap.peek().split("\\|")[2]);
                    if (llr > minLlr) {
                        minHeap.poll();//remove the head
                        minHeap.add(t.toString());
                    }
                }
            }
            // output sorted descending by LLR
            ArrayList<String> top = new ArrayList<>(minHeap);
            top.sort((a, b) -> {
                double la = Double.parseDouble(a.split("\\|")[2]);
                double lb = Double.parseDouble(b.split("\\|")[2]);
                return Double.compare(lb, la); // descending
            });

            for (String s : top) {
                // s = "w1|w2|llr"
                String[] parts = s.split("\\|");
                String w1 = parts[0];
                String w2 = parts[1];
                String llrStr = parts[2];

                context.write(new Text(key.toString() + "|" + w1 + "|" + w2), new Text(llrStr));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: collocationExtraction <input> <output>");
            System.exit(2);
        }
        String inputPath = args[0];
        String finalOut  = args[1];
        //temporery folders to hold output of job 1 and job 2
        String out1 = finalOut + "_job1";
        String out2 = finalOut + "_job2";
        Configuration conf = new Configuration();
        //the stopwords file will be named "stopwords" so we can use it from the system.
        String sp = System.getProperty("stopwords");
        if (sp != null) conf.set("stopwords", sp);

        String language = System.getProperty("language");
        if (language != null) conf.set("language", language);

        //related to local testing
        conf.set("mapreduce.framework.name", "local");
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.jobtracker.address", "local");

        //JOB1
        Job job1 = Job.getInstance(conf, "JOB1-COUNTS");
        job1.setJarByClass(collocationExtraction.class);

        job1.setMapperClass(MapperBasic.class);
        job1.setReducerClass(ReducerBasic.class);

        job1.setInputFormatClass(TextInputFormat.class);     // local testing
        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(out1)); // the output will be to folder out1;

        if (!job1.waitForCompletion(true)) System.exit(1); // wait until finished so we can transfer to job 2 - LLR

        // job2 - LLR
        Job job2 = Job.getInstance(conf, "JOB2-LLR");
        job2.setJarByClass(collocationExtraction.class);

        job2.setMapperClass(MapperLLR.class);
        job2.setReducerClass(ReducerLLR.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class); // you emit score as Text

        FileInputFormat.addInputPath(job2, new Path(out1));
        FileOutputFormat.setOutputPath(job2, new Path(out2));

        if (!job2.waitForCompletion(true)) System.exit(1);

        //job3 - top 100
        Job job3 = Job.getInstance(conf, "JOB3-TOP100");
        job3.setJarByClass(collocationExtraction.class);

        job3.setMapperClass(Mapper100.class);
        job3.setReducerClass(Reducer100.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(out2));
        FileOutputFormat.setOutputPath(job3, new Path(finalOut));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }

}

// The dataset you are using is already a 2-gram dataset So every record’s ngram field is exactly two words.