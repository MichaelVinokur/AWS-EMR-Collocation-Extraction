package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.*;
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

    public static class KeywordKey implements WritableComparable<KeywordKey> {
        public Text decade = new Text();
        public Text w1 = new Text();
        public Text w2 = new Text();
        public IntWritable type = new IntWritable(); // 0 for N, 1 for c1, 2 for c2, 3 for c12

        @Override
        public void write(DataOutput out) throws IOException {
            decade.write(out);
            w1.write(out);
            w2.write(out);
            type.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            decade.readFields(in);
            w1.readFields(in);
            w2.readFields(in);
            type.readFields(in);
        }

        public KeywordKey() {} // empty constructor

        @Override
        public int compareTo(KeywordKey other) {
            int res = this.decade.compareTo(other.decade); // first wee compare decade
            if (res != 0) return res;
            res = this.type.compareTo(other.type); //order inversion type compare
            if (res != 0) return res;
            res = this.w1.compareTo(other.w1); // word compare
            if (res != 0) return res;
            return this.w2.compareTo(other.w2);
        }
    }

    public static class DecadePartitioner extends Partitioner<KeywordKey, IntWritable> {
        @Override
        public int getPartition(KeywordKey key, IntWritable value, int numPartitions) {
            return (key.decade.hashCode() & Integer.MAX_VALUE) % numPartitions; // sends all keys with the same decade to the reducer
        }
    }

    public static class MapperBasic extends Mapper<LongWritable, Text,KeywordKey, IntWritable> {
        private final Set<String> stop = new HashSet<>(); // hash for stop words
        private String language;
        private final KeywordKey kKey = new KeywordKey();

        protected void setup(Context context) throws IOException {
            language = context.getConfiguration().get("language", "unknown");
            String stopPath = context.getConfiguration().get("stopwords");
            if (stopPath == null) return;

            Path p = new Path(stopPath);
            FileSystem fs = p.getFileSystem(context.getConfiguration());

            try (FSDataInputStream in = fs.open(p);//Open this file (on fs - disk / HDFS / S3 ) and give me a stream to read its bytes. FSDataInputStream-a Hadoop input stream you can read line-by-line
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

            kKey.decade.set(language + "|" + decadeStart);

            kKey.w1.set("*"); kKey.w2.set("*"); kKey.type.set(0);// N - Type 0
            context.write(kKey, new IntWritable(matchCount));

            kKey.w1.set(words[0]); kKey.w2.set("*"); kKey.type.set(1);// c1 - Type 1
            context.write(kKey, new IntWritable(matchCount));

            kKey.w1.set("*"); kKey.w2.set(words[1]); kKey.type.set(2);// c2 - Type 2
            context.write(kKey, new IntWritable(matchCount));

            kKey.w1.set(words[0]); kKey.w2.set(words[1]); kKey.type.set(3);// c12 - Type 3
            context.write(kKey, new IntWritable(matchCount));
        }
    }

    public static class ReducerBasic extends Reducer<KeywordKey , IntWritable, KeywordKey, IntWritable> {
        private final IntWritable outCount = new IntWritable();

        @Override
        protected void reduce(KeywordKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            outCount.set(sum);
            context.write(key, outCount);
        }
    }

    public static class ReducerLLR extends Reducer<KeywordKey, IntWritable, Text, Text> {
        private long currentN = 0;
        private final Map<String, Long> wordCounts = new HashMap<>();

        @Override
        protected void reduce(KeywordKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }

            int type = key.type.get();
            String w1 = key.w1.toString();
            String w2 = key.w2.toString();

            if (type == 0) {
                wordCounts.clear(); //clean the map
                currentN = sum;
            } else if (type == 1) {
                wordCounts.put(w1, (long) sum);
            } else if (type == 2) {
                wordCounts.put(w2, (long) sum);
            } else if (type == 3) {
                long k11 = sum;
                long c1 = wordCounts.getOrDefault(w1, 0L);
                long c2 = wordCounts.getOrDefault(w2, 0L);

                if (currentN > 0 && c1 > 0 && c2 > 0) {
                    long k12 = c1 - k11;
                    long k21 = c2 - k11;
                    long k22 = currentN - k11 - k12 - k21;

                    double score = llr(k11, k12, k21, k22);
                    context.write(new Text(key.decade + "|" + w1 + "|" + w2), new Text(String.valueOf(score)));
                }
            }
        }
    }


    public static class Mapper100 extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split("\t");
            if (parts.length != 2) return;

            String fullKey = parts[0];  //eng|1990|w1|w2
            String score = parts[1];    //LLR

            String[] keyParts = fullKey.split("\\|");
            if (keyParts.length < 2) return;

            String langAndDecade = keyParts[0] + "|" + keyParts[1];
            String w1w2Score = keyParts[2] + "|" + keyParts[3] + "|" + score;

            context.write(new Text(langAndDecade), new Text(w1w2Score));
        }
    }

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
        System.setProperty("hadoop.home.dir", "C:\\hadoop");
        if (args.length < 4) {
            System.err.println("Usage: collocationExtraction <input> <output> <language> <stopWordsPath>");
            System.exit(2);
        }
        String inputPath = args[0];
        String finalOut  = args[1];
        String lang = args[2]; // english / hebrew
        String stopPath = args[3]; // s3://.../stopwords.txt
        //temporery folders to hold output of job 1 and job 2
        String out1 = finalOut + "_job1";
        String out2 = finalOut + "_job2";
        Configuration conf = new Configuration();
        conf.set("language", lang);
        conf.set("stopwords", stopPath);

        //JOB1
        Job job1 = Job.getInstance(conf, "JOB1-COUNTS");
        job1.setJarByClass(collocationExtraction.class);

        job1.setMapperClass(MapperBasic.class);
        job1.setCombinerClass(ReducerBasic.class); // added combiner for efficiency
        job1.setReducerClass(ReducerBasic.class);

        job1.setInputFormatClass(SequenceFileInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);

        job1.setMapOutputKeyClass(KeywordKey.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(KeywordKey.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setPartitionerClass(DecadePartitioner.class); //new partition

        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(out1)); // the output will be to folder out1;

        if (!job1.waitForCompletion(true)) System.exit(1); // wait until finished so we can transfer to job 2 - LLR

        // job2 - LLR
        Job job2 = Job.getInstance(conf, "JOB2-LLR");
        job2.setJarByClass(collocationExtraction.class);

        job2.setMapperClass(Mapper.class); // just passes the key and the value as they are
        job2.setReducerClass(ReducerLLR.class);

        job2.setInputFormatClass(SequenceFileInputFormat.class);

        job2.setMapOutputKeyClass(KeywordKey.class);
        job2.setMapOutputValueClass(IntWritable.class);
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