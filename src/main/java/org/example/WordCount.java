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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Regular expression to match words or individual punctuation marks
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;  // Skip empty lines
            }

            // Regular expression: Match words and punctuation as separate tokens
            String[] tokens = line.split("(?=[,.!?()\\[\\]{}])|\\s+|(?<=[,.!?()\\[\\]{}])");

            // Process each token
            for (String token : tokens) {
                if (!token.trim().isEmpty()) {  // Ignore empty tokens
                    word.set(token);  // Set the token (word or punctuation)
                    context.write(word, one);  // Emit the token with count 1
                }
            }
        }
    }

    // Reducer Class
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        // Specify the JAR file that contains your driver, mapper, and reducer
        job.setJarByClass(WordCount.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // Set Output key/value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Run the job and wait for its completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
