package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.IOException;

public class WordRanker {

    public static class RankMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable count = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Expecting input as: "word \t count"
            String[] tokens = value.toString().split("\\t");

            if (tokens.length == 2) {
                try {
                    word.set(tokens[0]);  // Set the word
                    count.set(Integer.parseInt(tokens[1]));  // Parse the count
                    context.write(count, word);  // Emit count as key, word as value
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing count for input: " + value.toString());
                }
            } else {
                System.err.println("Skipping malformed line: " + value.toString());
            }
        }
    }

    public static class RankReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        private int rank = 1;  // Start ranking from 1

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Since the input is sorted in descending order by count (key),
            // we just assign the rank (ID) to each word.
            for (Text value : values) {
                context.write(new IntWritable(rank), value);  // Emit rank (ID) and word
                rank++;  // Increment rank for the next word
            }
        }
    }

    // Comparator for sorting the word counts in descending order
    public static class DescendingIntComparator extends WritableComparator {
        protected DescendingIntComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable k1 = (IntWritable) w1;
            IntWritable k2 = (IntWritable) w2;
            return -1 * k1.compareTo(k2);  // Multiply by -1 to reverse the order
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Ranker");

        job.setJarByClass(WordRanker.class);
        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);

        // Set Mapper output key/value types
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Set final output key/value types
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Use the custom comparator to sort by count in descending order
        job.setSortComparatorClass(DescendingIntComparator.class);

        // Ensure that there is only 1 reducer, so the output will be in one file
        job.setNumReduceTasks(1);

        // Set input and output paths (arguments)
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
