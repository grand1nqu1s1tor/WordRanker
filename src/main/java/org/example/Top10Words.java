package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Top10Words {

    public static class SortMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable count = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\\t");

            if (tokens.length == 2) {
                try {
                    word.set(tokens[0]);
                    count.set(Integer.parseInt(tokens[1]));  // Parse the count
                    context.write(count, word);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing count for input: " + value.toString());
                }
            } else {
                System.err.println("Skipping malformed line: " + value.toString());
            }
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        private int topCount = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (topCount < 10) {
                    context.write(value, key);
                    topCount++;
                }
            }
        }
    }

    public static class DescendingComparator extends WritableComparator {
        protected DescendingComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable int1 = (IntWritable) a;
            IntWritable int2 = (IntWritable) b;
            return -1 * int1.compareTo(int2);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Words");

        job.setJarByClass(Top10Words.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setSortComparatorClass(DescendingComparator.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
