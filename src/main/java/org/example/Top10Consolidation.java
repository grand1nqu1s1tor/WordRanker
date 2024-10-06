package org.example;

import java.io.IOException;
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

public class Top10Consolidation {

    // Mapper Class
    public static class ConsolidationMapper extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable count = new IntWritable();
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Split the line into word and count
            String[] tokens = value.toString().split("\\t");

            // Ensure that the line contains exactly two tokens: word and count
            if (tokens.length == 2) {
                try {
                    word.set(tokens[0]);  // Set the word
                    count.set(Integer.parseInt(tokens[1]));  // Parse the count
                    context.write(count, word);  // Emit count as key, word as value
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing count for input: " + value.toString());
                }
            } else {
                // Log and skip malformed lines
                System.err.println("Skipping malformed line: " + value.toString());
            }
        }
    }

    // Reducer Class
    public static class ConsolidationReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        private int topCount = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                if (topCount < 10) {
                    context.write(value, key);  // Output word and count
                    topCount++;
                }
            }
        }
    }

    // Custom Comparator for Sorting in Descending Order
    public static class DescendingComparator extends WritableComparator {
        protected DescendingComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            IntWritable int1 = (IntWritable) a;
            IntWritable int2 = (IntWritable) b;
            return -1 * int1.compareTo(int2);  // Reverse the order for descending sorting
        }
    }

    // Driver Method
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Words Consolidation");

        job.setJarByClass(Top10Consolidation.class);
        job.setMapperClass(ConsolidationMapper.class);
        job.setReducerClass(ConsolidationReducer.class);

        // Set Mapper output key/value types
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Set final output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Use the custom comparator to sort in descending order
        job.setSortComparatorClass(DescendingComparator.class);

        // Set the number of reducers to 1 to ensure only 10 final results
        job.setNumReduceTasks(1);

        // Set input and output paths (arguments)
        FileInputFormat.addInputPath(job, new Path(args[0]));  // Input from Job 2
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // Final output for top 10 words

        // Submit the job and wait for its completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
