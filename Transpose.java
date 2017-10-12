

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Transpose {
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            int i = 0;
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(new IntWritable(i), new Text(String.valueOf(key.get()) + word));
                i++;
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //We need the size of the iterable
            int size = 0;
            for(Text val : values){
                size ++;
            }
            String[] linetowrite = new String[size];
            for (Text val : values) {
                int index = Integer.parseInt(val.toString().split(" ")[0]);
                linetowrite[index] = val.toString().split(" ")[1];
            }
            String line = "";
            for (String word : linetowrite) {
                line += word + ",";
            }
                context.write(new Text(key.toString()), new Text(line));

        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "tanspose");

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }



}
