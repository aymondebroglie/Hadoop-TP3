

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

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");
            int i = 0;
            for (String word : words) {
                //The key i is the column number
                //The value is the byte offset + the value
                context.write(new IntWritable(i), new Text(String.valueOf(key.get()) + " " + word));
                i++;
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //We need to sort by offset that way we keep the line order
            //A tree map is naturally sorted
            TreeMap<Integer,String> map = new TreeMap<>();
            for(Text val : values){
                String[] input = val.toString().split(" ");
                map.put(Integer.parseInt(input[0]), input[1]);
            }

            //The sorting is done structurally
            String line = "";
            for (String value : map.values()) {
                line += value + ",";
            }
            //Getting rid of the last comma
                line = line.substring(0,line.length()-1);
                context.write(new Text(key.toString()), new Text(line));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "transpose");
        job.setJarByClass(Transpose.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(CSVOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }



}
