
import java.io.DataOutputStream;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.RecordWriter;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//Custom output writer to write a .csv file
public class CSVOutputFormat extends FileOutputFormat<Text, Text> {

    protected static class CSVRecordWriter extends RecordWriter<Text, Text> {

        private DataOutputStream out;

        public CSVRecordWriter(DataOutputStream out) throws IOException

        {

            this.out = out;

        }


        public synchronized void write(Text key, Text value) throws IOException

        {
            out.writeBytes(value.toString() + "\n");

        }

        public synchronized void close(TaskAttemptContext job) throws IOException {
            out.close();
        }

    }

    public RecordWriter<Text, Text> getRecordWriter(

            TaskAttemptContext job)

            throws IOException {

        String file_extension = ".csv";

        Path file = getDefaultWorkFile(job, file_extension);

        FileSystem fs = file.getFileSystem(job.getConfiguration());

        FSDataOutputStream fileOut = fs.create(file, false);

        return new CSVRecordWriter(fileOut);

    }

}