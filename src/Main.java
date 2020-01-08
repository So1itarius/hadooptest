import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class Main {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        //Path usersInputPath = new Path("input\\users.csv");
        //Path departmentsInputPath = new Path("input\\departments.csv");
        //MultipleInputs.addInputPath(job, usersInputPath, TextInputFormat.class, JoinJob.UserMapper.class);
        //MultipleInputs.addInputPath(job, departmentsInputPath, TextInputFormat.class, JoinJob.DepartmentMapper.class);
        //job.setReducerClass(JoinJob.NameReducer.class);
        //HashMap<String, String> allUsers = new HashMap<String, String>();
        //TextInputFormat.addInputPath(job, new Path("input\\SalesJan2009.csv"));

        //TextInputFormat.addInputPath(job, new Path("input\\logs_example.csv"));


            //job1.setMapperClass(TF_IDF.TFIDFMapper.class);
            //job1.setReducerClass(TF_IDF.TFIDFReducer.class);
          //MultipleInputs.addInputPath(job, new Path("corpus\\doc*"), TextInputFormat.class, TF_IDF.TFIDFMapper.class);
          //MultipleInputs.addInputPath(job, new Path("corpus\\doc*"), TextInputFormat.class, TF_IDF.TokenizerMapper.class);
            //job.setReducerClass(TF_IDF.IntSumReducer.class);



        //job.setMapperClass(UniqueUserMapper.class);
        //job.setReducerClass(UniqueUserReducer.class);
        //job.setMapperClass(UserMapper.class);
        //job.setMapperClass(DepartmentMapper.class);
        //job.setReducerClass(MultipleOutputsReducer.class);
        //TextOutputFormat.setOutputPath(job, new Path("doc2.txt"));
        //job.setInputFormatClass(TextInputFormat.class);

        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(IntWritable.class);


        //FileInputFormat.setInputPaths(job, new Path("doc2.txt"));

        //job.setOutputValueClass(IntWritable.class);
        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(DataWritable.class);
        TextOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


    public static class UniqueUserMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            context.write(new Text(tokens[4]), new Text(tokens[6]));
        }
    }

    public static class UniqueUserReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> tempString = new ArrayList<>();
            for (Text value : values) {
                System.out.println("key: " + key);
                System.out.println(value.toString());
                tempString.add(value.toString());
            }
            context.write(key, new Text(tempString.toString()));

        }
    }

}




