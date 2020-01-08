import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class JoinJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance(getConf(), "Join");
        job.setJarByClass(getClass());
        Path usersInputPath = new Path("input\\users.csv");
        Path departmentsInputPath = new Path("input\\departments.csv");

        MultipleInputs.addInputPath(job, usersInputPath, TextInputFormat.class, Join.UserMapper.class);
        MultipleInputs.addInputPath(job, departmentsInputPath, TextInputFormat.class, Join.DepartmentMapper.class);
        job.setReducerClass(Join.UnionReducer.class);
        TextOutputFormat.setOutputPath(job, output);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

class Join {
    public static class UserMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 5)
                return;
            context.write(new Text(parts[4]), new Text("USER" + parts[3]));
        }
    }

    public static class DepartmentMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] department = value.toString().split(",");
            context.write(new Text(department[0]), new Text(department[1]));
        }
    }

    public static class UnionReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            String tempOne = "";
            ArrayList<String> tempTwo = new ArrayList<>();
            for (Text value : values) {
                if (value.toString().contains("USER")) {
                    tempTwo.add(value.toString().substring(4));
                } else {
                    tempOne = value.toString();
                }

            }
            if ((tempTwo.size() > 0) && (tempOne.length() > 0)) {
                    context.write(new Text(tempOne + " : "), new Text(String.valueOf(tempTwo)));
            }
        }
    }
}