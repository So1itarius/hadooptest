import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.HashSet;


public class LoginCountJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem hdfss = FileSystem.get(conf);
        if (hdfss.exists(output)) {
            hdfss.delete(output, true);
        }

        Job job = Job.getInstance(getConf(), "LoginCount");
        job.setJarByClass(getClass());
        TextInputFormat.addInputPath(job, new Path("input\\logs_example.csv"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LoginCount.LoginMapper.class);
        job.setReducerClass(LoginCount.LoginReducer.class);
        TextOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
class LoginCount{
    public static class LoginMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] arr= value.toString().split(",");
            context.write(new Text(arr[2]), new Text(arr[4]));

        }
    }

    public static class LoginReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            HashSet<String> set = new HashSet<>();
            for (Text value : values) {
                set.add(value.toString());
            }
            context.write(new Text(key.toString().split(" ")[0]), new Text(set.toString()));
        }
    }
}
