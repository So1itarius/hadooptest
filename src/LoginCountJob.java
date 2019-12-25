import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;


public class LoginCountJob {

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
            context.write(key, new Text(set.toString()));
        }
    }
}
