import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SpamUserJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        TextInputFormat.addInputPath(job, new Path("input\\logs_example.csv"));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(SpamIp.LoginMapper.class);
        job.setReducerClass(SpamIp.LoginReducer.class);
        TextOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;

    }
}
class SpamIp{
    public static class LoginMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] splt = value.toString().split(",");
            context.write(new Text(splt[4]+" "+splt[3]), new Text(splt[2]+" "+splt[5]));
        }
    }

    public static class LoginReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            HashSet<String> tempMap = new HashSet<String>();
            ArrayList<Long> time = new ArrayList<>();
            HashMap<String, ArrayList<Long>> result = new HashMap<>();
            for (Text value : values) {
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                Date date = null;
                try { date = dateFormat.parse(value.toString().split(" ",2)[1]); } catch (ParseException e) { e.printStackTrace(); }
                long unixTime = (long) (Objects.requireNonNull(date)).getTime()/1000;

                tempMap.add(value.toString().split(" ")[0]);
                time.add(unixTime);

            }
            Collections.sort(time);
            Collections.reverse(time);

            if (//tempMap.size()>=3 &
                    ((time.get(0)-time.get(time.size()-1))/time.size())<=180
                    & key.toString().split(" ")[1].equals("LOGIN")) {
                context.write(key,new Text("AVGconnection: "+(time.get(0)-time.get(time.size()-1))/time.size()));
            }

        }
    }

}

