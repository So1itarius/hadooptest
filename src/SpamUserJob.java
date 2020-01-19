import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

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
            context.write(new Text(splt[2]+" "+splt[3]), new Text(splt[4]+" "+splt[5]));
        }
    }

    public static class LoginReducer
            extends Reducer<Text, Text, Text, Text> {
        Comparator<String []> comparator = new Comparator<String []>() {
            public int compare(String [] left, String [] right) {
                return Integer.compare(Integer.parseInt(left[1]), Integer.parseInt(right[1]));
            }
        };
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            ArrayList<String[]> pair = new ArrayList<>();
            for (Text value : values) {
                DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                Date date = null;
                try { date = dateFormat.parse(value.toString().split(" ",2)[1]); } catch (ParseException e) { e.printStackTrace(); }
                long unixTime = (Objects.requireNonNull(date)).getTime() /1000;

                String [] arr = {value.toString().split(" ")[0], String.valueOf(unixTime)};
                pair.add(arr);

            }
            pair.sort(comparator.reversed());

            if (key.toString().split(" ")[1].equals("LOGIN")) {
               for (int i=0;i<pair.size()-1;i++) {
                    if (!pair.get(i)[0].equals(pair.get(i+1)[0])
                            & Integer.parseInt(pair.get(i)[1])-Integer.parseInt(pair.get(i+1)[1])<=2){
                        context.write(key,new Text("difference between connections of different users: "+
                                String.valueOf(Integer.parseInt(pair.get(i)[1])-Integer.parseInt(pair.get(i+1)[1]))+" sec"));
                    }
               }
            }
        }
    }
}

