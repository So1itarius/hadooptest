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
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AVGPriceJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem hdfss = FileSystem.get(conf);
        if (hdfss.exists(output)) {
            hdfss.delete(output, true);
        }

        Job job = Job.getInstance(getConf(), "AVGPrice");
        job.setJarByClass(getClass());
        TextInputFormat.addInputPath(job, new Path("input\\SalesJan2009.csv"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(AVGPrice.AVGMapper.class);
        job.setReducerClass(AVGPrice.AVGReducer.class);
        TextOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

class AVGPrice {
    protected static String finder(String str) {
        Pattern pattern = Pattern.compile("\\d+/\\d+");
        Matcher matcher = pattern.matcher(str);
        while (matcher.find()) {
            str = matcher.group();
        }
        return str;
    }

    protected static String check(String str) {
        Pattern pattern = Pattern.compile("\"\\d+,\\d+\"");
        Matcher matcher = pattern.matcher(str);
        String s = "";
        while (matcher.find())
            s = matcher.group().replace("\"", "").replace(",", "");
        return str.replaceAll("\"\\d+,\\d+\"", s);
    }

    public static class AVGMapper
            extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.equals(new LongWritable(0))) {
                return;
            }
            String[] arr = check(value.toString()).split(",");

            context.write(new Text(arr[7] + ":" + finder(arr[0])), new Text(arr[2]));

        }
    }

    public static class AVGReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            ArrayList<String> arr = new ArrayList<>();
            for (Text value : values) {
                arr.add(value.toString());
            }

            double avg = arr.stream()
                        .mapToInt((s) -> Integer.parseInt(s))
                        .average()
                        .getAsDouble();

            context.write(key, new Text(String.valueOf(avg)));
        }
    }
}