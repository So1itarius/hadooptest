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
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CountryTypeJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance(getConf(), "CountryType");
        job.setJarByClass(getClass());
        TextInputFormat.addInputPath(job, new Path("input\\SalesJan2009.csv"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(CountryType.CountryMapper.class);
        job.setReducerClass(CountryType.CountryReducer.class);
        TextOutputFormat.setOutputPath(job, output);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

class CountryType {
    protected static ArrayList<String> Counter(ArrayList<String> arr) {
        Map<String, Integer> letters = new HashMap<>();

        for (String tempChar : arr) {
            if (!letters.containsKey(tempChar)) {
                letters.put(tempChar, 1);
            } else {
                letters.put(tempChar, letters.get(tempChar) + 1);
            }
        }
        int max = 0;
        String maxStr = "";
        for (Map.Entry<String, Integer> entry : letters.entrySet()) {
            if (entry.getValue() > max) {
                maxStr = entry.getKey();
                max = entry.getValue();
            }
        }
        ArrayList<String> res = new ArrayList<>();
        res.add(maxStr);
        res.add(String.valueOf(max));
        return res;

    }
    protected static String check(String str) {
        Pattern pattern = Pattern.compile("\"\\d+,\\d+\"");
        Matcher matcher = pattern.matcher(str);
        String s = "";
        while (matcher.find())
            s = matcher.group().replace("\"", "").replace(",", "");
        return str.replaceAll("\"\\d+,\\d+\"", s);
    }

    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.equals(new LongWritable(0))) {
                return;
            }
            String[] tokens = check(value.toString()).split(",");
            context.write(new Text(tokens[7]), new Text(tokens[3]));
        }
    }

    public static class CountryReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> arr = new ArrayList<>();
            for (Text value : values) arr.add(value.toString());

            context.write(key, new Text(String.valueOf(Counter(arr))));
        }
    }
}