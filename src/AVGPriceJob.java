import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AVGPriceJob {
    protected static String finder(String str) {
        Pattern pattern = Pattern.compile("\\d+/\\d+");
        Matcher matcher = pattern.matcher(str);
        while(matcher.find()){
            str = matcher.group();
        }
        return str;
    }
    protected static String check(String str) {
        Pattern pattern = Pattern.compile("\"\\d+,\\d+\"");
        Matcher matcher = pattern.matcher(str);
        String s = "";
        while(matcher.find())
            s = matcher.group().replace("\"","").replace(",","");
        return str.replaceAll("\"\\d+,\\d+\"", s);
    }
    public static class AVGMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.equals(new LongWritable(0))){
                return;
            }
            String[] arr = check(value.toString()).split(",");

            context.write(new Text(arr[7]+" "+ finder(arr[0])), new Text(arr[2]));

        }
    }

    public static class AVGReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {

            int flag = 0;
            HashMap<String, ArrayList<String>> dict = new HashMap<String, ArrayList<String>>();
            ArrayList<String> a;
            for (Text value : values) {
                if (flag==0){dict.put(key.toString(),new ArrayList<>());flag=1;}
                a = dict.get(key.toString());
                a.add(value.toString());
                dict.put(key.toString(),a);
            }
            HashMap<String, Double> dict1 = new HashMap<>();
            double a1;
            for (Map.Entry entry : dict.entrySet()) {
                a1 = ((ArrayList<String>) entry.getValue()).stream()
                                                           .mapToInt((s) -> Integer.parseInt(s))
                                                           .average()
                                                           .getAsDouble();
                dict1.put((String) entry.getKey(), a1);
            }

            context.write(new Text("Null"), new Text(dict1.toString()));
        }
    }

}
