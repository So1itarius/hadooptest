import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class CountryTypeJob {
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

    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.equals(new LongWritable(0))) {
                return;
            }
            String[] tokens = value.toString().split(",");
            context.write(new Text(tokens[7]), new Text(tokens[3]));
        }
    }

    public static class CountryReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int flag = 0;
            HashMap<String, ArrayList<String>> dict = new HashMap<String, ArrayList<String>>();
            ArrayList<String> a;
            for (Text value : values) {
                if (flag == 0) {
                    dict.put(key.toString(), new ArrayList<>());
                    flag = 1;
                }
                a = dict.get(key.toString());
                a.add(value.toString());
                dict.put(key.toString(), a);
            }
            for (Map.Entry entry : dict.entrySet()) {
                dict.put((String) entry.getKey(), Counter((ArrayList<String>) entry.getValue()));
            }

            context.write(new Text("Null"), new Text(dict.toString()));
        }
    }
}
