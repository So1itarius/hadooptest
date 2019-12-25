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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.*;

public class WordCountJob {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path output = new Path("doc2.txt");
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        //HashMap<String, String> allUsers = new HashMap<String, String>();
        Job job = Job.getInstance();
        job.setJarByClass(WordCountJob.class);
        TextInputFormat.addInputPath(job, new Path("C:\\Users\\User\\IdeaProjects\\hadooptest\\SalesJan2009.csv"));
        job.setInputFormatClass(TextInputFormat.class);
        //job.setMapperClass(CountryMapper.class);
        //job.setReducerClass(CountryReducer.class);
        job.setMapperClass(AVGMapper.class);
        job.setReducerClass(AVGReducer.class);
        //MultipleInputs.addInputPath(job, new Path("users.csv"), TextInputFormat.class, UserMapper.class);
        //MultipleInputs.addInputPath(job, new Path("departments.csv"), TextInputFormat.class, DepartmentMapper.class);

        //job.setMapperClass(UniqueUserMapper.class);
        //job.setReducerClass(UniqueUserReducer.class);
        //job.setMapperClass(UserMapper.class);
        //job.setMapperClass(DepartmentMapper.class);
        //job.setReducerClass(MultipleOutputsReducer.class);
        //TextOutputFormat.setOutputPath(job, new Path("doc2.txt"));
        //job.setInputFormatClass(TextInputFormat.class);



        //FileInputFormat.setInputPaths(job, new Path("doc2.txt"));
        TextOutputFormat.setOutputPath(job, new Path("doc2.txt"));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(DataWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class LoginMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] kosmos = value.toString().split(",");
            //context.write(new Text(kosmos[4]),new Text(kosmos[2]));
            context.write(new Text(kosmos[2]), new Text(kosmos[4]));

        }
    }

    public static class LoginReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            HashSet<String> tempString = new HashSet<>();
            for (Text value : values) {
                tempString.add(value.toString());
            }
            context.write(key, new Text(tempString.toString()));

        }
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
    public static class CountryMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (key.equals(new LongWritable(0))){
                return;
            }
            String[] tokens = value.toString().split(",");
            context.write(new Text(tokens[7]), new Text(tokens[3]));
        }
    }

    public static class CountryReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> tempString = new ArrayList<String>();
            int flag = 0;
            HashMap<String, ArrayList<String>> dict = new HashMap<String, ArrayList<String>>();
            ArrayList<String> a;
            for (Text value : values) {
                //System.out.println("key: " + key);
                //System.out.println(value.toString());
                if (flag==0){dict.put(key.toString(),new ArrayList<>());flag=1;}
                //dict.put(key.toString(),new ArrayList<>());
                a = dict.get(key.toString());
                a.add(value.toString());
                dict.put(key.toString(),a);
                tempString.add(value.toString());
                tempString.add(String.valueOf(flag));
                flag++;
            }
            for (Map.Entry entry : dict.entrySet()) {
                System.out.println("Key: " + entry.getKey() + " Value: "
                        + TestMain.Counter((ArrayList<String>) entry.getValue()));
                dict.put((String) entry.getKey(),TestMain.Counter((ArrayList<String>) entry.getValue()));
            }
            //for (ArrayList<String> value : dict.values()) {
            //    System.out.println("Value: " + TestMain.Counter(value));
            //}
            //for (Map.Entry entry : dict.entrySet()) {
            //    dict.put((String) entry.getKey(), TestMain.Counter((ArrayList<String>) entry.getValue()));
            //}
            context.write(new Text("Null"),new Text(dict.toString()));
        }
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
            //if
            String[] kosmos = TestMain.rep1(value.toString()).split(",");
            //context.write(new Text(kosmos[4]),new Text(kosmos[2]));
            context.write(new Text(kosmos[7]+" "+ TestMain.finder(kosmos[0])), new Text(kosmos[2]));

        }
    }

    public static class AVGReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            ArrayList<String> tempString = new ArrayList<String>();
            //for (Text value : values) {
            //    tempString.add(value.toString());
            //}
            int flag = 0;
            HashMap<String, ArrayList<String>> dict = new HashMap<String, ArrayList<String>>();
            ArrayList<String> a;
            for (Text value : values) {
                //System.out.println("key: " + key);
                //System.out.println(value.toString());
                if (flag==0){dict.put(key.toString(),new ArrayList<>());flag=1;}
                //dict.put(key.toString(),new ArrayList<>());
                a = dict.get(key.toString());
                a.add(value.toString());
                dict.put(key.toString(),a);
                tempString.add(value.toString());
                //tempString.add(String.valueOf(flag));
                flag++;
            }
            HashMap<String, Double> dict1 = new HashMap<>();
            double a1;
            for (Map.Entry entry : dict.entrySet()) {
                //System.out.println("Key: " + entry.getKey() + " Value: "
                //        + ((ArrayList<String>) entry.getValue()).stream().mapToInt((s) -> Integer.parseInt(s)).average());
                a1 = ((ArrayList<String>) entry.getValue()).stream().mapToInt((s) -> Integer.parseInt(s)).average().getAsDouble();
                dict1.put((String) entry.getKey(), a1);
            }
            /*for (Map.Entry entry : dict1.entrySet()) {
                System.out.println("Key: " + entry.getKey() + " Value: "
                        + ((ArrayList<String>) entry.getValue()).stream().mapToInt((s) -> Integer.parseInt(s)).average());
                //a1 = ((ArrayList<String>) entry.getValue()).stream().mapToInt((s) -> Integer.parseInt(s)).average();
                //dict1.put((String) entry.getKey(), a1);
            }*/
            context.write(new Text("Null"), new Text(dict1.toString()));
        }
    }

    public static class UserMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] kosmos = value.toString().split(",");
            //context.write(new Text(kosmos[4]),new Text(kosmos[2]));
            context.write(new Text(kosmos[4]), new Text(kosmos[3]));

        }
    }

    public static class DepartmentMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] kosmos = value.toString().split(",");
            //context.write(new Text(kosmos[4]),new Text(kosmos[2]));
            context.write(new Text(kosmos[0]), new Text(kosmos[1]));

        }
    }

    public static class CountReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            HashSet<String> tempString = new HashSet<>();
            for (Text value : values) {
                tempString.add(value.toString());
            }
            context.write(key, new Text(tempString.toString()));

        }
    }

    static class MultipleOutputsReducer extends Reducer<Text, Text, NullWritable, Text> {
        private MultipleOutputs<NullWritable, Text> multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                multipleOutputs.write(NullWritable.get(), value, key.toString());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }
}




