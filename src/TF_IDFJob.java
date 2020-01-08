import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.math.DoubleMath.log2;

public class TF_IDFJob extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path("output");
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        //Job job = Job.getInstance(getConf(), "WordCount");
        Job job1 = Job.getInstance(getConf(), "TF_IDFPart1");
        job1.setJarByClass(getClass());
        TextInputFormat.addInputPath(job1, new Path("corpus\\doc*"));
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapperClass(TF_IDFPart1.TFIDFMapper.class);
        job1.setReducerClass(TF_IDFPart1.TFIDFReducer.class);
        TextOutputFormat.setOutputPath(job1, output);
        job1.setOutputFormatClass(TextOutputFormat.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.waitForCompletion(true);

        Path output2 = new Path("output\\corpus_answer");
        FileSystem hdfss = FileSystem.get(conf);
        if (hdfss.exists(output2)) {
            hdfss.delete(output2, true);
        }

        Job job2 = Job.getInstance(getConf(), "TF_IDFPart2");
        job2.setJarByClass(getClass());
        TextInputFormat.addInputPath(job2, new Path("output\\part-r-00000"));
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapperClass(TF_IDFPart2.FinalMapper.class);
        job2.setReducerClass(TF_IDFPart2.FinalReducer.class);
        TextOutputFormat.setOutputPath(job2, output2);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        return job2.waitForCompletion(true) ? 0 : 1;
    }
}

class TF_IDFPart1 {
    static File listFile = new File("corpus");
    static File[] exportFiles = listFile.listFiles();
    static final int countFiles = Objects.requireNonNull(exportFiles).length;

    public static class TFIDFMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken() + ':' + fileName);
                //System.out.println(word+" "+one);
                context.write(new Text(fileName), word);
            }
        }
    }

    public static class TFIDFReducer
            extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            String a;
            HashMap<String, Integer> dict = new HashMap<String, Integer>();
            HashMap<String, Long> dict1 = new HashMap<>();
            ArrayList<String> lst = new ArrayList<>();
            for (Text value : values) {
                sum = 1;
                a = key.toString();
                lst.add(String.valueOf(value));
                dict.put(a, dict.getOrDefault(a, 0) + sum);

            }
            Stream<String> words = lst.stream();
            words.map(w -> w.split(" ")).flatMap(Arrays::stream)
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .forEach(dict1::put);
            for (Map.Entry<String, Long> item : dict1.entrySet()) {
                context.write(new Text(item.getKey()), new Text(String.valueOf((float) item.getValue() / (float) dict.get(key.toString()))));
            }

        }
    }
}

class TF_IDFPart2 {
    public static class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            //System.out.println(Arrays.toString(tokens));
            context.write(new Text(tokens[0]), new Text(tokens[1]));
        }
    }

    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            HashMap<String, Long> dict1 = new HashMap<String, Long>();
            HashMap<String, Long> dict2 = new HashMap<String, Long>();
            ArrayList<String[]> a = new ArrayList<>();
            for (Text value : values) {
                //System.out.println(key.toString());
                dict1.put(key.toString(), dict1.getOrDefault(key.toString(), (long) 0) + 1);
                a.add(new String[]{key.toString(), value.toString()});
            }
            for (String[] value : a) {
                //System.out.println(value[0] + ' ' + Double.parseDouble(value[1]) * log2((double) TF_IDF.countFiles / (double) dict1.get(value[0])));
                //System.out.println(key.toString()+" "+log2((double)TF_IDF.countFiles/(double)dict1.get(key.toString())));
                context.write(new Text(value[0]), new Text(String.valueOf(Double.parseDouble(value[1]) * log2((double) TF_IDFPart1.countFiles / (double) dict1.get(value[0])))));
            }
            //for(Map.Entry<String, Long> item : dict1.entrySet()){
            //    System.out.printf("Key: %s  Value: %s \n", item.getKey(), item.getValue());
            //}


            //context.write(key, ;
        }

    }
}
