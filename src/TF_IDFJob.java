import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString());
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken() + '#' + fileName);
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
            HashMap<String, Long> dict = new HashMap<>();
            ArrayList<String> lst = new ArrayList<>();
            for (Text value : values) {
                sum++;
                lst.add(String.valueOf(value));
            }
            System.out.println(sum);
            Stream<String> words = lst.stream();
            words.map(w -> w.split(" ")).flatMap(Arrays::stream)
                    .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                    .forEach(dict::put);
            for (Map.Entry<String, Long> item : dict.entrySet()) {
                context.write(new Text(item.getKey()), new Text(String.valueOf((float) item.getValue() / (float) sum)));
            }

        }
    }
}

class TF_IDFPart2 {
    public static class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("#");
            context.write(new Text(tokens[0]), new Text(tokens[1]));
        }
    }

    public static class FinalReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String[]> a = new ArrayList<>();
            int sum=0;
            for (Text value : values) {
                sum++;
                a.add(new String[]{key.toString(), value.toString()});
            }

            for (String[] value : a) {
                context.write(new Text(value[0]+":"+value[1].split("\t")[0]),
                        new Text(String.valueOf(Double.parseDouble(value[1].split("\t")[1]) *
                        log2((double) TF_IDFPart1.countFiles / (double) sum))));
            }

        }

    }
}
