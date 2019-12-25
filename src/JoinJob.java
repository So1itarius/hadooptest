import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoinJob {
    public static class UserMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 5)
                return;
            context.write(new Text(parts[4]), new Text("USER"+parts[3]));
        }
    }
    public static class DepartmentMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] department = value.toString().split(",");
            context.write(new Text(department[0]), new Text(department[1]));
            System.out.println(department[0] +" "+ department[1]);
        }
    }

    public static class NameReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            String tempOne = "";
            StringBuilder tempTwo = new StringBuilder();
            for (Text value : values) {
                if (value.toString().contains("USER")) {
                    tempTwo.append(value.toString().substring(4)).append("jopa");
                } else {
                    tempOne = value.toString();
                }

            }
            String[] splList = tempTwo.toString().split("jopa");
            if((tempTwo.length()>0)&&(tempOne.length()>0)) {
                for (String item : splList) {
                    context.write(new Text(tempOne + " : "), new Text(item));
                }
            }
        }
    }



}
