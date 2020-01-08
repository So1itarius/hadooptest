import org.apache.hadoop.conf.Configured;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.util.ToolRunner;

import javax.tools.Tool;

public abstract class NewMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //int exitCode = ToolRunner.run(new TF_IDFJob(), args);
        //int exitCode = ToolRunner.run(new AVGPriceJob(), args);
        //int exitCode = ToolRunner.run(new CountryTypeJob(), args);
        //int exitCode = ToolRunner.run(new LoginCountJob(), args);
        //int exitCode = ToolRunner.run(new WordCountJob(), args);
        //int exitCode = ToolRunner.run(new JoinJob(), args);
        int exitCode = ToolRunner.run(new SpamUserJob(), args);
        System.exit(exitCode);
    }

}

