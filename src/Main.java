import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.ToolRunner;

import javax.tools.Tool;

public abstract class Main extends Configured implements Tool {
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

