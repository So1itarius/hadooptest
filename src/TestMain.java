import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestMain {
    public static void main(String[] args) throws ParseException {
       String s = "2019-11-18 09:47:28.525000";


        DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        Date date = dateFormat.parse(s);
        long unixTime = date.getTime()/1000;
        System.out.println(unixTime);




    }
}
