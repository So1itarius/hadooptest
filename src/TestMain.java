import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TestMain {
    protected static ArrayList<String> Counter(ArrayList<String> fakeArray) {
        Map<String, Integer> letters = new HashMap<>();

        for (int i = 0; i < fakeArray.size(); i++) {
            String tempChar = fakeArray.get(i);

            if (!letters.containsKey(tempChar)) {
                letters.put(tempChar, 1);
            } else {
                letters.put(tempChar, letters.get(tempChar) + 1);
            }
        }
        int max = 0;
        String maxStr="";
        for (Map.Entry<String, Integer> entry : letters.entrySet()) {
            if (entry.getValue()>max){maxStr=entry.getKey();max=entry.getValue();}
            //System.out.println("key " + entry.getKey() + ",value " + entry.getValue());
        }
        //System.out.println(max);
        //System.out.println(maxStr);
        ArrayList<String> fakeArray1 = new ArrayList<>();
        fakeArray1.add(maxStr);
        fakeArray1.add(String.valueOf(max));
        return fakeArray1;

    }
    protected static String finder(String str) {
        Pattern pattern = Pattern.compile("\\d+/\\d+");
        Matcher matcher = pattern.matcher(str);
        while(matcher.find()){
           str = matcher.group();
        }
        //return matcher.group();
        return str;
    }
    protected static String rep1(String str) {
        Pattern pattern1 = Pattern.compile("\"\\d+,\\d+\"");
        Matcher matcher1 = pattern1.matcher(str);
        String s = "";
        while(matcher1.find())
            s = matcher1.group().replace("\"","").replace(",","");
        String myStr =str.replaceAll("\"\\d+,\\d+\"", s);
        //System.out.println(myStr);
        return myStr;
    }

    public static void main(String[] args) throws ParseException {
        String s = "2019-11-18 09:47:28.525000";
        DateFormat dateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
        Date date = dateFormat.parse(s);
        long unixTime = date.getTime() / 1000;
        //System.out.println(unixTime);
        ArrayList<String> a = new ArrayList<String>();
        a.add("Visa");a.add("Visa");a.add("Mastercard");a.add("Mastercard");a.add("Mastercard");
        //(Visa, Visa, Mastercard, Mastercard, Mastercard)
        //System.out.println(Counter(a));
        String s1 = "1/8/09 13:14";
        //String input = "Hello Java! Hello JavaScript! JavaSE 8.";
        Pattern pattern = Pattern.compile("\\d+/\\d+");
        Matcher matcher = pattern.matcher(s1);
        //System.out.println(matcher.group());
        //while(matcher.find())
        //    System.out.println(matcher.group());
        //System.out.println(finder(s1));
        String s2 = "1/28/09 18:00,Product1,13000,Visa,sandhya,Centennial                  ,CO,United States,12/2/06 23:24,2/7/09 15:18,39.57917,-104.87639";
        Pattern pattern1 = Pattern.compile("\"\\d+,\\d+\"");
        Matcher matcher1 = pattern1.matcher(s2);
        //System.out.println(matcher.group());
        //while(matcher1.find())
        //   System.out.println(matcher1.group().replace("\"","").replace(",",""));
        System.out.println(rep1(s2));
    }
}
