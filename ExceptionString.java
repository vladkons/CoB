package dbconnect;


import java.io.StringWriter;
import java.io.PrintWriter;

public class ExceptionString {


    public final static String toString(Exception e) {

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);

        return sw.toString();
    }
}
