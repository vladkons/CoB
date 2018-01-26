package dbconnect;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.Properties;


public class DBConnectTest {
    public DBConnectTest() {
        super();
    }
    private static DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");


    public static void main(String[] argv) throws Exception {

        long start = 0;
        long end = 0;

        int runTime = 60 * 24; // default runtime (24 hrs) - in minutes
        int sleepTime = 10000; //default sleep time - in milliseconds

        //Keep loaded properties
        HashMap props = new HashMap();

        // load properties
        File file = new File("db.properties");
        FileInputStream fileInput = new FileInputStream(file);
        Properties properties = new Properties();
        properties.load(fileInput);
        fileInput.close();


        // Get sleep time from properties
        String s = properties.getProperty("sleepTime");
        if (s != null && s.length() > 0) {
            sleepTime = Integer.parseInt(s);
        }

        // Get run time from properties, convert to milliseconds
        String r = properties.getProperty("runTime");
        if (r != null && r.length() > 0) {
            runTime = Integer.parseInt(r) * 60 * 1000; // minutes * seconds * milisecond
        }

        //get up to 5 profiles, if any key/value mising - ignore the profile
        for (int i = 1; i <= 5; i++) {
            String host = properties.getProperty("host" + i);
            String port = properties.getProperty("port" + i);
            String service = properties.getProperty("service" + i);
            String user = properties.getProperty("user" + i);
            String password = properties.getProperty("password" + i);
            String lease = properties.getProperty("lease" + i);


            if (host != null && port != null && service != null && user != null && password != null &&
                host.length() > 0 && port.length() > 0 && service.length() > 0 && user.length() > 0 &&
                password.length() > 0) {

                props.put("host" + i, host);
                props.put("port" + i, port);
                props.put("service" + i, service);
                props.put("user" + i, user);
                props.put("password" + i, password);
                props.put("lease" + i, lease);
            }

        }

        // Load driver, if error - exit
        try {

            Class.forName("oracle.jdbc.driver.OracleDriver");

        } catch (Exception e) {

            String exceptionString = ExceptionString.toString(e);
            writeLogError(-1, System.currentTimeMillis(), exceptionString);

            return;

        }

        Connection connection = null;

        int c = 0;
        long startTime = System.currentTimeMillis();

        while ((System.currentTimeMillis() - startTime) < runTime) {
            for (int i = 1; i <= 5; i++) {

                try {
                    String host = (String) props.get("host" + i);
                    String port = (String) props.get("port" + i);
                    String service = (String) props.get("service" + i);
                    String user = (String) props.get("user" + i);
                    String password = (String) props.get("password" + i);
                    String lease = (String) props.get("lease" + i);

                    //try connection
                    if (host != null && port != null && service != null && user != null && password != null &&
                        host.length() > 0 && port.length() > 0 && service.length() > 0 && user.length() > 0 &&
                        password.length() > 0) {


                        start = System.currentTimeMillis();
                        connection =
                            DriverManager.getConnection("jdbc:oracle:thin:@" + host + ":" + port + ":" + service, user,
                                                        password);
                        end = System.currentTimeMillis();

                        writeLog(i, " connect took ", System.currentTimeMillis(), end - start);

                        // Run a SQL statement - some profiles would query the lease table
                        if (lease != null && lease.equalsIgnoreCase("YES")) {
                            getLease(i, connection);
                        } else {
                            getDUAL(i, connection);
                        }

                        connection.close();
                    }
                } catch (SQLException e) {

                    String exceptionString = ExceptionString.toString(e);
                    writeLogError(i, System.currentTimeMillis(), exceptionString);


                    //If invalid password  - exit
                    if (exceptionString.toLowerCase().contains("invalid") ||
                        exceptionString.toLowerCase().contains("password")) {
                        writeLogError(i, System.currentTimeMillis(),
                                      "Program will exit in order to avoid account lockout.");
                        System.exit(0);
                    }
                }
            }

            Thread.sleep(sleepTime);
        }
    }


    public static void writeLog(int i, String message, long currTime, long elapsedTime) {

        BufferedWriter bw = null;

        try {

            bw = new BufferedWriter(new FileWriter("dbtest.log", true));
            
            //Write just message, or elapsed time and profile
            if (i == 0 || currTime == 0 || elapsedTime == 0) {
                bw.write(message);
            } else {
                bw.write(dateFormat.format(new Date(currTime)) + " Profile #" + i + " - " + message + elapsedTime + " milliseconds.");
            }
            bw.newLine();
            bw.flush();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally { // always close the file
            if (bw != null)
                try {
                    bw.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        } // end try/catch/finally
    }


    public static void writeLogError(int i, long currTime, String error) {

        BufferedWriter bw = null;

        try {

            bw = new BufferedWriter(new FileWriter("dbtest.log", true));
            bw.newLine();
            bw.newLine();
            bw.newLine();

            bw.write(dateFormat.format(new Date(currTime)) + " Profile #" + i + " ERROR <<<< ---  ");
            bw.write(error);
            bw.write(dateFormat.format(new Date(currTime)) + " Profile #" + i + " --- >>>> ERROR ");
            bw.newLine();
            bw.newLine();
            bw.newLine();
            bw.flush();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        } finally { // always close the file
            if (bw != null)
                try {
                    bw.close();
                } catch (IOException ioe) {
                    ioe.printStackTrace();
                }
        } // end try/catch/finally
    }

    private static void getDUAL(int i, Connection conn) throws Exception {
        PreparedStatement st = null;

        //Get Attendee count
        String sql = " SELECT 1 FROM DUAL";
        long start = System.currentTimeMillis();
        st = conn.prepareStatement(sql);
        ResultSet rs = st.executeQuery();
        long end = System.currentTimeMillis();

        writeLog(i, " execute 'Select 1 From DUAL' took ", System.currentTimeMillis(), end - start);
    }


    private static void getLease(int i, Connection conn) throws Exception {

        StringBuffer results = new StringBuffer(" Table 'Active' Content" + "\r\n" + "\r\n");
        String sql = "SELECT * FROM ACTIVE";
        PreparedStatement st = null;
        long start = System.currentTimeMillis();
        st = conn.prepareStatement(sql);
        ResultSet rs = st.executeQuery();
        long end = System.currentTimeMillis();
        while (rs.next()) {

            String server = rs.getString("server");
            String instance = rs.getString("instance");
            String domainname = rs.getString("domainname");
            String clustername = rs.getString("clustername");
            Date timeout = rs.getDate("timeout");

            results.append(padRight(server, 35) + padRight(instance, 40) + padRight(domainname, 20) +
                           padRight(clustername, 20) + padRight(dateFormat.format(timeout), 25) + "\r\n");

        }
        writeLog(i, " execute 'Select * From ACTIVE' took ", System.currentTimeMillis(), end - start);
        writeLog(0, results.toString(), 0, 0);
    }

    public static String padRight(String s, int n) {
        return String.format("%1$-" + n + "s", s);
    }

    public static String padLeft(String s, int n) {
        return String.format("%1$" + n + "s", s);
    }
}
