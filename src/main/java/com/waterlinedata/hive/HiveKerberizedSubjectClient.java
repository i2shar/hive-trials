package com.waterlinedata.hive;

import com.sun.security.auth.callback.TextCallbackHandler;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.PrivilegedExceptionAction;
import java.sql.*;

/**
 * Created by Tushar on 10/6/15.
 */
public class HiveKerberizedSubjectClient {

    private static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        LoginContext lc = getLoginContext();
        Subject subject = lc.getSubject();
        Connection connection = getConnection(subject);
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("show databases");
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        System.out.println("columnCount = " + columnCount);
        while(resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                Object object = resultSet.getString(i);
                System.out.println(columnName + " = " + object);
            }
        }
    }

    private static LoginContext getLoginContext() {
        LoginContext lc = null;

        try {
            lc = new LoginContext("SampleClient", new TextCallbackHandler());
            // attempt authentication
            lc.login();
        } catch (LoginException le) {
            le.printStackTrace();
        }
        return lc;
    }

    private static Connection getConnection( Subject signedOnUserSubject ) throws Exception{

        return Subject.doAs(signedOnUserSubject, new PrivilegedExceptionAction<Connection>() {
            public Connection run() throws Exception {
                Connection con = null;
                //String JDBC_DB_URL = "jdbc:hive2://sandbox:10000/default";
                //String JDBC_DB_URL = "jdbc:hive2://titan:10000/default;principal=hive/titan.waterline.lab@WATERLINEDATA.COM;auth=kerberos";
                String JDBC_DB_URL = "jdbc:hive2://titan:10000/default;principal=hive/titan.waterline.lab@WATERLINEDATA.COM;auth=kerberos;kerberosAuthType=fromSubject";
                Class.forName(JDBC_DRIVER);
                con = DriverManager.getConnection(JDBC_DB_URL);
                return con;
            }
        });
    }
}
