package com.waterlinedata.hive;

import com.sun.security.auth.callback.TextCallbackHandler;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.net.URL;
import java.security.PrivilegedExceptionAction;
import java.sql.*;
import java.text.MessageFormat;

/**
 * Created by Tushar on 10/7/15.
 */
public class HiveKerberizedSubjectProxyUserClient {

    private static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
    private static final String CONNECTION_URL = "jdbc:hive2://{0}:{1}/default;principal={2};auth=kerberos;kerberosAuthType=fromSubject;hive.server2.proxy.user={3}";

    private static final String HOST = "titan";
    private static final String PORT = "10000";
    private static final String PRINCIPAL = "hive/titan.waterline.lab@WATERLINEDATA.COM";



    private static final String END_USER = "tushar";
    private static final String TABLE_NAME = "T1";


    private static final String SHOW_TABLE = "SHOW TABLE EXTENDED LIKE {0}";
    private static final String DESCRIBE_TABLE = "DESCRIBE FORMATTED {0}";
    private static final String CREATE_TABLE = "CREATE TABLE {0} (C1 INT)";
    private static final String DROP_TABLE = "DROP TABLE {0}";
    private static final String SHOW_DATABASES = "SHOW DATABASES";
    private static final String SHOW_TABLES = "SHOW TABLES";


    public static void main(String[] args) throws Exception {
        String configKey = "java.security.auth.login.config";
        if(System.getProperty(configKey) == null) {
            URL resource = HiveKerberizedSubjectProxyUserClient.class.getClassLoader().getResource("login.conf");
            String path = resource.getPath();
            System.out.println(path);
            System.setProperty(configKey, path);
        }

        Subject subject = getKerberizedSubject();


        Connection connection = getConnection(subject, END_USER);

        Statement statement = connection.createStatement();

        createTable(statement);
        separator();


        describeTable(statement);
        separator();


        showTableInfo(statement);
        separator();


        dropTable(statement);
        separator();


        showDatabases(statement);
        separator();


        showTables(statement);
        separator();

    }

    private static void separator() {
        System.out.println("---------------------------------------------------------------------------");
    }

    private static void showTables(Statement statement) throws SQLException {
        System.out.println("Show Tables ...");
        ResultSet resultSet;
        resultSet = statement.executeQuery(SHOW_TABLES);
        while (resultSet.next()) {
            String name = resultSet.getString(1);
            System.out.println(name);
        }
    }

    private static void showDatabases(Statement statement) throws SQLException {
        System.out.println("Show Databases ...");
        ResultSet resultSet;
        resultSet = statement.executeQuery(SHOW_DATABASES);
        while (resultSet.next()) {
            String name = resultSet.getString(1);
            System.out.println(name);
        }
    }

    private static void dropTable(Statement statement) throws SQLException {
        System.out.println("Drop Table ...");
        String sql;
        sql = MessageFormat.format(DROP_TABLE, TABLE_NAME);
        statement.execute(sql);
        System.out.println("Table Dropped");
    }

    private static void showTableInfo(Statement statement) throws SQLException {
        System.out.println("Show Table Info ...");
        String sql;
        ResultSet resultSet;
        sql = MessageFormat.format(SHOW_TABLE, TABLE_NAME);
        resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1));
        }
    }

    private static void describeTable(Statement statement) throws SQLException {
        System.out.println("Describe Table ...");
        String sql;
        ResultSet resultSet;
        sql = MessageFormat.format(DESCRIBE_TABLE, TABLE_NAME);
        resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + "\t\t" + resultSet.getString(2));
        }
    }

    private static void createTable(Statement statement) throws SQLException {
        System.out.println("Create Table ...");
        String sql;
        sql = MessageFormat.format(CREATE_TABLE, TABLE_NAME);
        statement.execute(sql);
        System.out.println("Table Created");
    }

    /**
     * @return - Returns the kerberized subject for the current kinited user.
     */
    private static Subject getKerberizedSubject() {
        LoginContext lc = getLoginContext();
        return lc.getSubject();
    }

    private static LoginContext getLoginContext() {
        LoginContext lc = null;

        try {
            lc = new LoginContext("SampleClient", new TextCallbackHandler());
            lc.login();
        } catch (LoginException le) {
            le.printStackTrace();
        }
        return lc;
    }

    /**
     * @param signedOnUserSubject - The kerberized Subject for the proxy user
     * @return Returns the connection for the proxied user
     * @throws Exception
     */
    private static Connection getConnection(Subject signedOnUserSubject, final String endUser) throws Exception {
        return Subject.doAs(signedOnUserSubject, new PrivilegedExceptionAction<Connection>() {
            public Connection run() throws Exception {
                String url = MessageFormat.format(CONNECTION_URL, HOST, PORT, PRINCIPAL, endUser);
                Class.forName(JDBC_DRIVER);
                return DriverManager.getConnection(url);
            }
        });
    }
}
