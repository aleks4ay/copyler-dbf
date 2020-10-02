package kiyv.domain.dao;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import static kiyv.log.ClassNameUtil.getCurrentClassName;

public class UtilDao {
//    private Connection connDbf = null;
//    private Connection connPostgres = null;
//    private Connection connTest = null;
    private static final Logger log = LoggerFactory.getLogger(getCurrentClassName());

//    private static String driverDbf = null;
    private static String driverPostgres = null;
//    private static String urlDbf = null;
    private static String urlPostgres = null;
    private static String urlPostgresFrom = null;
    private static String urlTest = null;
    private static String user = null;
    private static String userTest = null;
    private static String password = null;
    private static String passwordTest = null;


    static {
        //load DB properties
        try (InputStream in = UtilDao.class.getClassLoader().getResourceAsStream("persistence.properties")){
            Properties properties = new Properties();
            properties.load(in);
//            driverDbf = properties.getProperty("dbf.driverClassName");
//            urlDbf = "jdbc:dbf:/" + properties.getProperty("dbf.path");

            driverPostgres = properties.getProperty("database.driverClassName");
            urlPostgres = properties.getProperty("database.url");
            urlPostgresFrom = properties.getProperty("urlWhereFromCopy");
            user = properties.getProperty("database.username");
            password = properties.getProperty("database.password");
            log.debug("Loaded properties as Stream: dbf.driverClassName = {}, dbf.url = {}, database.driverClassName = {}, " +
                            "database.url = {}, database.username = {})",
                    /*driverDbf, urlDbf,*/ driverPostgres, urlPostgres, user);

            urlTest = properties.getProperty("db.urlTest");
            userTest = properties.getProperty("db.usernameTest");
            passwordTest = properties.getProperty("db.passwordTest");
            log.debug("Loaded properties as Stream for Test: db.urlTest = {}, db.usernameTest = {})", urlTest, userTest);
        } catch (IOException e) {
            log.warn("Exception during Loaded properties from file {}.", new File("/persistence.properties").getPath(), e);
        }
    }

/*    public Connection getConnDbf() {
        try {
            Class.forName(driverDbf);
            Connection connDbf = DriverManager.getConnection(urlDbf);
            log.debug("Created connection for 'dbf-files' from 1C. Url= {}.", urlDbf);
            return connDbf;
        } catch (SQLException | ClassNotFoundException e) {
            log.warn("Exception during create connection for 'dbf-files' from 1C. Url= {}.", urlDbf, e);
        }
        return null;
    }*/

    public Connection getConnPostgres() {
        try {
            Class.forName(driverPostgres);
            Connection connPostgres = DriverManager.getConnection(urlPostgres, user, password);
            connPostgres.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connPostgres.setAutoCommit(false);
            log.debug("Created connection for 'postgres'. Url= {}, user= {}.", urlPostgres, user);
            return connPostgres;
        } catch (SQLException | ClassNotFoundException e) {
            log.warn("Exception during create connection for 'postgres' url= {}, user= {}.", urlPostgres, user, e);
        }
        return null;
    }

    public Connection getConnPostgresFrom() {
        try {
            Class.forName(driverPostgres);
            Connection connPostgres = DriverManager.getConnection(urlPostgresFrom, user, password);
            connPostgres.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connPostgres.setAutoCommit(false);
            log.debug("Created connection for 'postgres'. Url= {}, user= {}.", urlPostgresFrom, user);
            return connPostgres;
        } catch (SQLException | ClassNotFoundException e) {
            log.warn("Exception during create connection for 'postgres' url= {}, user= {}.", urlPostgresFrom, user, e);
        }
        return null;
    }

    public Connection getConnTest() {
        try {
            Class.forName(driverPostgres);
            Connection connTest = DriverManager.getConnection(urlTest, userTest, passwordTest);
            connTest.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            connTest.setAutoCommit(false);
            log.debug("Created connection for 'postgres'. Url= {}, user= {}.", urlTest, userTest);
            return connTest;
        } catch (SQLException | ClassNotFoundException e) {
            log.warn("Exception during create connection for 'postgres' url= {}, user= {}.", urlTest, userTest, e);
        }
        return null;
    }

    public void closeConnection(Connection conn) {
        try {
            log.debug("Closing connection {}.", conn);
            conn.close();
        } catch (SQLException e) {
            log.warn("Exception during Closing connection {}.", conn, e);
        }
    }
}
