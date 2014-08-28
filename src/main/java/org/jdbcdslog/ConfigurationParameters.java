package org.jdbcdslog;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationParameters {

    private static Logger logger = LoggerFactory.getLogger(ConfigurationParameters.class);
    private static Properties props;

    static long slowQueryThreshold = Long.MAX_VALUE;
    static boolean logText = false;
    static Boolean showTime = false;
    static boolean printStackTrace = false;
    static boolean printFullStackTrace = false;
    static String printStackTracePattern = null;
    static boolean inlineQueryParams = true;
    static RdbmsSpecifics rdbmsSpecifics = new OracleRdbmsSpecifics(); // oracle is default db.

    static {
        ClassLoader loader = ConfigurationParameters.class.getClassLoader();
        InputStream in = null;
        try {
            in = loader.getResourceAsStream("jdbcdslog.properties");
            props = new Properties(System.getProperties());
            if (in != null){
                props.load(in);
            }

            initSlowQueryThreshold();
            initLogText();
            initPrintStackTrace();
            initPrintFullStackTrace();
            initPrintStackTracePattern();
            initShowTime();
            initInlineQueryParams();
            initRdbmsSpecifics();

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (in != null)
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
        }
    }

    /* init parameters start. */
    private static void initSlowQueryThreshold() {
        String sSlowQueryThreshold = props.getProperty("jdbcdslog.slowQueryThreshold");
        if (sSlowQueryThreshold != null && isLong(sSlowQueryThreshold)) {
            slowQueryThreshold = Long.parseLong(sSlowQueryThreshold);
        }
        if (slowQueryThreshold == -1) {
            slowQueryThreshold = Long.MAX_VALUE;
        }
    }

    private static void initLogText() {
        String sLogText = props.getProperty("jdbcdslog.logText", "false");
        if ("true".equalsIgnoreCase(sLogText)) {
            logText = true;
        }
    }

    private static void initPrintStackTrace() {
        String sPrintStackTrace = props.getProperty("jdbcdslog.printStackTrace", "false");
        if ("true".equalsIgnoreCase(sPrintStackTrace)) {
            printStackTrace = true;
        }
    }

    private static void initPrintFullStackTrace() {
        String sPrintFullStackTrace = props.getProperty("jdbcdslog.printFullStackTrace", "false");
        printFullStackTrace = "true".equalsIgnoreCase(sPrintFullStackTrace);
    }

    private static void initPrintStackTracePattern() {
        printStackTracePattern = props.getProperty("jdbcdslog.printStackTracePattern", "");
    }


    private static void initShowTime() {
        String isShowTime = props.getProperty("jdbcdslog.showTime", "false");
        if ("true".equalsIgnoreCase(isShowTime)) {
            showTime = true;
        }
    }

    private static void initInlineQueryParams() {
        String isInlineQueryParams = props.getProperty("jdbcdslog.inlineQueryParams", "true");
        inlineQueryParams = ("true".equalsIgnoreCase(isInlineQueryParams)) ;
    }

    private static void initRdbmsSpecifics() {
        String driverName = props.getProperty("jdbcdslog.driverName");
        if ("oracle".equalsIgnoreCase(driverName)) {
            // no op. since = default db. and skip next if statement,maybe better.
        } else if ("mysql".equalsIgnoreCase(driverName)) {
            rdbmsSpecifics = new MySqlRdbmsSpecifics();
        } else if ("sqlserver".equalsIgnoreCase(driverName)) {
            rdbmsSpecifics = new SqlServerRdbmsSpecifics();
        }
    }
    /* init parameters end. */

    public static void setLogText(boolean alogText) {
        logText = alogText;
    }

    private static boolean isLong(String sSlowQueryThreshold) {
        try {
            Long.parseLong(sSlowQueryThreshold);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

}
