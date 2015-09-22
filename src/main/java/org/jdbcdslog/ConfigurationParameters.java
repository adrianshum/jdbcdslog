package org.jdbcdslog;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurationParameters {

    private static Logger logger = LoggerFactory.getLogger(ConfigurationParameters.class);
    static Properties props;

    static long slowQueryThresholdInNano = Long.MAX_VALUE;
    static Boolean showTime = false;
    static boolean printStackTrace = false;
    static boolean printFullStackTrace = false;
    static String printStackTracePattern = null;
    static boolean inlineQueryParams = true;
    static RdbmsSpecifics rdbmsSpecifics = new OracleRdbmsSpecifics(); // oracle is default db.
    static boolean logBeforeStatement = false;
    static boolean logDetailAfterStatement = true;
    static boolean logAddBatchDetail = true;
    static boolean logAddBatch = true;
    static boolean logExecuteBatchDetail = true;
    static boolean logExceptions = true;

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
            initPrintStackTrace();
            initPrintFullStackTrace();
            initPrintStackTracePattern();
            initShowTime();
            initInlineQueryParams();
            initRdbmsSpecifics();
            initLogBeforeStatement();
            initLogDetailAfterStatement();
            initLogAddBatch();
            initLogAddBatchDetail();
            initLogExecuteBatchDetail();
            initLogExceptions();

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
    static void initSlowQueryThreshold() {
        String slowQueryThresholdString = props.getProperty("jdbcdslog.slowQueryThreshold");
        if (slowQueryThresholdString != null)  {
            slowQueryThresholdString = slowQueryThresholdString.trim();
            try {
                if (slowQueryThresholdString.endsWith("ns")) {
                    slowQueryThresholdInNano = Long.parseLong(slowQueryThresholdString.substring(0, slowQueryThresholdString.length() - 2));
                } else if (slowQueryThresholdString.endsWith("ms")) {
                    slowQueryThresholdInNano = Long.parseLong(slowQueryThresholdString.substring(0, slowQueryThresholdString.length() - 2)) * 1000000;
                } else if (slowQueryThresholdString.endsWith("s")) {
                    slowQueryThresholdInNano = Long.parseLong(slowQueryThresholdString.substring(0, slowQueryThresholdString.length() - 1)) * 1000000 * 1000;
                } else {
                    slowQueryThresholdInNano = Long.parseLong(slowQueryThresholdString) * 1000000;  // assume ms by default
                }
            } catch (NumberFormatException ignored) {
                slowQueryThresholdInNano = 0;
            }
        }

        if (slowQueryThresholdInNano <= 0 ) {
            slowQueryThresholdInNano = Long.MAX_VALUE;
        }
    }

    private static void initPrintStackTrace() {
        printStackTrace = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.printStackTrace", "false"));
    }

    private static void initPrintFullStackTrace() {
        printFullStackTrace = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.printFullStackTrace", "false"));
    }

    private static void initPrintStackTracePattern() {
        printStackTracePattern = props.getProperty("jdbcdslog.printStackTracePattern", "");
    }


    private static void initShowTime() {
        showTime = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.showTime", "false"));
    }

    private static void initInlineQueryParams() {
        inlineQueryParams = ("true".equalsIgnoreCase(props.getProperty("jdbcdslog.inlineQueryParams", "true"))) ;
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


    private static void initLogBeforeStatement() {
        logBeforeStatement = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.logBeforeStatement", "false"));
    }

    private static void initLogDetailAfterStatement() {
        if ( ! logBeforeStatement) {
            logDetailAfterStatement = true;
        } else {
            logDetailAfterStatement = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.logDetailAfterStatement", "true"));
        }
    }


    private static void initLogAddBatch() {
        logAddBatch = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.logAddBatch", "true"));
    }

    private static void initLogAddBatchDetail() {
        logAddBatchDetail = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.logAddBatchDetail", "true"));
    }

    private static void initLogExecuteBatchDetail() {
        logExecuteBatchDetail = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.logExecuteBatchDetail", "false"));
    }

    private static void initLogExceptions() {
        logExceptions = "true".equalsIgnoreCase(props.getProperty("jdbcdslog.logExceptions", "false"));
    }
    /* init parameters end. */
}
