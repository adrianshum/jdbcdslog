package org.jdbcdslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.util.TreeMap;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class CallableStatementLoggingHandler extends PreparedStatementLoggingHandler implements InvocationHandler {

    static Logger logger = LoggerFactory.getLogger(CallableStatementLoggingHandler.class);

    TreeMap namedParameters = new TreeMap();

    public CallableStatementLoggingHandler(CallableStatement ps, String sql) {
        super(ps, sql);
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final String methodName = "invoke() ";
        logger.debug("invoke() method = {}", method);

        Object r = null;
        try {
            boolean toLog = (StatementLogger.isInfoEnabled() || SlowQueryLogger.isInfoEnabled()) && executeMethods.contains(method.getName());
            long t1 = 0;
            if (toLog)
                t1 = System.nanoTime();
            logger.debug(methodName + "before method call..");
            r = method.invoke(target, args);
            logger.debug(methodName + "after method call. result = {}", r);

            if (setMethods.contains(method.getName()) && args[0] instanceof Integer)
                parameters.put(args[0], args[1]);
            if (setMethods.contains(method.getName()) && args[0] instanceof String)
                namedParameters.put(args[0], args[1]);
            if ("clearParameters".equals(method.getName()))
                parameters = new TreeMap();
            if (toLog) {
                long t2 = System.nanoTime();
                long time = t2 - t1;

                StringBuffer s = LogUtils.createLogEntry(method, sql, parameters, namedParameters);

                if (ConfigurationParameters.showTime) {
                    s.append(" ").append(String.format("%.9f", time/1000000000.0)).append(" s.");
                }

                StatementLogger.info(s.toString());

                if (time/1000000 >= ConfigurationParameters.slowQueryThreshold) {
                    SlowQueryLogger.info(s.toString());
                }
            }
            if (r instanceof ResultSet)
                r = ResultSetLoggingHandler.wrapByResultSetProxy((ResultSet) r);
        } catch (Throwable t) {
            LogUtils.handleException(t, StatementLogger.getLogger(), LogUtils.createLogEntry(method, sql, parameters, namedParameters));
        }
        return r;
    }

}
