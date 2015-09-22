package org.jdbcdslog;

import static org.jdbcdslog.Loggers.resultSetLogger;
import static org.jdbcdslog.ProxyUtils.wrapByResultSetProxy;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Map;

public class ResultSetLoggingHandler extends LoggingHandlerSupport<ResultSet> {
    private int resultCount = 0;
    private long totalFetchTime = 0;
    private LogMetaData logMetaData = null;

    public ResultSetLoggingHandler(LogMetaData logMetaData, ResultSet target) {
        super(target);
        this.logMetaData = logMetaData;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Object r = null;
        long startTimeInNano = System.nanoTime();

        Map<String, String> oldMdc = LogUtils.setMdc(logMetaData);
        try {
            try {
                r = method.invoke(target, args);
            } catch (Throwable e) {
                LogUtils.handleException(e, resultSetLogger, LogUtils.createLogEntry(method, null, null, null));
            }

            if (UNWRAP_METHOD_NAME.equals(method.getName())) {
                Class<?> unwrapClass = (Class<?>)args[0];
                if (r == target && unwrapClass.isInstance(proxy)) {
                    r = proxy;      // returning original proxy if it is enough to represent the unwrapped obj
                } else if (unwrapClass.isInterface() && ResultSet.class.isAssignableFrom(unwrapClass)) {
                    r = wrapByResultSetProxy(logMetaData, target);
                }
            }

            if (resultSetLogger.isInfoEnabled() && method.getName().equals("next")) {
                long elapsedTimeInNano = System.nanoTime() - startTimeInNano;

                totalFetchTime += elapsedTimeInNano;

                if ((Boolean) r ) {     // next() returns true
                    ++resultCount;
                    if (resultSetLogger.isDebugEnabled()) {

                        ResultSetMetaData md = target.getMetaData();
                        StringBuilder sb = new StringBuilder(method.getDeclaringClass().getName()).append(".").append(method.getName()).append(": ");

                        sb.append(" {");
                        for (int i = 1; i <= md.getColumnCount(); i++) {
                            if ( i > 1) {
                                sb.append(", ");
                            }
                            sb.append(ConfigurationParameters.rdbmsSpecifics.formatParameter(target.getObject(i)));
                        }
                        sb.append("} Row Number: ").append(resultCount);

                        LogUtils.appendStackTrace(sb);
                        LogUtils.appendElapsedTime(sb, elapsedTimeInNano);

                        resultSetLogger.debug(sb.toString());
                    }

                } else {

                    StringBuilder sb = new StringBuilder(method.getDeclaringClass().getName()).append(".").append(method.getName()).append(": ")
                                            .append(" Total Results: ").append(resultCount)
                                            .append(".  Total Fetch Time: ").append(String.format("%.9f", totalFetchTime/1000000000.0)).append(" s.");
                    totalFetchTime = 0;
                    LogUtils.appendStackTrace(sb);
                    LogUtils.appendElapsedTime(sb, elapsedTimeInNano);

                    resultSetLogger.info(sb.toString());
                }

            }
        } finally {
            LogUtils.resetMdc(oldMdc);
        }
        return r;
    }

}
