package org.jdbcdslog;

import static org.jdbcdslog.Loggers.connectionLogger;
import static org.jdbcdslog.ProxyUtils.*;

import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class ConnectionLoggingHandler extends LoggingHandlerSupport<Connection> {
    protected LogMetaData logMetaData = null;

    public ConnectionLoggingHandler(Connection target) {
        this(null, target);
    }

    public ConnectionLoggingHandler(LogMetaData logMetaData, Connection target) {
        super(target);
        if (logMetaData == null) {
            this.logMetaData = LogMetaData.create();
        } else {
            this.logMetaData = logMetaData;
        }

        // Logging for Connection creation
        Map<String,String> oldMdc = LogUtils.setMdc(this.logMetaData);
        if (connectionLogger.isInfoEnabled()) {
            try {
                DatabaseMetaData md = target.getMetaData();
                connectionLogger.info("Connected to URL {} for user {}", md.getURL(), md.getUserName());
            } catch (SQLException ex) {
                connectionLogger.error("Problem reading connection metadata", ex);
            } finally {
                LogUtils.resetMdc(oldMdc);
            }
        }
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        Map<String, String> oldMdc = LogUtils.setMdc(this.logMetaData);

        try {
            if (method.getName().equals("commit") ||
                    method.getName().equals("rollback")) {
                if (connectionLogger.isInfoEnabled()) {
                    connectionLogger.info(LogUtils.appendStackTrace(method.getName()));
                }
            }
            Object r = method.invoke(target, args);
            if (UNWRAP_METHOD_NAME.equals(method.getName())) {
                Class<?> unwrapClass = (Class<?>) args[0];
                if (r == target && unwrapClass.isInstance(proxy)) {
                    r = proxy;      // returning original proxy if it is enough to represent the unwrapped obj
                } else if (unwrapClass.isInterface() && Connection.class.isAssignableFrom(unwrapClass)) {
                    r = wrapByConnectionProxy(logMetaData, (Connection)r);
                }
            } else if (method.getName().equals("createStatement")) {
                r = wrapByStatementProxy(logMetaData, (Statement) r);
            } else if (method.getName().equals("prepareCall")) {
                r = wrapByCallableStatementProxy(logMetaData, (CallableStatement) r, (String) args[0]);
            } else if (method.getName().equals("prepareStatement")) {
                r = wrapByPreparedStatementProxy(logMetaData, (PreparedStatement)r, (String) args[0]);
            } else {
                r = wrap(logMetaData, r);
            }
            return r;
        } catch (Throwable t) {
            LogUtils.handleException(t, connectionLogger, LogUtils.createLogEntry(method, null, null, null));
        } finally {
            LogUtils.resetMdc(oldMdc);
        }
        return null;
    }
}
