package org.jdbcdslog;

import static org.jdbcdslog.Loggers.connectionLogger;
import static org.jdbcdslog.ProxyUtils.*;

import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;


public class ConnectionLoggingHandler extends LoggingHandlerSupport {
    private static final AtomicInteger ID_COUNTER = new AtomicInteger(1);
    protected int connectionId = 0;

    public ConnectionLoggingHandler(Connection target) {
        super(target);
        connectionId = ID_COUNTER.incrementAndGet();
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        try {
            if (method.getName().equals("commit") ||
                method.getName().equals("rollback")) {
                if (connectionLogger.isInfoEnabled()) {
                    connectionLogger.info(LogUtils.appendStackTrace(connectionId, method.getName()));
                }
            }
            Object r = method.invoke(target, args);
            if (UNWRAP_METHOD_NAME.equals(method.getName())) {
                Class<?> unwrapClass = (Class<?>) args[0];
                if (r == target && unwrapClass.isInstance(proxy)) {
                    r = proxy;      // returning original proxy if it is enough to represent the unwrapped obj
                } else if (unwrapClass.isInterface() && Connection.class.isAssignableFrom(unwrapClass)) {
                    r = wrapByConnectionProxy((Connection)r);
                }
            } else if (method.getName().equals("createStatement")) {
                r = wrapByStatementProxy(connectionId, (Statement) r);
            } else if (method.getName().equals("prepareCall")) {
                r = wrapByCallableStatementProxy(connectionId, (CallableStatement) r, (String) args[0]);
            } else if (method.getName().equals("prepareStatement")) {
                r = wrapByPreparedStatementProxy(connectionId, (PreparedStatement) r, (String) args[0]);
            } else {
                r = wrap(r, connectionId);
            }
            return r;
        } catch (Throwable t) {
            LogUtils.handleException(t, connectionLogger, LogUtils.createLogEntry(method, connectionId, null, null, null));
        }
        return null;
    }
}
