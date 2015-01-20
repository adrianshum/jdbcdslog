/*
 *  ProxyUtils.java
 *
 *  $id$
 *
 * Copyright (C) FIL Limited. All rights reserved
 *
 * This software is the confidential and proprietary information of
 * FIL Limited You shall not disclose such Confidential Information
 * and shall use it only in accordance with the terms of the license
 * agreement you entered into with FIL Limited.
 */

package org.jdbcdslog;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.sql.PooledConnection;
import javax.sql.XAConnection;

import static org.jdbcdslog.Loggers.connectionLogger;


/**
 * @author a511990
 */
public class ProxyUtils {
    private static Class<?>[] EMPTY_CLASS_ARRAY = new Class<?>[0];

    /**
     * Find out all interfaces from clazz that is compatible with requiredInterface,
     * and generate proxy base on that.
     */
    @SuppressWarnings("unchecked")
    public static <T> T proxyForCompatibleInterfaces(Class<?> clazz, Class<T> requiredInterface, InvocationHandler invocationHandler) {
        // TODO: can cache clazz+iface vs compatibleInterfaces to avoid repetitive lookup
        return (T)Proxy.newProxyInstance(clazz.getClassLoader(),
                                        findCompatibleInterfaces(clazz, requiredInterface),
                                        invocationHandler);
    }

    /**
     * Find all interfaces of clazz that is-a requiredInterface.
     */
    public static Class<?>[] findCompatibleInterfaces(Class<?> clazz, Class<?> requiredInterface) {
        ArrayList<Class<?>> interfaces = new ArrayList<Class<?>>();
        for ( ; ! clazz.equals(Object.class) ; clazz = clazz.getSuperclass()) {
            for (Class<?> iface : clazz.getInterfaces()) {
                if (requiredInterface.isAssignableFrom(iface)) {
                    interfaces.add(iface);
                }
            }
        }
        return interfaces.toArray(EMPTY_CLASS_ARRAY);
    }


    public static Statement wrapByStatementProxy(int connectionId, Statement r) {
        return ProxyUtils.proxyForCompatibleInterfaces(r.getClass(), Statement.class, new StatementLoggingHandler(connectionId, r));
    }

    public static PreparedStatement wrapByPreparedStatementProxy(int connectionId, PreparedStatement r, String sql) {
        return ProxyUtils.proxyForCompatibleInterfaces(r.getClass(), PreparedStatement.class, new PreparedStatementLoggingHandler(connectionId, r, sql));
    }

    public static CallableStatement wrapByCallableStatementProxy(int connectionId, CallableStatement r, String sql) {
        return ProxyUtils.proxyForCompatibleInterfaces(r.getClass(), CallableStatement.class, new CallableStatementLoggingHandler(connectionId, r, sql));
    }

    public static Connection wrapByConnectionProxy(Connection r) {
        ConnectionLoggingHandler loggingHandler = new ConnectionLoggingHandler(r);
        if (connectionLogger.isInfoEnabled()) {
            try {
                DatabaseMetaData md = r.getMetaData();
                String message = LogUtils.appendStackTrace(loggingHandler.connectionId, "Connected to URL {} for user {}");
                connectionLogger.info(message, md.getURL(), md.getUserName());
            } catch (SQLException ex) {
                connectionLogger.error("Problem reading metadata", ex);
            }
        }

        return ProxyUtils.proxyForCompatibleInterfaces(r.getClass(), Connection.class, loggingHandler);
    }

    public static ResultSet wrapByResultSetProxy(int connectionId, ResultSet r) {
        return ProxyUtils.proxyForCompatibleInterfaces(r.getClass(), ResultSet.class, new ResultSetLoggingHandler(connectionId, r));
    }

    public static XAConnection wrapByXaConnection(XAConnection con) {
        return ProxyUtils.proxyForCompatibleInterfaces(con.getClass(), XAConnection.class, new ConnectionSourceLoggingHandler(con));
    }

    public static PooledConnection wrapByPooledConnection(PooledConnection con) {
        return ProxyUtils.proxyForCompatibleInterfaces(con.getClass(), PooledConnection.class, new ConnectionSourceLoggingHandler(con));
    }


    /**
     * Convenient helper to wrap object base on its type.
     */
    public static Object wrap(Object r, int connectionId) {
        if (r == null) {
            return null;
        }
        if (Proxy.isProxyClass(r.getClass())) {
            throw new IllegalStateException("This should never happen!");
        }
        if (r instanceof Connection) {
            return wrapByConnectionProxy((Connection)r);
        } else if (r instanceof Statement) {
            return wrapByStatementProxy(connectionId, (Statement) r);
        } else if (r instanceof ResultSet) {
            return wrapByResultSetProxy(connectionId, (ResultSet) r);
        } else {
            return r;
        }
    }
}
