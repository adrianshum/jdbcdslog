package org.jdbcdslog;

import static org.jdbcdslog.Loggers.*;
import static org.jdbcdslog.ProxyUtils.*;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PreparedStatementLoggingHandler extends StatementLoggingHandlerTemplate {
    protected Map<Integer, Object> parameters = new TreeMap<Integer, Object>();

    protected String sql = null;

    protected List<Map<Integer, Object>> batchParameters;

    protected static final Set<String> SET_METHODS
            = new HashSet<String>(Arrays.asList("setAsciiStream", "setBigDecimal", "setBinaryStream", "setBoolean", "setByte",
                                                "setBytes", "setCharacterStream", "setDate", "setDouble", "setFloat",
                                                "setInt", "setLong", "setObject", "setShort", "setString",
                                                "setTime", "setTimestamp", "setURL" ));

    protected static final Set<String> EXECUTE_METHODS
            = new HashSet<String>(Arrays.asList("addBatch", "execute", "executeQuery", "executeUpdate", "executeBatch" ));

    public PreparedStatementLoggingHandler(int connectionId, PreparedStatement ps, String sql) {
        super(connectionId, ps);
        this.sql = sql;
    }


    @Override
    protected boolean needsLogging(Method method) {
        return (statementLogger.isInfoEnabled() || slowQueryLogger.isInfoEnabled())
                && EXECUTE_METHODS.contains(method.getName());
    }


    @Override
    protected void appendStatement(StringBuilder sb, Object proxy, Method method, Object[] args) {
        LogUtils.appendSql(sb, sql, parameters, null);
    }

    @Override
    protected void doAddBatch(Object proxy, Method method, Object[] args) {
        if (this.batchParameters == null) {
            this.batchParameters = new ArrayList<Map<Integer,Object>>();
        }

        this.batchParameters.add(new TreeMap<Integer, Object>(this.parameters));
    }

    @Override
    protected void appendBatchStatements(StringBuilder sb) {
        LogUtils.appendBatchSqls(sb, sql, batchParameters, null);
    }

    @Override
    protected Object doAfterInvoke(Object proxy,Method method, Object[] args, Object result) {
        Object r = result;

        if (UNWRAP_METHOD_NAME.equals(method.getName())) {
            Class<?> unwrapClass = (Class<?>)args[0];
            if (r == target && unwrapClass.isInstance(proxy)) {
                r = proxy;      // returning original proxy if it is enough to represent the unwrapped obj
            } else if (unwrapClass.isInterface() && PreparedStatement.class.isAssignableFrom(unwrapClass)) {
                r = wrapByPreparedStatementProxy(connectionId, (PreparedStatement) r, sql);
            }
        }

        if (r instanceof ResultSet) {
            r = wrapByResultSetProxy(connectionId, (ResultSet) r);
        }


        if (SET_METHODS.contains(method.getName()) && args[0] instanceof Integer) {
            parameters.put((Integer)args[0], args[1]);
        }

        if ("clearParameters".equals(method.getName())) {
            parameters.clear();
        }

        if (batchParameters != null && "executeBatch".equals(method.getName())) {
            batchParameters.clear();
        }


        return r;
    }

    @Override
    protected void handleException(Throwable t, Object proxy, Method method, Object[] args) throws Throwable {
        LogUtils.handleException(t, statementLogger, LogUtils.createLogEntry(method, connectionId, sql, parameters, null));
    }


}
