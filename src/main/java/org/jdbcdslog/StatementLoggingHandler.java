package org.jdbcdslog;

import static org.jdbcdslog.Loggers.*;
import static org.jdbcdslog.ProxyUtils.*;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;


public class StatementLoggingHandler extends StatementLoggingHandlerTemplate {
    protected final static Set<String> EXECUTE_METHODS = new HashSet<String>(Arrays.asList("addBatch", "execute", "executeQuery", "executeUpdate", "executeBatch"));
    protected List<String> batchStatements = null;

    public StatementLoggingHandler(int connectionId, Statement statement) {
        super(connectionId, statement);
    }

    @Override
    protected boolean needsLogging(Method method) {
        return (statementLogger.isInfoEnabled() || slowQueryLogger.isInfoEnabled())
                && EXECUTE_METHODS.contains(method.getName());
    }

    @Override
    protected void appendStatement(StringBuilder sb, Object proxy, Method method, Object[] args) {
        LogUtils.appendSql(sb, (args == null || args.length == 0) ? null : args[0].toString(), null, null);
    }

    @Override
    protected void doAddBatch(Object proxy, Method method, Object[] args) {
        if (this.batchStatements == null) {
            this.batchStatements = new ArrayList<String>();
        }

        StringBuilder sb = new StringBuilder();
        appendStatement(sb, proxy, method, args);
        sb.append(";");
        this.batchStatements.add(sb.toString());

    }

    @Override
    protected void appendBatchStatements(StringBuilder sb) {
        if (this.batchStatements != null) {
            if (batchStatements.size() == 1) {
                sb.append(batchStatements.get(0));
            } else {
                for (String s : batchStatements) {
                    sb.append("\n\t").append(s);
                }
            }
            this.batchStatements = null;
        }
    }

    @Override
    protected Object doAfterInvoke(Object proxy,Method method, Object[] args, Object result) {
        Object r = result;

        if (UNWRAP_METHOD_NAME.equals(method.getName())) {
            Class<?> unwrapClass = (Class<?>)args[0];
            if (r == target && unwrapClass.isInstance(proxy)) {
                r = proxy;      // returning original proxy if it is enough to represent the unwrapped obj
            } else if (unwrapClass.isInterface() && Statement.class.isAssignableFrom(unwrapClass)) {
                r = wrapByStatementProxy(connectionId, (Statement) r);
            }
        }

        if (r instanceof ResultSet) {
            r = wrapByResultSetProxy(connectionId, (ResultSet) r);
        }

        return r;
    }

    @Override
    protected void handleException(Throwable t, Object proxy, Method method, Object[] args) throws Throwable {
        LogUtils.handleException(t, statementLogger,
                LogUtils.createLogEntry(method, connectionId, (args == null || args.length == 0) ? null : args[0].toString(), null, null));
    }

}
