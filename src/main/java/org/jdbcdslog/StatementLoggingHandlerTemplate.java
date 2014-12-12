package org.jdbcdslog;

import static org.jdbcdslog.LogUtils.*;
import static org.jdbcdslog.Loggers.*;
import static org.jdbcdslog.ProxyUtils.wrap;

import java.lang.reflect.Method;

import org.slf4j.Logger;

/**
 * Template for statement logging handler, which includes slow query handling, and
 * before/after statement logging
 *
 * @author a511990
 */
public abstract class StatementLoggingHandlerTemplate extends LoggingHandlerSupport {
    protected int connectionId;

    public StatementLoggingHandlerTemplate(int connectionId, Object target) {
        super(target);
        this.connectionId = connectionId;
    }


    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        try {
            boolean needsLog = needsLogging(method);
            long startTimeInNano = 0;
            StringBuilder sb= null;

            if (isAddBatch(method)) {
                if (!ConfigurationParameters.logAddBatchDetail) {
                    needsLog = false;
                }
                if (ConfigurationParameters.logExecuteBatchDetail) {
                    doAddBatch(proxy, method, args);
                }
            }

            if (needsLog) {
                startTimeInNano = System.nanoTime();

                sb = new StringBuilder();
                if (ConfigurationParameters.logBeforeStatement) {
                    sb.append("START: ");      // Reserve space for START: and END:
                }
                sb.append(method.getDeclaringClass().getName()).append(".").append(method.getName()).append(": ");

                if (isExecuteBatch(method)) {
                    if (ConfigurationParameters.logExecuteBatchDetail) {
                        appendBatchStatements(sb);
                    }
                }  else {
                    appendStatement(sb, proxy, method, args);
                }

                appendStackTrace(sb);

                logBeforeInvoke(proxy, method, args, sb);
            }

            Object result = method.invoke(target, args);

            result = doAfterInvoke(proxy, method, args, result);

            if (needsLog) {
                long elapsedTimeInNano = System.nanoTime() - startTimeInNano;

                if (ConfigurationParameters.logBeforeStatement) {
                    sb.setCharAt(0, 'E');
                    sb.setCharAt(1, 'N');
                    sb.setCharAt(2, 'D');
                    sb.setCharAt(3, ':');
                    sb.setCharAt(4, ' ');
                    sb.setCharAt(5, ' ');
                    sb.setCharAt(6, ' ');
                }

                appendElapsedTime(sb, elapsedTimeInNano);

                logAfterInvoke(proxy, method, args, result, elapsedTimeInNano, sb);
            }
            return result;

        } catch (Throwable t) {
            handleException(t, proxy, method, args);
        }
        return null;
    }

    protected abstract void doAddBatch(Object proxy, Method method, Object[] args);

    protected abstract void appendBatchStatements(StringBuilder sb);

    protected boolean isExecuteBatch(Method method) {
        return method.getName().equals("executeBatch");
    }


    protected boolean isAddBatch(Method method) {
        return method.getName().equals("addBatch");
    }


    protected abstract void appendStatement(StringBuilder sb, Object proxy, Method method, Object[] args) ;

    protected boolean needsLogging(Method method) {
        return false;
    }

    protected void logBeforeInvoke(Object proxy, Method method, Object[] args, StringBuilder sb) {
        if (ConfigurationParameters.logBeforeStatement) {
            getLogger().info("[Conn #" + connectionId + "] " + sb.toString());
        }
    }

    protected Object doAfterInvoke(Object proxy, Method method, Object[] args, Object result) {
        return wrap(result, connectionId);
    }

    protected void logAfterInvoke(Object proxy, Method method, Object[] args, Object result, long elapsedTimeInNano, StringBuilder message) {

        StringBuilder endMessage = new StringBuilder("[Conn #").append(connectionId).append("] ");
        if ( ! ConfigurationParameters.logDetailAfterStatement) {
            // replace the log message to a simple message

            endMessage.append("END:    ")
                        .append(method.getDeclaringClass().getName()).append(".").append(method.getName())
                        .append(": ");
            appendStackTrace(endMessage);
            appendElapsedTime(endMessage, elapsedTimeInNano);

        } else {
            endMessage.append(message);
        }

        getLogger().info(endMessage.toString());

        if (elapsedTimeInNano >= ConfigurationParameters.slowQueryThresholdInNano) {
            getSlowQueryLogger().info(message.toString());       // log the original message
        }

    }

    protected boolean needsSlowOperationLogging(Object proxy, Method method, Object[] args, Object result, long elapsedTimeInNano) {
        return true;
    }

    protected void handleException(Throwable t, Object proxy, Method method, Object[] args) throws Throwable {
        LogUtils.handleException(t, getLogger(), LogUtils.createLogEntry(method, connectionId, null, null, null));
    }

    protected Logger getLogger() {
        return statementLogger;
    }
    protected Logger getSlowQueryLogger() {
        return slowQueryLogger;
    }

}
