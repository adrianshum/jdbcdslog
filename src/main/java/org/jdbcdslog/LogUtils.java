package org.jdbcdslog;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.MDC;

public class LogUtils {

    public final static String CONNECTION_ID_MDC_KEY = "jdbcdslog.connectionId";

    private final static String NAMED_PARAMETERS_PREFIX = ":";

    public static void handleException(Throwable e, Logger l, StringBuilder msg) throws Throwable {
        if (e instanceof InvocationTargetException) {
            e = ((InvocationTargetException) e).getTargetException();
        }

        if (ConfigurationParameters.logExceptions) {
            l.error(msg.toString(), e);
        }
        throw e;
    }

    /**
     * Append Elapsed Time to log message if it is configured to be included.
     */
    public static StringBuilder appendElapsedTime(StringBuilder sb, long elapsedTimeInNano) {
        if (ConfigurationParameters.showTime) {
            sb.append("\nElapsed Time: ").append(String.format("%.9f", elapsedTimeInNano/1000000000.0)).append(" s.");
        }
        return sb;

    }

    public static String appendStackTrace(String message) {
        if (ConfigurationParameters.printStackTrace) {
            return appendStackTrace(new StringBuilder(message)).toString();
        } else {
            return message;
        }
    }

    public static StringBuilder appendStackTrace(StringBuilder sb) {
        if (ConfigurationParameters.printStackTrace) {
            StackTraceElement[] stackTraces = new Throwable().getStackTrace();
            int firstNonJdbcDsLogStackIndex = firstNonJdbcDsLogStackIndex(stackTraces);

            if (ConfigurationParameters.printFullStackTrace) {
                for (int i = firstNonJdbcDsLogStackIndex; i < stackTraces.length; ++i) {
                    sb.append("\nat ").append(stackTraces[i]);
                }
            } else if (ConfigurationParameters.printStackTracePattern.length() == 0) {
                sb.append("\nat ").append(stackTraces[firstNonJdbcDsLogStackIndex]);
            } else {   // pattern provided
                String matchPattern =  ConfigurationParameters.printStackTracePattern;
                for (StackTraceElement stackTraceElement : stackTraces) {
                    if ( stackTraceElement.getClassName().matches(matchPattern)){
                        sb.append("\nat ").append(stackTraceElement);
                        break;
                    }
                }
            }
        }

        return sb;
    }

    public static int firstNonJdbcDsLogStackIndex(StackTraceElement[] stackTraces) {
        int i;
        for (i = 0; i < stackTraces.length; ++i) {
            if ( ! stackTraces[i].getClassName().startsWith("org.jdbcdslog")) {
                break;
            }
        }

        try {
            if (i > 0 && Proxy.isProxyClass(Class.forName(stackTraces[i].getClassName()))) {
                ++i;    // skip one more level for the proxy
            }
        } catch (ClassNotFoundException ignore) {
        }

        return i;
    }

    public static StringBuilder createLogEntry(Method method, String sql, Map<Integer,Object> parameters, Map<String,Object> namedParameters) {
        StringBuilder s = new StringBuilder();
        if (method != null) {
            s.append(method.getDeclaringClass().getName()).append(".").append(method.getName()).append(": ");
        }
        appendSql(s, sql, parameters, namedParameters);

        return s;
    }

    public static void appendSql(StringBuilder s,
                                 String sql,
                                 Map<Integer, Object> parameters,
                                 Map<String, Object> namedParameters) {

        if (ConfigurationParameters.inlineQueryParams) {
            if (parameters != null && !parameters.isEmpty()) {
                appendSqlWithInlineIndexedParams(s, sql, parameters);
            } else {
                appendSqlWithInlineNamedParams(s, sql, namedParameters);
            }
        } else {    // display separate query parameters
            appendSqlWithSeparateParams(s, sql, parameters, namedParameters);

        }

    }

    protected static void appendSqlWithSeparateParams(StringBuilder s,
                                                    String sql,
                                                    Map<Integer, Object> parameters,
                                                    Map<String, Object> namedParameters) {
        if (sql != null) {
            s.append(sql);
        }
        if (parameters != null && !parameters.isEmpty()) {
            s.append(" parameters: ")
                .append(parameters);
        } else if (namedParameters != null && !namedParameters.isEmpty()){
            s.append(" named parameters: ")
                .append(namedParameters);
        }
    }


    public static void appendBatchSqls(StringBuilder s,
                                 String sql,
                                 List<Map<Integer, Object>> parameters,
                                 List<Map<String, Object>> namedParameters) {

        if (ConfigurationParameters.inlineQueryParams) {
            if (parameters != null) {
                for (Map<Integer, Object> p : parameters) {
                    if (s.length() > 0) {
                        s.append("\n\t");
                    }
                    appendSqlWithInlineIndexedParams(s, sql, p);
                }
            }
            if (namedParameters != null) {
                for (Map<String, Object> p : namedParameters) {
                    if (s.length() > 0) {
                        s.append("\n\t");
                    }
                    appendSqlWithInlineNamedParams(s, sql, p);
                }
            }
        } else {    // display separate query parameters
            appendBatchSqlsWithSeparateParams(s, sql, parameters, namedParameters);
        }
    }

    protected static void appendBatchSqlsWithSeparateParams(StringBuilder s,
                                                    String sql,
                                                    List<Map<Integer, Object>> parameters,
                                                    List<Map<String, Object>> namedParameters) {
        if (sql != null) {
            s.append(sql);
        }
        if (parameters != null && !parameters.isEmpty()) {
            s.append(" parameters: ")
                .append(parameters);
        } else if (namedParameters != null && !namedParameters.isEmpty()){
            s.append(" named parameters: ")
                .append(namedParameters);
        }
    }

    protected static void appendSqlWithInlineIndexedParams(StringBuilder sb, String sql, Map<Integer,Object> parameters) {

        if (sql != null) {
            int questionMarkCount = 1;
            Pattern p = Pattern.compile("\\?");
            Matcher m = p.matcher(sql);
            StringBuffer sqlStringBuffer = new StringBuffer();

            while (m.find()) {
                m.appendReplacement(sqlStringBuffer, ConfigurationParameters.rdbmsSpecifics.formatParameter(parameters.get(questionMarkCount)));
                questionMarkCount++;
            }
            m.appendTail(sqlStringBuffer);

            sb.append(sqlStringBuffer).append(";");
        }
    }

    protected static void appendSqlWithInlineNamedParams(StringBuilder sb, String sql, Map<String,Object> namedParameters) {
        if (sql != null) {
            if (namedParameters != null && !namedParameters.isEmpty()) {
                for (Entry<String, Object> entry : namedParameters.entrySet()) {
                    sql = sql.replaceAll(NAMED_PARAMETERS_PREFIX + entry.getKey(), ConfigurationParameters.rdbmsSpecifics.formatParameter(entry.getValue()));
                }
            }
            sb.append(sql).append(";");
        }

    }

    protected static Map<String, String> setMdc(LogMetaData logMetaData) {
        if (logMetaData == null) {
            return null;
        }
        Map<String, String> oldMdc = new HashMap<String,String>();

        if ( ! logMetaData.getConnectionId().equals(MDC.get(LogUtils.CONNECTION_ID_MDC_KEY))) {
            oldMdc.put(LogUtils.CONNECTION_ID_MDC_KEY, MDC.get(LogUtils.CONNECTION_ID_MDC_KEY));
            MDC.put(LogUtils.CONNECTION_ID_MDC_KEY, logMetaData.getConnectionId());
        }

        return oldMdc;
    }

    protected static void resetMdc(Map<String, String> oldMdc) {
        if (oldMdc == null) {
            return;
        }
        for (Map.Entry<String, String> entry: oldMdc.entrySet()) {
            if (entry.getValue() == null) {
                MDC.remove(entry.getKey());
            } else {
                MDC.put(entry.getKey(), entry.getValue());
            }
        }
    }

    // Refer apache common lang StringUtils.
    protected static String replaceEach(String text, String[] searchList, String[] replacementList) {

        // mchyzer Performance note: This creates very few new objects (one major goal)
        // let me know if there are performance requests, we can create a harness to measure

        if (text == null || text.length() == 0 || searchList == null ||
                searchList.length == 0 || replacementList == null || replacementList.length == 0) {
            return text;
        }

        int searchLength = searchList.length;
        int replacementLength = replacementList.length;

        // make sure lengths are ok, these need to be equal
        if (searchLength != replacementLength) {
            throw new IllegalArgumentException("Search and Replace array lengths don't match: "
                    + searchLength
                    + " vs "
                    + replacementLength);
        }

        // keep track of which still have matches
        boolean[] noMoreMatchesForReplIndex = new boolean[searchLength];

        // index on index that the match was found
        int textIndex = -1;
        int replaceIndex = -1;

        // index of replace array that will replace the search string found
        // NOTE: logic duplicated below START
        for (int i = 0; i < searchLength; i++) {
            if (noMoreMatchesForReplIndex[i] || searchList[i] == null ||
                    searchList[i].length() == 0 || replacementList[i] == null) {
                continue;
            }
            int tempIndex = text.indexOf(searchList[i]);

            // see if we need to keep searching for this
            if (tempIndex == -1) {
                noMoreMatchesForReplIndex[i] = true;
            } else {
                if (textIndex == -1 || tempIndex < textIndex) {
                    textIndex = tempIndex;
                    replaceIndex = i;
                }
            }
        }
        // NOTE: logic mostly below END

        // no search strings found, we are done
        if (textIndex == -1) {
            return text;
        }

        int start = 0;

        // get a good guess on the size of the result buffer so it doesn't have to double if it goes over a bit
        int increase = 0;

        // count the replacement text elements that are larger than their corresponding text being replaced
        for (int i = 0; i < searchList.length; i++) {
            if (searchList[i] == null || replacementList[i] == null) {
                continue;
            }
            int greater = replacementList[i].length() - searchList[i].length();
            if (greater > 0) {
                increase += 3 * greater; // assume 3 matches
            }
        }
        // have upper-bound at 20% increase, then let Java take over
        increase = Math.min(increase, text.length() / 5);

        StringBuilder buf = new StringBuilder(text.length() + increase);

        while (textIndex != -1) {

            for (int i = start; i < textIndex; i++) {
                buf.append(text.charAt(i));
            }
            buf.append(replacementList[replaceIndex]);

            start = textIndex + searchList[replaceIndex].length();

            textIndex = -1;
            replaceIndex = -1;
            int tempIndex;
            // find the next earliest match
            // NOTE: logic mostly duplicated above START
            for (int i = 0; i < searchLength; i++) {
                if (noMoreMatchesForReplIndex[i] || searchList[i] == null ||
                        searchList[i].length() == 0 || replacementList[i] == null) {
                    continue;
                }
                tempIndex = text.indexOf(searchList[i], start);

                // see if we need to keep searching for this
                if (tempIndex == -1) {
                    noMoreMatchesForReplIndex[i] = true;
                } else {
                    if (textIndex == -1 || tempIndex < textIndex) {
                        textIndex = tempIndex;
                        replaceIndex = i;
                    }
                }
            }
            // NOTE: logic duplicated above END

        }
        int textLength = text.length();
        for (int i = start; i < textLength; i++) {
            buf.append(text.charAt(i));
        }
        return buf.toString();
    }
}
