package ch.sama.db.query.select;

import ch.sama.db.base.NotANumberException;
import ch.sama.db.base.UnknownFieldException;
import ch.sama.db.data.UnknownAliasException;

import java.util.Map;
import java.util.function.Function;

public class Condition {
    private static final double EPS = 1e-6;

    private static double toNumber(Object value) throws NotANumberException {
        if (value instanceof Integer) {
            return (double) (Integer) value;
        }

        if (value instanceof Short) {
            return (double) (Short) value;
        }

        if (value instanceof Long) {
            return (double) (Long) value;
        }

        if (value instanceof Float) {
            return (double) (Float) value;
        }

        if (value instanceof Double) {
            return (double) value;
        }

        throw new NotANumberException(value);
    }

    private static Object getValue(Map<String, Map<String, Object>> row, Object value) {
        if (value instanceof String) {
            String string = (String) value;

            if (string.startsWith("'") && string.endsWith("'")) {
                return string;
            }

            return Util.extractValue(row, string);
        }

        return value;
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> eq(Object lhs, Object rhs) {
        return row -> {
            Object realLhs = getValue(row, lhs);
            Object realRhs = getValue(row, rhs);

            if (realLhs == null && realRhs == null) {
                return true;
            }

            if (realLhs == null || realRhs == null) {
                return false;
            }

            return realLhs.equals(realRhs);
        };
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> neq(Object lhs, Object rhs) {
        return row -> !eq(lhs, rhs).apply(row);
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> lt(Object lhs, Object rhs) {
        return row -> {
            double realLhs = toNumber(getValue(row, lhs));
            double realRhs = toNumber(getValue(row, rhs));

            return realLhs < realRhs;
        };
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> gt(Object lhs, Object rhs) {
        return row -> {
            double realLhs = toNumber(getValue(row, lhs));
            double realRhs = toNumber(getValue(row, rhs));

            return realLhs > realRhs;
        };
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> lte(Object lhs, Object rhs) {
        return row -> {
            double realLhs = toNumber(getValue(row, lhs));
            double realRhs = toNumber(getValue(row, rhs));

            return realLhs < realRhs || Math.abs(realLhs - realRhs) < EPS;
        };
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> gte(Object lhs, Object rhs) {
        return row -> {
            double realLhs = toNumber(getValue(row, lhs));
            double realRhs = toNumber(getValue(row, rhs));

            return realLhs > realRhs || Math.abs(realLhs - realRhs) < EPS;
        };
    }

    public Function<Map<String, Map<String, Object>>, Boolean> isNull(Object obj) {
        return row -> getValue(row, obj) == null;
    }
}
