package ch.sama.db.query.select;

import ch.sama.db.base.NotANumberException;
import ch.sama.db.base.UnknownFieldException;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.UnknownAliasException;

import java.util.Map;
import java.util.function.Function;

public class WhereCondition {
    private static Object getValue(Map<String, Map<String, Object>> row, String key) {
        String alias;
        String field;

        int idx = key.indexOf(".");
        if (idx == -1) {
            // TODO: Find field if it is unique?

            if (row.keySet().size() > 1) {
                throw new UnknownFieldException(key);
            }

            alias = row.keySet().iterator().next();
            field = key;
        } else {
            alias = key.substring(0, idx);
            field = key.substring(idx + 1);
        }

        if (!row.containsKey(alias)) {
            throw new UnknownAliasException(alias);
        }

        Map<String, Object> set = row.get(alias);

        if (!set.containsKey(field)) {
            throw new UnknownFieldException(alias, field);
        }

        return set.get(field);
    }

    private static double toNumber(String field, Object value) throws NotANumberException {
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

        throw new NotANumberException(field);
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> eq(String field, Object value) {
        return row -> {
            Object fieldVal = getValue(row, field);

            if (fieldVal == null) {
                return value == null;
            }

            return fieldVal.equals(value);
        };
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> neq(String field, Object value) {
        return row -> !eq(field, value).apply(row);
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> lt(String field, Double value) {
        return row -> {
            Object fieldVal = getValue(row, field);

            return fieldVal != null && toNumber(field, fieldVal) < value;

        };
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> gt(String field, Double value) {
        return row -> {
            Object fieldVal = getValue(row, field);

            return fieldVal != null && toNumber(field, fieldVal) > value;

        };
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> lte(String field, Double value) {
        return row -> {
            Object fieldVal = getValue(row, field);

            return fieldVal != null && toNumber(field, fieldVal) <= value;

        };
    }

    public static Function<Map<String, Map<String, Object>>, Boolean> gte(String field, Double value) {
        return row -> {
            Object fieldVal = getValue(row, field);

            return fieldVal != null && toNumber(field, fieldVal) >= value;

        };
    }

    public Function<Map<String, Map<String, Object>>, Boolean> isNull(String field) {
        return row -> getValue(row, field) == null;
    }
}
