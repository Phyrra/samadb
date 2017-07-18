package ch.sama.db.query.select;

import ch.sama.db.base.NotANumberException;
import ch.sama.db.data.DataRow;

import java.util.function.Function;

public class WhereCondition {
    public static Function<DataRow, Boolean> eq(String field, Object value) {
        return row -> {
            Object fieldVal = row.get(field);

            if (fieldVal == null) {
                return value == null;
            }

            return row.get(field).equals(value);
        };
    }

    public static Function<DataRow, Boolean> neq(String field, Object value) {
        return row -> !eq(field, value).apply(row);
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

    public static Function<DataRow, Boolean> lt(String field, Double value) {
        return row -> {
            Object fieldVal = row.get(field);

            return fieldVal != null && toNumber(field, fieldVal) < value;

        };
    }

    public static Function<DataRow, Boolean> gt(String field, Double value) {
        return row -> {
            Object fieldVal = row.get(field);

            return fieldVal != null && toNumber(field, fieldVal) > value;

        };
    }

    public static Function<DataRow, Boolean> lte(String field, Double value) {
        return row -> {
            Object fieldVal = row.get(field);

            return fieldVal != null && toNumber(field, fieldVal) <= value;

        };
    }

    public static Function<DataRow, Boolean> gte(String field, Double value) {
        return row -> {
            Object fieldVal = row.get(field);

            return fieldVal != null && toNumber(field, fieldVal) >= value;

        };
    }

    public Function<DataRow, Boolean> isNull(String field) {
        return row -> row.get(field) == null;
    }
}
