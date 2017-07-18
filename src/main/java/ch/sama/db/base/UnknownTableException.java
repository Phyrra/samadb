package ch.sama.db.base;

public class UnknownTableException extends RuntimeException {
    public UnknownTableException(String table) {
        super("Unknown table {" + table + "}");
    }

    public UnknownTableException(Table table) {
        this(table.getName());
    }
}
