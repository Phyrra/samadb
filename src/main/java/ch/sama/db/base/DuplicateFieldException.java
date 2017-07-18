package ch.sama.db.base;

public class DuplicateFieldException extends RuntimeException {
    public DuplicateFieldException(Table table, Field field) {
        super("Field {" + field.getName() + "} already exists on table {" + table.getName() + "}");
    }
}
