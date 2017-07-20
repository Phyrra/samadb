package ch.sama.db.base;

public class UnknownFieldException extends RuntimeException {
    public UnknownFieldException(String field) {
        super("Unkown field {" + field + "}");
    }

    public UnknownFieldException(Field field) {
        this(field.getName());
    }

    public UnknownFieldException(String alias, String field) {
        this(alias + "." + field);
    }
}
