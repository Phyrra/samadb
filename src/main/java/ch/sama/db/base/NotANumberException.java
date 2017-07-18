package ch.sama.db.base;

public class NotANumberException extends RuntimeException {
    public NotANumberException(String field) {
        super("Field {" + field + "} is not a number");
    }

    public NotANumberException(Field field) {
        this(field.getName());
    }
}
