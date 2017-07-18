package ch.sama.db.base;

public class IllegalValueException extends RuntimeException {
    public IllegalValueException(String field) {
        super("Illegal value for field {" + field + "}");
    }

    public IllegalValueException(Field field) {
        this(field.getName());
    }
}
