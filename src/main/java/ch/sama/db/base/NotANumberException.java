package ch.sama.db.base;

public class NotANumberException extends RuntimeException {
    public NotANumberException(Object obj) {
        super("Field {" + (obj == null ? "null" : obj.toString()) + "} is not a number");
    }
}
