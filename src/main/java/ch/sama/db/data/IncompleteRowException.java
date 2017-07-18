package ch.sama.db.data;

import ch.sama.db.base.Table;

public class IncompleteRowException extends RuntimeException {
    public IncompleteRowException(String message) {
        super(message);
    }
}
