package ch.sama.db.data;

public class DuplicateAliasException extends RuntimeException {
    public DuplicateAliasException(String alias) {
        super("Alias {" + alias + "} already exists");
    }
}
