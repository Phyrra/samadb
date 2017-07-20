package ch.sama.db.data;

import ch.sama.db.base.Table;

public class UnknownAliasException extends RuntimeException {
    public UnknownAliasException(String alias) {
        super("Unknown alias {" + alias + "}");
    }
}
