package ch.sama.db.query.insert;

import ch.sama.db.Datastore;
import ch.sama.db.query.IStatement;

import java.util.Arrays;
import java.util.Map;

public class InsertInto implements IStatement {
    private Insert parent;
    private String table;

    InsertInto(Insert parent, String table) {
        this.parent = parent;
        this.table = table;
    }

    public InsertValues values(Map<String, Object>... values) {
        return new InsertValues(this, Arrays.asList(values));
    }

    Datastore getDataStore() {
        return parent.getDatastore();
    }

    String getTable() {
        return table;
    }
}
