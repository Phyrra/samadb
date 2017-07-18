package ch.sama.db.query.insert;

import ch.sama.db.Datastore;
import ch.sama.db.base.Table;
import ch.sama.db.query.IStatement;

public class Insert implements IStatement {
    private Datastore datastore;

    public Insert(Datastore datastore) {
        this.datastore = datastore;
    }

    public InsertInto into(Table table) {
        return into(table.getName());
    }

    public InsertInto into(String table) {
        return new InsertInto(this, table);
    }

    Datastore getDatastore() {
        return datastore;
    }
}
