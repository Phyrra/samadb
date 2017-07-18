package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.Table;

import java.util.List;

public class Select {
    private Datastore datastore;
    private List<String> fields;
    // TODO: *

    public Select(Datastore datastore, List<String> fields) {
        this.datastore = datastore;
        this.fields = fields;
    }

    Datastore getDatastore() {
        return datastore;
    }

    List<String> getFields() {
        return fields;
    }

    public SelectFrom from(Table table) {
        return from(table.getName());
    }

    public SelectFrom from(String table) {
        return new SelectFrom(this, table);
    }
}
