package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

public class SelectJoinAs {
    private SelectJoin parent;
    private String alias;

    SelectJoinAs(SelectJoin parent, String alias) {
        this.parent = parent;
        this.alias = alias;
    }

    Datastore getDatastore() {
        return parent.getDatastore();
    }

    DataContext getContext(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return parent.getContext(alias, filter);
    }

    DataContext getFilteredContext(DataContext context) {
        return parent.getFilteredContext(context);
    }

    public SelectJoinAsOn on(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new SelectJoinAsOn(this, filter);
    }
}
