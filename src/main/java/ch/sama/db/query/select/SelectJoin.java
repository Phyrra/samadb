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
import java.util.stream.Collectors;

public class SelectJoin {
    private Datastore datastore;
    private IStatement parent;
    private String table;

    SelectJoin(Datastore datastore, IStatement parent, String table) {
        this.datastore = datastore;
        this.parent = parent;
        this.table = table;
    }

    Datastore getDatastore() {
        return datastore;
    }

    // TODO: Other than inner joins?
    DataContext getContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
        DataContext context = parent.getContext();

        context.registerAlias(alias);

        Set<String> knownAliases = context.getKnownAliases();
        List<Map<String, Map<String, Object>>> data = context.getData();

        return new DataContext(
                knownAliases,
                data.stream()
                        .filter(filter::apply)
                        .collect(Collectors.toList())
        );
    }

    DataContext getFilteredContext(DataContext context) {
        return parent.getFilteredContext(context);
    }

    public SelectJoinAs as(String alias) {
        return new SelectJoinAs(this, alias);
    }
}
