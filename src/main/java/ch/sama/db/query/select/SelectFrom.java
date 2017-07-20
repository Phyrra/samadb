package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.UnknownFieldException;
import ch.sama.db.base.UnknownTableException;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.DataSet;
import ch.sama.db.data.Tupel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SelectFrom {
    private Select parent;
    private String table;
    private String alias;

    SelectFrom(Select parent, String table) {
        this.parent = parent;
        this.table = table;
        this.alias = null;
    }

    Datastore getDatastore() {
        return parent.getDatastore();
    }

    String getTable() {
        return table;
    }

    List<String> getFields() {
        return parent.getFields();
    }

    private String getAlias() {
        if (alias != null) {
            return alias;
        }

        return table;
    }

    public DataContext getContext() {
        Datastore datastore = parent.getDatastore();

        if (!datastore.hasTable(table)) {
            throw new UnknownTableException(table);
        }

        DataSet data = datastore.getData(table);

        String alias = getAlias();
        List<String> fields = parent.getFields();

        DataContext context = new DataContext();
        context.registerAlias(alias);

        data.getRows().stream()
                .map(row -> row.toMap(fields))
                .map(row -> {
                    Map<String, Map<String, Object>> map = new HashMap<>();
                    map.put(alias, row);

                    return map;
                })
                .forEach(context::addRow);

        return context;
    }

    public List<List<Tupel>> execute() throws UnknownTableException, UnknownFieldException {
        return getContext().getFlattened();
    }

    public SelectFrom as(String alias) {
        this.alias = alias;

        return this;
    }

    public SelectWhere where(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new SelectWhere(this, filter);
    }
}
