package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.UnknownTableException;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.DataSet;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class From implements IStatement {
    private Select parent;
    private String table;

    From(Select parent, String table) {
        this.parent = parent;
        this.table = table;
    }

    Datastore getDatastore() {
        return parent.getDatastore();
    }

    DataContext getContext(String alias) throws UnknownTableException {
        Datastore datastore = parent.getDatastore();

        if (!datastore.hasTable(table)) {
            throw new UnknownTableException(table);
        }

        DataSet data = datastore.getData(table);

        DataContext context = new DataContext();
        context.registerAlias(alias, datastore.getTable(table));

        data.getRows().stream()
                .map(DataRow::toMap)
                .map(row -> {
                    Map<String, Map<String, Object>> map = new HashMap<>();
                    map.put(alias, row);

                    return map;
                })
                .forEach(context::addRow);

        return context;
    }

    @Override
    public DataContext getContext() {
        return getContext(table);
    }

    @Override
    public DataContext getFilteredContext(DataContext context) {
        return parent.getFilteredContext(context);
    }

    @Override
    public List<List<Tupel>> execute() {
        return getFilteredContext(getContext()).getFlattened();
    }

    public FromAs as(String alias) {
        return new FromAs(this, alias);
    }

    public Where where(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new Where(this, filter);
    }

    public Join join(String table) {
        return new Join(getDatastore(), this, table);
    }

    public Order orderBy(String key) {
    	return new Order(this, key);
	}
}
