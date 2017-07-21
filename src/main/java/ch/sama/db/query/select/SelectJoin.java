package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.UnknownTableException;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.DataSet;
import ch.sama.db.query.IStatement;

import java.util.*;
import java.util.function.Function;

public class SelectJoin implements ISelectJoin {
    private Datastore datastore;
    private IStatement parent;
    private String table;

    SelectJoin(Datastore datastore, IStatement parent, String table) {
        this.datastore = datastore;
        this.parent = parent;
        this.table = table;
    }

	@Override
	public DataContext getContext(Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return getContext(table, filter);
	}

	// TODO: Other than inner joins?
    DataContext getContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
        DataContext context = parent.getContext();

		DataSet tableData = datastore.getData(table);
		context.registerAlias(alias);

        Set<String> knownAliases = context.getKnownAliases();
        List<Map<String, Map<String, Object>>> data = context.getData();

		List<Map<String, Map<String, Object>>> joined = new ArrayList<>();

		data.forEach(row -> {
			tableData.getRows().stream()
					.map(DataRow::toMap)
					.map(rowMap -> {
						Map<String, Map<String, Object>> newSet = new HashMap<>();

						// Add existing data
						row.keySet()
								.forEach(key -> newSet.put(key, row.get(key)));

						// Add new data
						newSet.put(alias, rowMap);

						return newSet;
					})
					.filter(filter::apply)
					.forEach(joined::add);
		});

        return new DataContext(
                knownAliases,
                joined
        );
    }

    @Override
    public DataContext getFilteredContext(DataContext context) {
        return parent.getFilteredContext(context);
    }

    public SelectJoinAs as(String alias) {
        return new SelectJoinAs(datastore, this, alias);
    }

	public SelectJoinOn on(Function<Map<String, Map<String, Object>>, Boolean> filter) {
		return new SelectJoinOn(datastore, this, filter);
	}
}
