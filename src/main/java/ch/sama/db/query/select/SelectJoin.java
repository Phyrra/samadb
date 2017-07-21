package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;
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
    	return getContext(JoinBuilder.Type.INNER, table, filter);
	}

    DataContext getContext(JoinBuilder.Type type, String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return new JoinBuilder(type, datastore, parent.getContext(), table)
				.buildContext(alias, filter);
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
