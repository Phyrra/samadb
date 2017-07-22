package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;
import ch.sama.db.query.IStatement;

import java.util.*;
import java.util.function.Function;

public class Join implements IJoin1, IJoin2 {
    private Datastore datastore;
    private IStatement parent;
    private String table;

    Join(Datastore datastore, IStatement parent, String table) {
        this.datastore = datastore;
        this.parent = parent;
        this.table = table;
    }

	@Override
	public DataContext getContext(Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return getContext(JoinBuilder.Type.INNER, table, filter);
	}

	@Override
	public DataContext getContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return getContext(JoinBuilder.Type.INNER, alias, filter);
	}

	DataContext getContext(JoinBuilder.Type type, Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return getContext(type, table, filter);
	}

    DataContext getContext(JoinBuilder.Type type, String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return new JoinBuilder(type, datastore, parent.getContext(), table)
				.buildContext(alias, filter);
    }

    @Override
    public DataContext getFilteredContext(DataContext context) {
        return parent.getFilteredContext(context);
    }

    public JoinType inner() {
    	return new JoinType(datastore, this, JoinBuilder.Type.INNER);
	}

    public JoinType left() {
    	return new JoinType(datastore, this, JoinBuilder.Type.LEFT);
	}

	public JoinType right() {
    	return new JoinType(datastore, this, JoinBuilder.Type.RIGHT);
	}

    public JoinAs as(String alias) {
        return new JoinAs(datastore, this, alias);
    }

	public JoinOn on(Function<Map<String, Map<String, Object>>, Boolean> filter) {
		return new JoinOn(datastore, this, filter);
	}
}
