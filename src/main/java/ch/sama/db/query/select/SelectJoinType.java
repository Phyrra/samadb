package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;

import java.util.Map;
import java.util.function.Function;

public class SelectJoinType implements ISelectJoin {
	private Datastore datastore;
    private SelectJoin parent;
    private JoinBuilder.Type type;

    SelectJoinType(Datastore datastore, SelectJoin parent, JoinBuilder.Type type) {
    	this.datastore = datastore;
        this.parent = parent;
        this.type = type;
    }

    @Override
    public DataContext getContext(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return parent.getContext(type, filter);
    }

    DataContext getContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return parent.getContext(type, alias, filter);
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
