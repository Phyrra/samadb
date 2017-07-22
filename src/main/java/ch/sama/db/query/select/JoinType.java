package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;

import java.util.Map;
import java.util.function.Function;

public class JoinType implements IJoin1, IJoin2 {
	private Datastore datastore;
    private Join parent;
    private JoinBuilder.Type type;

    JoinType(Datastore datastore, Join parent, JoinBuilder.Type type) {
    	this.datastore = datastore;
        this.parent = parent;
        this.type = type;
    }

    @Override
    public DataContext getContext(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return parent.getContext(type, filter);
    }

    @Override
    public DataContext getContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return parent.getContext(type, alias, filter);
	}

	@Override
	public DataContext getFilteredContext(DataContext context) {
		return parent.getFilteredContext(context);
	}

	public JoinAs as(String alias) {
		return new JoinAs(datastore, this, alias);
	}

    public JoinOn on(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new JoinOn(datastore, this, filter);
    }
}
