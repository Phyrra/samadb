package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;

import java.util.Map;
import java.util.function.Function;

public class JoinAs implements IJoin2 {
	private Datastore datastore;
    private IJoin1 parent;
    private String alias;

    JoinAs(Datastore datastore, IJoin1 parent, String alias) {
    	this.datastore = datastore;
        this.parent = parent;
        this.alias = alias;
    }

    @Override
    public DataContext getContext(Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	return parent.getContext(alias, filter);
    }

	@Override
	public DataContext getFilteredContext(DataContext context) {
		return parent.getFilteredContext(context);
	}

    public JoinOn on(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new JoinOn(datastore, this, filter);
    }
}
