package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;

import java.util.Map;
import java.util.function.Function;

public class SelectJoinAs implements ISelectJoin2 {
	private Datastore datastore;
    private ISelectJoin1 parent;
    private String alias;

    SelectJoinAs(Datastore datastore, ISelectJoin1 parent, String alias) {
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

    public SelectJoinOn on(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new SelectJoinOn(datastore, this, filter);
    }
}
