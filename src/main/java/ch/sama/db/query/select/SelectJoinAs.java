package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SelectJoinAs implements ISelectJoin {
	private Datastore datastore;
    private SelectJoin parent;
    private String alias;

    SelectJoinAs(Datastore datastore, SelectJoin parent, String alias) {
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
