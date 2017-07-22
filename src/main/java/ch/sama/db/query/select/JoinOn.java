package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class JoinOn implements IStatement {
	private Datastore datastore;
    private IJoin2 parent;
    private Function<Map<String, Map<String, Object>>, Boolean> filter;

    JoinOn(Datastore datastore, IJoin2 parent, Function<Map<String, Map<String, Object>>, Boolean> filter) {
    	this.datastore = datastore;
        this.parent = parent;
        this.filter = filter;
    }

    @Override
    public DataContext getContext() {
        return parent.getContext(filter);
    }

    @Override
    public DataContext getFilteredContext(DataContext context) {
        return parent.getFilteredContext(context);
    }

    @Override
    public List<List<Tupel>> execute() {
        return getFilteredContext(getContext()).getFlattened();
    }

    public Where where(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new Where(this, filter);
    }

    public Join join(String table) {
        return new Join(datastore,this, table);
    }
}
