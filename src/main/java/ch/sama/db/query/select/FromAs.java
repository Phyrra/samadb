package ch.sama.db.query.select;

import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class FromAs implements IStatement {
    private From parent;
    private String alias;

    FromAs(From parent, String alias) {
        this.parent = parent;
        this.alias = alias;
    }

    @Override
    public DataContext getContext() {
        return parent.getContext(alias);
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
        return new Join(parent.getDatastore(), this, table);
    }
}
