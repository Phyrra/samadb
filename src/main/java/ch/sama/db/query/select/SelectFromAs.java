package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.UnknownTableException;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.DataSet;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class SelectFromAs implements IStatement {
    private SelectFrom parent;
    private String alias;

    SelectFromAs(SelectFrom parent, String alias) {
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

    public SelectWhere where(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new SelectWhere(this, filter);
    }

    public SelectJoin join(String table) {
        return new SelectJoin(parent.getDatastore(), this, table);
    }
}
