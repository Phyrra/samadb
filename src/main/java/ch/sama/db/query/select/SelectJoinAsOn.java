package ch.sama.db.query.select;

import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SelectJoinAsOn implements IStatement {
    private SelectJoinAs parent;
    private Function<Map<String, Map<String, Object>>, Boolean> filter;

    SelectJoinAsOn(SelectJoinAs parent, Function<Map<String, Map<String, Object>>, Boolean> filter) {
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

    public SelectWhere where(Function<Map<String, Map<String, Object>>, Boolean> filter) {
        return new SelectWhere(this, filter);
    }

    public SelectJoin join(String table) {
        return new SelectJoin(parent.getDatastore(),this, table);
    }
}
