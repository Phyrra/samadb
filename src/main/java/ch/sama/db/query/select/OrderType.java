package ch.sama.db.query.select;

import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.List;
import java.util.stream.Collectors;

public class OrderType implements IStatement {
    private Order parent;
    private Order.Type type;

    OrderType(Order parent, Order.Type type) {
        this.parent = parent;
        this.type = type;
    }

    @Override
    public DataContext getContext() {
        return parent.getContext(type);
    }

    @Override
    public DataContext getFilteredContext(DataContext context) {
        return parent.getFilteredContext(context);
    }

    @Override
    public List<List<Tupel>> execute() {
        return getFilteredContext(getContext()).getFlattened();
    }
}
