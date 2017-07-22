package ch.sama.db.query.select;

import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Order implements IStatement {
	enum Type {
		ASC,
		DESC;
	}

    private IStatement parent;
    private String field;

    Order(IStatement parent, String field) {
        this.parent = parent;
        this.field = field;
    }

    @Override
    public DataContext getContext() {
        return getContext(Order.Type.ASC);
    }

	@SuppressWarnings("unchecked")
    DataContext getContext(Order.Type type) {
		DataContext context = parent.getContext();

		return new DataContext(
				context.getKnownAliases(),
				context.getData().stream()
						.sorted((row1, row2) -> {
							Object lhs = Util.extractValue(row1, field);
							Object rhs = Util.extractValue(row2, field);

							if (lhs == null && rhs == null) {
								return 0;
							}

							if (lhs == null) {
								return 1;
							}

							if (rhs == null) {
								return -1;
							}

							if (lhs.getClass().equals(rhs.getClass())) {
								if (lhs instanceof Comparable && rhs instanceof Comparable) {
									return ((Comparable) lhs).compareTo((Comparable) rhs);
								}
							}

							throw new ComparisonException(lhs, rhs);
						})
						.collect(Collectors.toList())
		);
	}

    @Override
    public DataContext getFilteredContext(DataContext context) {
        return parent.getFilteredContext(context);
    }

    @Override
    public List<List<Tupel>> execute() {
        return getFilteredContext(getContext()).getFlattened();
    }

    public OrderType asc() {
    	return new OrderType(this, Order.Type.ASC);
	}

	public OrderType desc() {
    	return new OrderType(this, Order.Type.DESC);
	}
}
