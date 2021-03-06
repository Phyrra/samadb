package ch.sama.db.query.select;

import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.query.IStatement;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Where implements IStatement {
    private IStatement parent;
    private Function<Map<String, Map<String, Object>>, Boolean> filter;

    Where(IStatement parent, Function<Map<String, Map<String, Object>>, Boolean> filter) {
        this.parent = parent;
        this.filter = filter;
    }

    @Override
    public DataContext getContext() {
        DataContext context = parent.getContext();

        return new DataContext(
				context.getKnownAliases(),
				context.getData().stream()
                        .filter(filter::apply)
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

	public Order orderBy(String key) {
		return new Order(this, key);
	}
}
