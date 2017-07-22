package ch.sama.db.query.select;

import ch.sama.db.data.DataContext;

import java.util.Map;
import java.util.function.Function;

public interface ISelectJoin2 {
	DataContext getContext(Function<Map<String, Map<String, Object>>, Boolean> filter);
	DataContext getFilteredContext(DataContext context);
}
