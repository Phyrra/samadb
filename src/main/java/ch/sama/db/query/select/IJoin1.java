package ch.sama.db.query.select;

import ch.sama.db.data.DataContext;

import java.util.Map;
import java.util.function.Function;

public interface IJoin1 {
	DataContext getContext(String alias, Function<Map<String, Map<String, Object>>, Boolean> filter);
	DataContext getFilteredContext(DataContext context);
}
