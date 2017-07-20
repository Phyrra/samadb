package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.UnknownFieldException;
import ch.sama.db.base.UnknownTableException;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.DataSet;
import ch.sama.db.data.Tupel;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SelectWhere {
    private SelectFrom parent;
    private Function<Map<String, Map<String, Object>>, Boolean> filter;

    SelectWhere(SelectFrom parent, Function<Map<String, Map<String, Object>>, Boolean> filter) {
        this.parent = parent;
        this.filter = filter;
    }

    public DataContext getContext() {
        DataContext context = parent.getContext();

        Set<String> knownAliases = context.getKnownAliases();
        List<Map<String, Map<String, Object>>> data = context.getData();

        return new DataContext(
                knownAliases,
                data.stream()
                        .filter(row -> filter.apply(row))
                        .collect(Collectors.toList())
        );
    }

    public List<List<Tupel>> execute() throws UnknownTableException, UnknownFieldException {
        return getContext().getFlattened();
    }
}
