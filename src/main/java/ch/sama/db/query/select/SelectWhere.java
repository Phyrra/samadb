package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.UnknownFieldException;
import ch.sama.db.base.UnknownTableException;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.DataSet;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SelectWhere {
    private SelectFrom parent;
    private Function<DataRow, Boolean> filter;

    SelectWhere(SelectFrom parent, Function<DataRow, Boolean> filter) {
        this.parent = parent;
        this.filter = filter;
    }

    public List<Map<String, Object>> execute() throws UnknownTableException, UnknownFieldException {
        Datastore datastore = parent.getDatastore();
        String table = parent.getTable();

        if (!datastore.hasTable(table)) {
            throw new UnknownTableException(table);
        }

        DataSet data = datastore.getData(table);

        List<String> fields = parent.getFields();

        return data.getRows().stream()
                .filter(row -> filter.apply(row))
                .map(row -> row.toMap(fields))
                .collect(Collectors.toList());
    }
}
