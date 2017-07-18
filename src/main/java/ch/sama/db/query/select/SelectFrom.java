package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.Table;
import ch.sama.db.base.UnknownFieldException;
import ch.sama.db.base.UnknownTableException;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.DataSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SelectFrom {
    private Select parent;
    private String table;

    SelectFrom(Select parent, String table) {
        this.parent = parent;
        this.table = table;
    }

    Datastore getDatastore() {
        return parent.getDatastore();
    }

    String getTable() {
        return table;
    }

    List<String> getFields() {
        return parent.getFields();
    }

    public List<Map<String, Object>> execute() throws UnknownTableException, UnknownFieldException {
        Datastore datastore = parent.getDatastore();

        if (!datastore.hasTable(table)) {
            throw new UnknownTableException(table);
        }

        DataSet data = datastore.getData(table);

        List<String> fields = parent.getFields();

        return data.getRows().stream()
                .map(row -> row.toMap(fields))
                .collect(Collectors.toList());
    }

    public SelectWhere where(Function<DataRow, Boolean> filter) {
        return new SelectWhere(this, filter);
    }
}
