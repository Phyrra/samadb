package ch.sama.db.query.insert;

import ch.sama.db.Datastore;
import ch.sama.db.base.*;
import ch.sama.db.data.DataRow;
import ch.sama.db.data.DataSet;
import ch.sama.db.data.IncompleteRowException;
import ch.sama.db.query.IStatement;

import java.util.List;
import java.util.Map;

public class InsertValues implements IStatement {
    private InsertInto parent;
    private List<Map<String, Object>> rows;

    InsertValues(InsertInto parent, List<Map<String, Object>> rows) {
        this.parent = parent;
        this.rows = rows;
    }

    public void execute() throws UnknownTableException, IncompleteRowException, IllegalValueException {
        Datastore datastore = parent.getDataStore();
        String table = parent.getTable();

        if (!datastore.hasTable(table)) {
            throw new UnknownTableException(table);
        }

        Table tbl = datastore.getTable(table);
        DataSet data = datastore.getData(table);

        rows.forEach(row -> {
            insertRow(data, tbl, row);
        });
    }

    private void insertRow(DataSet dataSet, Table table, Map<String, Object> row) throws IncompleteRowException, IllegalValueException {
        DataRow dataRow = new DataRow();

        boolean hasAllFields = table.getFields().stream().allMatch(field -> {
            return row.containsKey(field.getName());
        });

        if (!hasAllFields) {
            throw new IncompleteRowException("Incomplete Row for table {" + table.getName() + "}");
        }

        row.keySet().forEach(key -> {
            if (!table.hasField(key)) {
                throw new UnknownFieldException(key);
            }

            Field field = table.getField(key);

            Object value = row.get(key);

            if (value == null) {
                if (!field.isNullable()) {
                    throw new IllegalValueException(field);
                }
            } else {
                if (field.getClazz() != value.getClass()) {
                    throw new IllegalValueException(field);
                }
            }

            dataRow.put(field.getName(), value);
        });

        dataSet.addRow(dataRow);
    }
}
