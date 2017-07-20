package ch.sama.db;

import ch.sama.db.base.Field;
import ch.sama.db.base.Table;
import ch.sama.db.data.DataSet;
import ch.sama.db.query.insert.Insert;
import ch.sama.db.query.select.Select;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Datastore {
    private Map<String, Table> tables;
    private Map<String, DataSet> data;

    public Datastore() {
        this.tables = new HashMap<>();
        this.data = new HashMap<String, DataSet>();
    }

    public Datastore addTable(Table table) {
        String tableName = table.getName();

        tables.put(tableName, table);
        data.put(tableName, new DataSet());

        return this;
    }

    public boolean hasTable(Table table) {
        return hasTable(table.getName());
    }

    public boolean hasTable(String table) {
        return tables.containsKey(table);
    }

    public Table getTable(String table) {
        return tables.get(table);
    }

    public DataSet getData(String table) {
        return data.get(table);
    }

    public Insert insert() {
        return new Insert(this);
    }

    public Select select(Field... fields) {
        // TODO: this should be possible with streams too?
        String[] fieldNames = new String[fields.length];
        for (int i = 0; i < fields.length; ++i) {
            fieldNames[i] = fields[i].getName();
        }

        return select(fieldNames);
    }

    public Select select(String... fields) {
        return new Select(this, Arrays.asList(fields));
    }
}
