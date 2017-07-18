package ch.sama.db.data;

import ch.sama.db.base.UnknownFieldException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataRow {
    private Map<String, Object> row;

    public DataRow() {
        this.row = new HashMap<>();
    }

    public Map<String, Object> getRow() {
        return row;
    }

    public Object get(String key) throws UnknownFieldException {
        if (!row.containsKey(key)) {
            throw new UnknownFieldException(key);
        }

        return row.get(key);
    }

    public void put(String key, Object value) {
        row.put(key, value);
    }

    public Map<String, Object> toMap(List<String> fields) throws UnknownFieldException {
        Map<String, Object> map = new HashMap<>();

        fields.forEach(field -> map.put(field, get(field)));

        return map;
    }
}
