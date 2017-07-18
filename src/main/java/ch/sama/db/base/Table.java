package ch.sama.db.base;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Table {
    private String name;
    private Map<String, Field> fields;

    public Table(String name) {
        this.name = name;
        this.fields = new HashMap<>();
    }

    public Table addField(Field field) {
        if (hasField(field)) {
            throw new DuplicateFieldException(this, field);
        }

        fields.put(field.getName(), field);

        return this;
    }

    public String getName() {
        return name;
    }

    public Collection<Field> getFields() {
        return fields.values();
    }

    public boolean hasField(Field field) {
        return hasField(field.getName());
    }

    public boolean hasField(String field) {
        return fields.containsKey(field);
    }

    public Field getField(String field) {
        return fields.get(field);
    }
}
