package ch.sama.db.base;

public class Field<T> {
    private String name;
    private Class<T> clazz;
    private boolean nullable;

    public Field(String name, Class<T> clazz, boolean nullable) {
        this.name = name;
        this.clazz = clazz;
        this.nullable = nullable;
    }

    public Field(String name, Class<T> clazz) {
        this(name, clazz, true);
    }

    public String getName() {
        return name;
    }

    public Class<T> getClazz() {
        return clazz;
    }

    public boolean isNullable() {
        return nullable;
    }
}
