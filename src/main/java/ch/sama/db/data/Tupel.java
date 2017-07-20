package ch.sama.db.data;

public class Tupel {
    private String name;
    private Object value;

    public Tupel(String name, Object value) {
        this.name = name;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public Object getValue() {
        return value;
    }
}
