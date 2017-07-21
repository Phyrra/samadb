package ch.sama.db.test;

import ch.sama.db.Datastore;
import ch.sama.db.base.Field;
import ch.sama.db.base.Table;
import ch.sama.db.data.Tupel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static ch.sama.db.query.select.WhereCondition.gt;

public class Test {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        Datastore datastore = new Datastore();

        Table table = new Table("test")
                .addField(new Field<>("a", String.class))
                .addField(new Field<>("b", Double.class));

        datastore.addTable(table);

        datastore.insert()
                .into(table)
                .values(
                        new HashMap<String, Object>() {{
                            put("a", "Hello");
                            put("b", 12.3);
                        }},
                        new HashMap<String, Object>() {{
                            put("a", "Mark");
                            put("b", 17.3);
                        }},
                        new HashMap<String, Object>() {{
                            put("a", "Velo");
                            put("b", 19.8);
                        }}
                )
                .execute();

        List<List<Tupel>> result1 = datastore.select("*")
                .from("test")
                .execute();

        printResult(result1);

        List<List<Tupel>> result2 = datastore.select("t.a")
                .from("test").as("t")
                .where(gt("t.b", 17.0))
                .execute();

        printResult(result2);
    }

    private static void printResult(List<List<Tupel>> result) {
        System.out.println("result:");

        result.forEach(row -> {
            System.out.println("\trow:");

            row.forEach(entry -> {
                System.out.println("\t\t" + entry.getName() + " = " + entry.getValue());
            });
        });

        System.out.println("");
    }
}
