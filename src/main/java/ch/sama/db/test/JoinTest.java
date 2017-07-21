package ch.sama.db.test;

import ch.sama.db.Datastore;
import ch.sama.db.base.Field;
import ch.sama.db.base.Table;
import ch.sama.db.data.Tupel;

import java.util.HashMap;
import java.util.List;

import static ch.sama.db.query.select.WhereCondition.eq;
import static ch.sama.db.query.select.WhereCondition.gt;

public class JoinTest {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        Datastore datastore = new Datastore();

        Table client = new Table("client")
                .addField(new Field<>("id", Integer.class))
                .addField(new Field<>("name", String.class));

        Table account = new Table("account")
				.addField(new Field<>("id", Integer.class))
				.addField(new Field<>("bank", String.class))
				.addField(new Field<>("ref", Integer.class));

        datastore
				.addTable(client)
				.addTable(account);

        datastore.insert()
                .into(client)
                .values(
                        new HashMap<String, Object>() {{
                            put("id", 1);
                            put("name", "Adam");
                        }},
                        new HashMap<String, Object>() {{
                            put("id", 2);
                            put("name", "Eve");
                        }},
						new HashMap<String, Object>() {{
							put("id", 3);
							put("name", "Charlie");
						}}
                )
                .execute();

        datastore.insert()
				.into(account)
				.values(
						new HashMap<String, Object>() {{
							put("id", 1);
							put("bank", "UBS");
							put("ref", 1);
						}},
						new HashMap<String, Object>() {{
							put("id", 2);
							put("bank", "CSS");
							put("ref", 1);
						}},
						new HashMap<String, Object>() {{
							put("id", 3);
							put("bank", "UBS");
							put("ref", 2);
						}},
						new HashMap<String, Object>() {{
							put("id", 4);
							put("bank", "UBS");
							put("ref", 4);
						}}
				)
				.execute();

        List<List<Tupel>> result = datastore.select("c.name", "a.bank")
                .from("client").as("c")
				.join("account").as("a").on(eq("c.id", "a.ref"))
                .execute();

        printResult(result);
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
