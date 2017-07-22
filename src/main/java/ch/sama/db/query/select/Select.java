package ch.sama.db.query.select;

import ch.sama.db.Datastore;
import ch.sama.db.base.Table;
import ch.sama.db.base.UnknownFieldException;
import ch.sama.db.data.DataContext;
import ch.sama.db.data.Tupel;
import ch.sama.db.data.UnknownAliasException;
import ch.sama.db.query.IStatement;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Select implements IStatement{
    private Datastore datastore;
    private String[] fields;
    private String all;

    public Select(Datastore datastore, String... fields) {
        this.datastore = datastore;
        this.fields = fields;
    }

    private Map<String, Map<String, Object>> filter(Map<String, Map<String, Object>> row, String[] fields) {
        Map<String, Map<String, Object>> filtered = new HashMap<>();

        Arrays.asList(fields)
                .forEach(key -> {
                    String[] aliasFieldSet = Util.getAliasFieldSet(row, key);

                    String alias = aliasFieldSet[0];
                    String field = aliasFieldSet[1];

                    if (!row.containsKey(alias)) {
                        throw new UnknownAliasException(alias);
                    }

                    Map<String, Object> set = row.get(alias);

                    if (!set.containsKey(field)) {
                        throw new UnknownFieldException(alias, field);
                    }

                    if (!filtered.containsKey(alias)) {
                        filtered.put(alias, new HashMap<>());
                    }

                    filtered.get(alias).put(field, set.get(field));
                });

        return filtered;
    }

    Datastore getDatastore() {
        return datastore;
    }

    @Override
    public DataContext getContext() {
        // TODO

        return null;
    }

    @Override
    public DataContext getFilteredContext(DataContext context) {
        if (fields.length == 1 && "*".equals(fields[0])) {
            return context;
        }

        return new DataContext(
                context.getKnownAliases(),
                context.getData().stream()
                        .map(row -> filter(row, fields))
                        .collect(Collectors.toList())
        );
    }

    @Override
    public List<List<Tupel>> execute() {
        return getContext().getFlattened();
    }

    public From from(Table table) {
        return from(table.getName());
    }

    public From from(String table) {
        return new From(this, table);
    }
}
