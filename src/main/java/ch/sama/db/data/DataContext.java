package ch.sama.db.data;

import ch.sama.db.base.Table;

import java.util.*;
import java.util.stream.Collectors;

public class DataContext {
    private Map<String, Map<String, Object>> knownAliases;
    private List<Map<String, Map<String, Object>>> data;

    public DataContext() {
        knownAliases = new HashMap<>();
        data = new ArrayList<>();
    }

    public DataContext(Map<String, Map<String, Object>> knownAliases, List<Map<String, Map<String, Object>>> data) {
        this.knownAliases = knownAliases;
        this.data = data;
    }

    public void registerAlias(String alias, Table table) {
        if (knownAliases.containsKey(alias)) {
            throw new DuplicateAliasException(alias);
        }

        Map<String, Object> empty = new HashMap<>();
        table.getFields().forEach(field -> empty.put(field.getName(), null));

        knownAliases.put(alias, empty);
    }

    public DataContext addRow(Map<String, Map<String, Object>> row) {
        data.add(row);

        return this;
    }

    public List<Map<String, Map<String, Object>>> getData() {
        return data;
    }

    public Map<String, Map<String, Object>> getKnownAliases() {
        return knownAliases;
    }

    public DataContext fill() {
    	data.forEach(row -> {
    		knownAliases.keySet().stream()
					.filter(alias -> !row.containsKey(alias))
					.forEach(alias -> {
						row.put(alias, knownAliases.get(alias));
					});
		});

    	return this;
	}

    public List<List<Tupel>> getFlattened() {
        return data.stream()
                .map(all -> {
                    List<Tupel> list = new ArrayList<>();

                    all.values()
                            .forEach(map -> {
                                map.keySet()
                                        .forEach(key -> {
                                            list.add(new Tupel(key, map.get(key)));
                                        });
                            });

                    return list;
                })
                .collect(Collectors.toList());
    }
}
