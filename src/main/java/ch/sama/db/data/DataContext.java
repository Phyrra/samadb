package ch.sama.db.data;

import java.util.*;
import java.util.stream.Collectors;

public class DataContext {
    private Set<String> knownAliases;
    private List<Map<String, Map<String, Object>>> data;

    public DataContext() {
        knownAliases = new HashSet<>();
        data = new ArrayList<>();
    }

    public DataContext(Set<String> knownAliases, List<Map<String, Map<String, Object>>> data) {
        this.knownAliases = knownAliases;
        this.data = data;
    }

    public void registerAlias(String alias) {
        if (knownAliases.contains(alias)) {
            throw new DuplicateAliasException(alias);
        }

        knownAliases.add(alias);
    }

    public DataContext addRow(Map<String, Map<String, Object>> row) {
        data.add(row);

        return this;
    }

    public List<Map<String, Map<String, Object>>> getData() {
        return data;
    }

    public Set<String> getKnownAliases() {
        return knownAliases;
    }

    public DataContext fill() {
    	Map<String, Map<String, Object>> fullMap = new HashMap<>();

    	knownAliases.forEach(alias -> {
    		Map<String, Object> map = data.stream()
					.filter(row -> {
						return row.containsKey(alias);
					})
					.findFirst()
					.get()
					.get(alias);

    		Map<String, Object> empty = new HashMap<>();
    		map.keySet().forEach(key -> empty.put(key, null));

    		fullMap.put(alias, empty);
		});

    	data.forEach(row -> {
    		knownAliases.stream()
					.filter(alias -> !row.containsKey(alias))
					.forEach(alias -> {
						row.put(alias, fullMap.get(alias));
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
