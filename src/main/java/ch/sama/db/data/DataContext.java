package ch.sama.db.data;

import ch.sama.db.base.UnknownFieldException;

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
