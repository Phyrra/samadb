package ch.sama.db.query.select;

import ch.sama.db.base.UnknownFieldException;
import ch.sama.db.data.UnknownAliasException;

import java.util.Map;

class Util {
    public static String[] getAliasFieldSet(Map<String, Map<String, Object>> row, String key) {
        String alias;
        String field;

        int idx = key.indexOf(".");
        if (idx == -1) {
            // TODO: Find field if it is unique?

            if (row.keySet().size() > 1) {
                throw new UnknownFieldException(key);
            }

            alias = row.keySet().iterator().next();
            field = key;
        } else {
            alias = key.substring(0, idx);
            field = key.substring(idx + 1);
        }

        return new String[] { alias, field };
    }

    public static Object extractValue(Map<String, Map<String, Object>> row, String key) {
        String[] aliasFieldSet = getAliasFieldSet(row, key);

        String alias = aliasFieldSet[0];
        String field = aliasFieldSet[1];

        if (!row.containsKey(alias)) {
            throw new UnknownAliasException(alias);
        }

        Map<String, Object> set = row.get(alias);

        if (!set.containsKey(field)) {
            throw new UnknownFieldException(alias, field);
        }

        return set.get(field);
    }
}
