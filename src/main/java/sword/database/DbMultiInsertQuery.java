package sword.database;

import sword.collections.ImmutableIntKeyMap;
import sword.collections.ImmutableList;
import sword.collections.MutableIntKeyMap;
import sword.collections.MutableList;

public final class DbMultiInsertQuery {

    private final DbTable _table;
    private final ImmutableIntKeyMap<DbValue> _repeatedValues;
    private final int _wildcardColumn;
    private final ImmutableList<DbValue> _wildcardValues;

    private DbMultiInsertQuery(DbTable table, ImmutableIntKeyMap<DbValue> repeatedValues, int wildcardColumn, ImmutableList<DbValue> wildcardValues) {
        if (table == null || repeatedValues == null || wildcardColumn < 0 || wildcardColumn >= table.columns().size()) {
            throw new IllegalArgumentException();
        }

        _table = table;
        _repeatedValues = repeatedValues;
        _wildcardColumn = wildcardColumn;
        _wildcardValues = wildcardValues;
    }

    public DbTable getTable() {
        return _table;
    }

    public ImmutableList<DbColumn> columns() {
        final ImmutableList<DbColumn> tableColumns = _table.columns();
        return _repeatedValues.keySet().toList().append(_wildcardColumn).map(tableColumns::valueAt);
    }

    public ImmutableIntKeyMap<DbValue> repeatedValues() {
        return _repeatedValues;
    }

    public ImmutableList<DbValue> wildcardValues() {
        return _wildcardValues;
    }

    public ImmutableList<DbInsertQuery> queryList() {
        return _wildcardValues.map(value -> new DbInsertQuery(_table, _repeatedValues.put(_wildcardColumn, value)));
    }

    public static final class WildcardBuilder {
        private final DbTable _table;
        private final ImmutableIntKeyMap<DbValue> _repeatedValues;
        private final int _wildcardColumn;
        private final MutableList<DbValue> _wildcardValues = MutableList.empty();

        private WildcardBuilder(DbTable table, ImmutableIntKeyMap<DbValue> repeatedValues, int wildcardColumn) {
            if (table == null || repeatedValues == null || wildcardColumn < 0 || wildcardColumn >= table.columns().size()) {
                throw new IllegalArgumentException();
            }

            _table = table;
            _repeatedValues = repeatedValues;
            _wildcardColumn = wildcardColumn;
        }

        public WildcardBuilder add(DbValue value) {
            _wildcardValues.append(value);
            return this;
        }

        public WildcardBuilder add(int value) {
            return add(new DbIntValue(value));
        }

        public WildcardBuilder add(String value) {
            return add(new DbStringValue(value));
        }

        public DbMultiInsertQuery build() {
            return new DbMultiInsertQuery(_table, _repeatedValues, _wildcardColumn, _wildcardValues.toImmutable());
        }
    }

    public static final class Builder implements DbSettableQueryBuilder {
        private final DbTable _table;
        private final MutableIntKeyMap<DbValue> _values = MutableIntKeyMap.empty();

        public Builder(DbTable table) {
            if (table == null) {
                throw new IllegalArgumentException();
            }

            _table = table;
        }

        private Builder put(int column, DbValue value) {
            if (column < 0 || column >= _table.columns().size() || _values.keySet().contains(column)) {
                throw new IllegalArgumentException();
            }

            _values.put(column, value);
            return this;
        }

        @Override
        public Builder put(int column, int value) {
            return put(column, new DbIntValue(value));
        }

        public Builder put(int column, String value) {
            return put(column, new DbStringValue(value));
        }

        public WildcardBuilder wildcard(int column) {
            return new WildcardBuilder(_table, _values.toImmutable(), column);
        }
    }
}
