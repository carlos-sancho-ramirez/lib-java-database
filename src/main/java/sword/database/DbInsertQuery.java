package sword.database;

import sword.collections.ImmutableIntKeyMap;
import sword.collections.MutableIntKeyMap;

public final class DbInsertQuery {

    private final DbTable _table;
    private final ImmutableIntKeyMap<DbValue> _values;

    DbInsertQuery(DbTable table, ImmutableIntKeyMap<DbValue> values) {
        _table = table;
        _values = values;
    }

    public DbTable getTable() {
        return _table;
    }

    public int getColumnCount() {
        return _values.size();
    }

    public DbColumn getColumn(int index) {
        return _table.columns().get(_values.keyAt(index));
    }

    public DbValue getValue(int index) {
        return _values.valueAt(index);
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

        private DbInsertQuery.Builder put(int column, DbValue value) {
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

        public DbInsertQuery build() {
            return new DbInsertQuery(_table, _values.toImmutable());
        }
    }
}
