package sword.database;

import sword.collections.ImmutableIntKeyMap;
import sword.collections.MutableIntPairMap;

public final class DbDeleteQuery {

    private final DbTable _table;
    private final ImmutableIntKeyMap<DbValue> _constraints;

    private DbDeleteQuery(DbTable table, ImmutableIntKeyMap<DbValue> constraints) {
        _table = table;
        _constraints = constraints;
    }

    public DbTable table() {
        return _table;
    }

    public ImmutableIntKeyMap<DbValue> constraints() {
        return _constraints;
    }

    public static final class Builder implements DbIdentifiableQueryBuilder {
        private final DbTable _table;
        private final MutableIntPairMap _constraints = MutableIntPairMap.empty();

        public Builder(DbTable table) {
            _table = table;
        }

        private void assertValidIntColumn(int columnIndex) {
            if (columnIndex < 0 || columnIndex >= _table.columns().size()) {
                throw new IllegalArgumentException("Wrong column index");
            }

            final DbColumn column = _table.columns().get(columnIndex);
            if (column.isText()) {
                throw new IllegalArgumentException("Column is not an integer value");
            }
        }

        @Override
        public Builder where(int columnIndex, int value) {
            assertValidIntColumn(columnIndex);
            if (_constraints.keySet().contains(columnIndex)) {
                throw new IllegalArgumentException("Column already constrained");
            }

            _constraints.put(columnIndex, value);
            return this;
        }

        public DbDeleteQuery build() {
            if (_constraints.isEmpty()) {
                throw new AssertionError("No constraints set");
            }

            return new DbDeleteQuery(_table, _constraints.toImmutable().map(DbIntValue::new));
        }
    }
}
