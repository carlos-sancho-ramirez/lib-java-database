package sword.database;

import sword.collections.ImmutableIntKeyMap;
import sword.collections.MutableIntKeyMap;
import sword.collections.MutableIntPairMap;

public final class DbUpdateQuery {

    private final DbTable _table;
    private final ImmutableIntKeyMap<DbValue> _constraints;
    private final ImmutableIntKeyMap<DbValue> _values;

    private DbUpdateQuery(DbTable table, ImmutableIntKeyMap<DbValue> constraints, ImmutableIntKeyMap<DbValue> values) {
        _table = table;
        _constraints = constraints;
        _values = values;
    }

    public DbTable table() {
        return _table;
    }

    public ImmutableIntKeyMap<DbValue> constraints() {
        return _constraints;
    }

    public ImmutableIntKeyMap<DbValue> values() {
        return _values;
    }

    public static final class Builder implements DbIdentifiableQueryBuilder, DbSettableQueryBuilder {
        private final DbTable _table;
        private final MutableIntPairMap _constraints = MutableIntPairMap.empty();
        private final MutableIntKeyMap<DbValue> _values = MutableIntKeyMap.empty();

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

        private void assertValidTextColumn(int columnIndex) {
            if (columnIndex < 0 || columnIndex >= _table.columns().size()) {
                throw new IllegalArgumentException("Wrong column index");
            }

            final DbColumn column = _table.columns().get(columnIndex);
            if (!column.isText()) {
                throw new IllegalArgumentException("Column is not a text value");
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

        @Override
        public Builder put(int columnIndex, int value) {
            assertValidIntColumn(columnIndex);
            if (_values.keySet().contains(columnIndex)) {
                throw new IllegalArgumentException("Column already has value");
            }

            _values.put(columnIndex, new DbIntValue(value));
            return this;
        }

        public Builder put(int columnIndex, String value) {
            assertValidTextColumn(columnIndex);
            if (_values.keySet().contains(columnIndex)) {
                throw new IllegalArgumentException("Column already has value");
            }

            _values.put(columnIndex, new DbStringValue(value));
            return this;
        }

        public DbUpdateQuery build() {
            if (_values.isEmpty()) {
                throw new AssertionError("No values set");
            }

            return new DbUpdateQuery(_table,
                    _constraints.toImmutable().map(DbIntValue::new),
                    _values.toImmutable());
        }
    }
}
