package sword.database;

import static sword.collections.SortUtils.equal;

public final class DbStringValue implements DbValue {
    private final String _value;

    public DbStringValue(String value) {
        _value = value;
    }

    public String get() {
        return _value;
    }

    @Override
    public boolean isText() {
        return true;
    }

    @Override
    public int toInt() throws UnsupportedOperationException {
        throw new UnsupportedOperationException("String column should not be converted to integer");
    }

    @Override
    public String toText() {
        return _value;
    }

    @Override
    public int hashCode() {
        return (_value != null)? _value.hashCode() : 0;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof DbStringValue)) {
            return false;
        }

        final DbStringValue that = (DbStringValue) other;
        return equal(_value, that._value);
    }
}
