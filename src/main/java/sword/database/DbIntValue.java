package sword.database;

public final class DbIntValue implements DbValue {
    private final int _value;

    public DbIntValue(int value) {
        _value = value;
    }

    public int get() {
        return _value;
    }

    @Override
    public boolean isText() {
        return false;
    }

    @Override
    public int toInt() {
        return get();
    }

    @Override
    public String toText() {
        return Integer.toString(_value);
    }

    @Override
    public int hashCode() {
        return _value;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof DbIntValue)) {
            return false;
        }

        return _value == ((DbIntValue) other)._value;
    }
}
