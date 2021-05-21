package sword.database;

public final class DbTextColumn extends DbColumn {

    public DbTextColumn(String name) {
        super(name);
    }

    @Override
    public boolean isText() {
        return true;
    }
}
