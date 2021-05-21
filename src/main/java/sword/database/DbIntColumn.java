package sword.database;

public final class DbIntColumn extends DbColumn {

    public DbIntColumn(String name) {
        super(name);
    }

    @Override
    public boolean isText() {
        return false;
    }
}
