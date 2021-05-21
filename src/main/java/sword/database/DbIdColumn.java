package sword.database;

public final class DbIdColumn extends DbColumn {

    private static final String idColumnName = "id";

    DbIdColumn() {
        super(idColumnName);
    }

    @Override
    public boolean isText() {
        return false;
    }

    @Override
    public boolean isPrimaryKey() {
        return true;
    }

    @Override
    public boolean isUnique() {
        return true;
    }
}
