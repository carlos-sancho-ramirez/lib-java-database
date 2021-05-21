package sword.database;

import sword.collections.ImmutableList;

public class DbTable implements DbView {
    private final String _name;
    private final ImmutableList<DbColumn> _columns;

    public DbTable(String name, DbColumn... columns) {
        final ImmutableList.Builder<DbColumn> builder = new ImmutableList.Builder<>();
        builder.add(new DbIdColumn());
        for (DbColumn column : columns) {
            builder.add(column);
        }

        _name = name;
        _columns = builder.build();
    }

    public final String name() {
        return _name;
    }

    @Override
    public final ImmutableList<DbColumn> columns() {
        return _columns;
    }

    public final int getIdColumnIndex() {
        return 0;
    }

    @Override
    public final DbTable asTable() {
        return this;
    }

    @Override
    public final DbQuery asQuery() {
        return null;
    }
}
