package sword.database;

/**
 * Pointer to an specific column within an specific {@link DbTable} that is
 * expected to be accessed frequently and need an extra indexation to look
 * values faster.
 */
public final class DbIndex {
    public final DbTable table;
    public final int column;

    public DbIndex(DbTable table, int column) {
        if (table == null || column < 0 || column >= table.columns().size()) {
            throw new IllegalArgumentException();
        }

        this.table = table;
        this.column = column;
    }

    @Override
    public int hashCode() {
        return table.hashCode() * 13 + column;
    }

    @Override
    public boolean equals(Object other) {
        if (other == null || !(other instanceof DbIndex)) {
            return false;
        }

        final DbIndex that = (DbIndex) other;
        return column == that.column && table.equals(that.table);
    }
}
