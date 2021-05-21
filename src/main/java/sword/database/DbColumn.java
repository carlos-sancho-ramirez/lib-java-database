package sword.database;

/**
 * This class specifies all info related to a single column within a {@link DbView}
 */
public abstract class DbColumn {

    private final String _name;

    DbColumn(String name) {
        _name = name;
    }

    public final String name() {
        return _name;
    }

    /**
     * Whether the field content may be understood as a char sequence.
     * Right now there is only int and text fields, then so far it is secure
     * to assume that all non-text field are actually int fields.
     */
    public abstract boolean isText();

    /**
     * Whether this column represents a primary key for this table,
     * which also means that it is not possible to have two registers within the table with the same value in this column.
     *
     * If this method return true, {@link #isUnique()} must return true as well.
     *
     * @return Whether this column represents a primary key for this table or not.
     */
    public boolean isPrimaryKey() {
        return false;
    }

    /**
     * Whether this column is not possible to have two registers within the table with the same value in this column.
     *
     * If this method return false, {@link #isPrimaryKey()} must return false as well.
     * @return Whether the value of this column can be repeated in other registers of this table or not.
     */
    public boolean isUnique() {
        return false;
    }
}
