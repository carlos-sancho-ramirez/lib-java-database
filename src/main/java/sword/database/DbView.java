package sword.database;

import sword.collections.ImmutableList;

/**
 * Definition for a structure of data that holds an arbitrary number of
 * rows (also called registers) and columns.
 * <p>
 * This class do not include any register, but just the definition of its
 * structure, which is repeated on each row.
 * <p>
 * This class is intentionally immutable, which means that once its created
 * its contents remain the same.
 * <p>
 * The number and order of columns is determined by the number of columns
 * provided in construction time. And columns within this table are typed
 * according to the {@link DbColumn} provided in construction time.
 * That means that all rows have the same number of columns and the type
 * for each column matches on each row.
 */
public interface DbView {
    ImmutableList<DbColumn> columns();

    /**
     * Return this instance typed as DbTable, or null if it's not a table
     */
    DbTable asTable();

    /**
     * Return this instance typed as DbQuery, or null if it's not a query
     */
    DbQuery asQuery();
}
