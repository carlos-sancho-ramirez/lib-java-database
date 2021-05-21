package sword.database;

import sword.collections.ImmutableList;

/**
 * List of tables and indexes that composes a databases.
 * <p>
 * This class is intentionally immutable as the structure of the database is
 * expected to be the same, not its table contents though.
 */
public interface DbSchema {
    ImmutableList<DbTable> tables();
    ImmutableList<DbIndex> indexes();
}
