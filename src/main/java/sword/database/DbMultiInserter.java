package sword.database;

public interface DbMultiInserter extends DbInserter {
    default void insert(DbMultiInsertQuery query) {
        for (DbInsertQuery singleQuery : query.queryList()) {
            insert(singleQuery);
        }
    }
}
