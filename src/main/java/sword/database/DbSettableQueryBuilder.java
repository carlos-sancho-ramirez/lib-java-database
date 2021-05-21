package sword.database;

public interface DbSettableQueryBuilder {
    DbSettableQueryBuilder put(int columnIndex, int value);
}
