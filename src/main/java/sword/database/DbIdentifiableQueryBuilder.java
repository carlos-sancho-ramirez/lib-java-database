package sword.database;

public interface DbIdentifiableQueryBuilder {
    DbIdentifiableQueryBuilder where(int columnIndex, int value);
}
