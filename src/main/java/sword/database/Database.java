package sword.database;

public interface Database extends DbImporter.Database, Deleter {
    boolean update(DbUpdateQuery query);
}
