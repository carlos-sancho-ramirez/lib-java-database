package sword.database;

public interface DbExporter {

    void save(Database db) throws UnableToExportException;

    interface Database {
        DbResult select(DbQuery query);
    }

    class UnableToExportException extends Exception {
    }
}
