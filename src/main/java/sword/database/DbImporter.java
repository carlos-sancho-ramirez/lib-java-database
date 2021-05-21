package sword.database;

public interface DbImporter {

    void init(Database db) throws UnableToImportException;

    interface Database extends DbExporter.Database, DbMultiInserter {
    }

    class UnableToImportException extends Exception {
    }
}
