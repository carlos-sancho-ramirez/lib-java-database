package sword.database;

public interface DbValue {
    boolean isText();

    int toInt() throws UnsupportedOperationException;
    String toText();
}
