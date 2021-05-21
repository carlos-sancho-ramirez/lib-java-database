package sword.database;

interface Predicate2<A, B> {
    boolean apply(A a, B b);
}
