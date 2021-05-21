package sword.database;

import java.util.ArrayList;

import sword.collections.ImmutableHashSet;
import sword.collections.ImmutableIntKeyMap;
import sword.collections.ImmutableIntList;
import sword.collections.ImmutableIntRange;
import sword.collections.ImmutableIntSet;
import sword.collections.ImmutableIntSetCreator;
import sword.collections.ImmutableList;
import sword.collections.ImmutableSet;
import sword.collections.MutableHashSet;
import sword.collections.MutableIntKeyMap;
import sword.collections.MutableSet;

public final class DbQuery implements DbView {

    private static final int FLAG_COLUMN_FUNCTION_MAX = 0x10000;
    private static final int FLAG_COLUMN_FUNCTION_CONCAT = 0x20000;
    private static final ImmutableIntRange ALL_ROWS_RANGE = new ImmutableIntRange(0, Integer.MAX_VALUE);

    private final DbView[] _tables;
    private final int[] _joinPairs;
    private final ImmutableSet<JoinColumnPair> _columnValueMatchingPairs;
    private final ImmutableIntKeyMap<Restriction> _restrictions;
    private final int[] _groupBy;
    private final Ordered[] _orderBy;
    private final ImmutableIntRange _range;

    // column indexes for the selection in the given order
    private final ImmutableIntList _selection;

    // Function that should be applied on each selection. This list should match the selection length
    private final ImmutableIntList _selectionFunctions;

    private final transient DbColumn[] _joinColumns;
    private transient ImmutableList<DbColumn> _columns;

    private DbQuery(DbView[] tables, int[] joinPairs, ImmutableSet<JoinColumnPair> columnValueMatchPairs,
            ImmutableIntKeyMap<Restriction> restrictions, int[] groupBy, Ordered[] orderBy, ImmutableIntRange range, int[] selection) {

        if (tables == null || tables.length == 0 || selection == null || selection.length == 0 || range == null || range.min() < 0) {
            throw new IllegalArgumentException();
        }

        int joinColumnCount = tables[0].columns().size();
        for (int i = 1; i < tables.length; i++) {
            joinColumnCount += tables[i].columns().size();
        }

        final DbColumn[] joinColumns = new DbColumn[joinColumnCount];
        int index = 0;
        for (int i = 0; i < tables.length; i++) {
            for (DbColumn column : tables[i].columns()) {
                joinColumns[index++] = column;
            }
        }

        if (joinPairs == null) {
            joinPairs = new int[0];
        }

        if ((joinPairs.length & 1) == 1 || !hasValidValues(joinPairs, 0, joinColumnCount - 1)) {
            throw new IllegalArgumentException("Invalid column join pairs");
        }

        if (columnValueMatchPairs == null) {
            columnValueMatchPairs = ImmutableHashSet.empty();
        }

        for (JoinColumnPair pair : columnValueMatchPairs) {
            if (pair.right() >= joinColumnCount) {
                throw new IllegalArgumentException("Invalid column index");
            }
        }

        final ImmutableIntList.Builder selectionBuilder = new ImmutableIntList.Builder();
        final ImmutableIntList.Builder selectionFuncBuilder = new ImmutableIntList.Builder();
        for (int i = 0; i < selection.length; i++) {
            final int column = (FLAG_COLUMN_FUNCTION_MAX - 1) & selection[i];
            if (column < 0 || column >= joinColumnCount) {
                throw new IllegalArgumentException("Selected column indexes out of bounds");
            }
            selectionBuilder.add(column);

            final int func = selection[i] & ~(FLAG_COLUMN_FUNCTION_MAX - 1);
            if (func != 0 && func != FLAG_COLUMN_FUNCTION_MAX && func != FLAG_COLUMN_FUNCTION_CONCAT) {
                throw new IllegalArgumentException("Unexpected aggregate function");
            }
            selectionFuncBuilder.add(func);
        }

        if (restrictions == null) {
            restrictions = ImmutableIntKeyMap.empty();
        }

        final int restrictionCount = restrictions.size();
        final ImmutableIntSet restrictedColumns = restrictions.keySet();
        if (restrictionCount > 0 && (restrictedColumns.min() < 0 || restrictedColumns.max() >= joinColumnCount)) {
            throw new IllegalArgumentException("Restricted column indexes out of bounds");
        }

        for (int i = 0; i < restrictionCount; i++) {
            final DbValue value = restrictions.valueAt(i).value;
            if (value == null || joinColumns[restrictions.keyAt(i)].isText() != value.isText()) {
                throw new IllegalArgumentException();
            }
        }

        if (groupBy == null) {
            groupBy = new int[0];
        }

        if (!hasValidSetValues(groupBy, 0, joinColumnCount - 1)) {
            throw new IllegalArgumentException("Invalid grouping parameters");
        }

        if (orderBy == null) {
            orderBy = new Ordered[0];
        }

        final int[] orderedColumnIndexes = new int[orderBy.length];
        for (int i = 0; i < orderBy.length; i++) {
            orderedColumnIndexes[i] = orderBy[i].columnIndex;
        }

        if (!hasValidSetValues(orderedColumnIndexes, 0, joinColumnCount - 1)) {
            throw new IllegalArgumentException("Invalid ordering parameters");
        }

        _tables = tables;
        _joinPairs = joinPairs;
        _columnValueMatchingPairs = columnValueMatchPairs;
        _restrictions = restrictions;
        _groupBy = groupBy;
        _orderBy = orderBy;
        _range = range;
        _selection = selectionBuilder.build();
        _selectionFunctions = selectionFuncBuilder.build();

        _joinColumns = joinColumns;
    }

    public int getTableCount() {
        return _tables.length;
    }

    public DbView getView(int index) {
        return _tables[index];
    }

    public int getTableIndexFromColumnIndex(int column) {
        int count = 0;
        int tableIndex = 0;
        while (column >= count) {
            count += _tables[tableIndex++].columns().size();
        }
        return tableIndex - 1;
    }

    public DbColumn getJoinColumn(int index) {
        return _joinColumns[index];
    }

    public ImmutableIntList selection() {
        return _selection;
    }

    /**
     * Return the range of rows that should be returned when applying this query.
     * <p>
     * This functionality allows limiting the number of rows to be presented to
     * the user or even adding a pagination mechanism.
     * <p>
     * This range will never be empty or null, and minimum will never be a negative value.
     * The default value is the range from 0 to the maximum value of an integer.
     *
     * @return The range of rows that should be displayed.
     */
    public ImmutableIntRange range() {
        return _range;
    }

    @Override
    public ImmutableList<DbColumn> columns() {
        if (_columns == null) {
            final ImmutableList.Builder<DbColumn> builder = new ImmutableList.Builder<>();
            for (int selected : _selection) {
                builder.add(_joinColumns[selected]);
            }

            _columns = builder.build();
        }

        return _columns;
    }

    @Override
    public DbTable asTable() {
        return null;
    }

    @Override
    public DbQuery asQuery() {
        return this;
    }

    public static final class JoinColumnPair {

        private final int _left;
        private final int _right;

        /**
         * If set, it is understood that both columns must have the same value in order to match.
         * If clear, it is understood that values in both column must differ.
         */
        private final boolean _mustMatch;

        private JoinColumnPair(int columnA, int columnB, boolean mustMatch) {
            if (columnA > columnB) {
                int temp = columnA;
                columnA = columnB;
                columnB = temp;
            }

            if (columnA < 0 || columnA == columnB) {
                throw new IllegalArgumentException();
            }

            _left = columnA;
            _right = columnB;
            _mustMatch = mustMatch;
        }

        public int left() {
            return _left;
        }

        public int right() {
            return _right;
        }

        public boolean mustMatch() {
            return _mustMatch;
        }
    }

    public interface RestrictionTypes {
        int EXACT = 0;
    }

    public interface RestrictionStringTypes extends RestrictionTypes {
        int ENDS_WITH = 1;
        int STARTS_WITH = 2;
        int CONTAINS = 3;
    }

    public static final class Restriction {
        public final DbValue value;

        /**
         * If DbValue is text, this must be one of the values within {@link DbQuery.RestrictionStringTypes}
         * If DbValue is int, this must be 0
         */
        public final int type;

        public Restriction(DbValue value, int type) {
            if (value == null || value.isText() && (type < 0 || type > 3) || !value.isText() && type != RestrictionTypes.EXACT) {
                throw new IllegalArgumentException();
            }

            this.value = value;
            this.type = type;
        }
    }

    public static final class Ordered {
        public final int columnIndex;
        public final boolean descendantOrder;

        public Ordered(int columnIndex, boolean descendantOrder) {
            this.columnIndex = columnIndex;
            this.descendantOrder = descendantOrder;
        }
    }

    public JoinColumnPair getJoinPair(int index) {
        final int pairCount = _joinPairs.length / 2;
        for (int j = 0; j < pairCount; j++) {
            if (getTableIndexFromColumnIndex(_joinPairs[2 * j + 1]) == index + 1 && getTableIndexFromColumnIndex(_joinPairs[2 * j]) < index + 1) {
                return new JoinColumnPair(_joinPairs[2 * j], _joinPairs[2 * j + 1], true);
            }
        }

        throw new AssertionError("No pair found for table " + (index + 1));
    }

    public ImmutableSet<JoinColumnPair> columnValueMatchPairs() {
        return _columnValueMatchingPairs;
    }

    public int getRestrictionCount() {
        return _restrictions.size();
    }

    public int getRestrictedColumnIndex(int index) {
        return _restrictions.keyAt(index);
    }

    public DbValue getRestriction(int index) {
        return _restrictions.valueAt(index).value;
    }

    public ImmutableIntKeyMap<Restriction> restrictions() {
        return _restrictions;
    }

    public int getGroupingCount() {
        return _groupBy.length;
    }

    public int getGrouping(int index) {
        return _groupBy[index];
    }

    public ImmutableIntSet grouping() {
        final ImmutableIntSet.Builder builder = new ImmutableIntSetCreator();
        for (int columnIndex : _groupBy) {
            builder.add(columnIndex);
        }

        return builder.build();
    }

    public ImmutableList<Ordered> ordering() {
        final ImmutableList.Builder<Ordered> builder = new ImmutableList.Builder<>();
        for (Ordered ordered : _orderBy) {
            builder.add(ordered);
        }
        return builder.build();
    }

    private static boolean hasValidValues(int[] values, int min, int max) {
        final int length = (values != null)? values.length : 0;
        if (length < 1) {
            return true;
        }

        for (int i = 0; i < length; i++) {
            final int iValue = values[i];
            if (iValue < min || iValue > max) {
                return false;
            }
        }

        return true;
    }

    private static boolean hasValidSetValues(int[] values, int min, int max) {
        final int length = (values != null)? values.length : 0;
        if (length < 1) {
            return true;
        }
        else if (length == 1) {
            return values[0] >= min && values[0] <= max;
        }

        for (int i = 0; i < length - 1; i++) {
            final int iValue = values[i];
            if (iValue < min || iValue > max) {
                return false;
            }

            for (int j = i + 1; j < length; j++) {
                if (values[i] == values[j]) {
                    return false;
                }
            }
        }

        return values[length - 1] >= min && values[length - 1] <= max;
    }

    public boolean isMaxAggregateFunctionSelection(int selectionIndex) {
        return (_selectionFunctions.get(selectionIndex) & FLAG_COLUMN_FUNCTION_MAX) != 0;
    }

    public boolean isConcatAggregateFunctionSelection(int selectionIndex) {
        return (_selectionFunctions.get(selectionIndex) & FLAG_COLUMN_FUNCTION_CONCAT) != 0;
    }

    public static int max(int index) {
        return FLAG_COLUMN_FUNCTION_MAX | index;
    }

    public static int concat(int index) {
        return FLAG_COLUMN_FUNCTION_CONCAT | index;
    }

    public static final class Builder implements DbIdentifiableQueryBuilder {
        private final ArrayList<DbView> _tables = new ArrayList<>();
        private final ArrayList<Integer> _joinPairs = new ArrayList<>();
        private final MutableIntKeyMap<Restriction> _restrictions = MutableIntKeyMap.empty();
        private final MutableSet<JoinColumnPair> _columnValueMatchPairs = MutableHashSet.empty();
        private int[] _groupBy;
        private Ordered[] _orderBy;
        private int _joinColumnCount;
        private ImmutableIntRange _range = ALL_ROWS_RANGE;

        public Builder(DbView table) {
            _tables.add(table);
            _joinColumnCount = table.columns().size();
        }

        public Builder where(int columnIndex, DbValue value) {
            _restrictions.put(columnIndex, new Restriction(value, RestrictionTypes.EXACT));
            return this;
        }

        @Override
        public Builder where(int columnIndex, int value) {
            _restrictions.put(columnIndex, new Restriction(new DbIntValue(value), RestrictionTypes.EXACT));
            return this;
        }

        public Builder where(int columnIndex, Restriction restriction) {
            _restrictions.put(columnIndex, restriction);
            return this;
        }

        public Builder where(int columnIndex, String value) {
            _restrictions.put(columnIndex, new Restriction(new DbStringValue(value), RestrictionTypes.EXACT));
            return this;
        }

        public Builder whereColumnValueMatch(int columnIndexA, int columnIndexB) {
            final JoinColumnPair pair = new JoinColumnPair(columnIndexA, columnIndexB, true);
            if (pair.right() >= _joinColumnCount) {
                throw new IndexOutOfBoundsException();
            }
            _columnValueMatchPairs.add(pair);
            return this;
        }

        public Builder whereColumnValueDiffer(int columnIndexA, int columnIndexB) {
            final JoinColumnPair pair = new JoinColumnPair(columnIndexA, columnIndexB, false);
            if (pair.right() >= _joinColumnCount) {
                throw new IndexOutOfBoundsException();
            }
            _columnValueMatchPairs.add(pair);
            return this;
        }

        public Builder join(DbTable table, int left, int newTableColumnIndex) {
            final int tableColumnCount = table.columns().size();
            if (left < 0 || left >= _joinColumnCount || newTableColumnIndex < 0 || newTableColumnIndex >= tableColumnCount) {
                throw new IndexOutOfBoundsException();
            }

            _joinPairs.add(left);
            _joinPairs.add(_joinColumnCount + newTableColumnIndex);
            _tables.add(table);
            _joinColumnCount += tableColumnCount;

            return this;
        }

        public Builder groupBy(int... columnIndexes) {
            if (_groupBy != null) {
                throw new UnsupportedOperationException("groupBy can only be called once per query");
            }

            _groupBy = columnIndexes;
            return this;
        }

        public Builder orderBy(int... columnIndexes) {
            final int length = columnIndexes.length;
            final Ordered[] ordering = new Ordered[length];
            for (int i = 0; i < length; i++) {
                ordering[i] = new Ordered(columnIndexes[i], false);
            }

            return orderBy(ordering);
        }

        public Builder orderBy(Ordered... ordering) {
            if (_orderBy != null) {
                throw new UnsupportedOperationException("orderBy can only be called once per query");
            }

            _orderBy = ordering;
            return this;
        }

        public Builder range(ImmutableIntRange range) {
            if (range == null || range.min() < 0) {
                throw new IllegalArgumentException("amount in limit must be a positive value");
            }

            if (_range != ALL_ROWS_RANGE) {
                throw new UnsupportedOperationException("range can only be called once per query");
            }

            _range = range;
            return this;
        }

        public DbQuery select(int... selection) {
            final DbView[] views = new DbView[_tables.size()];
            final int[] joinPairs = new int[_joinPairs.size()];

            _tables.toArray(views);

            for (int i = 0; i < _joinPairs.size(); i++) {
                joinPairs[i] = _joinPairs.get(i);
            }

            return new DbQuery(views, joinPairs, _columnValueMatchPairs.toImmutable(),
                    _restrictions.toImmutable(), _groupBy, _orderBy, _range, selection);
        }
    }
}
