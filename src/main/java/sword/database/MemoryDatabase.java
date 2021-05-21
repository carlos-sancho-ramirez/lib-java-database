package sword.database;

import java.util.Iterator;

import sword.collections.AbstractTransformer;
import sword.collections.ImmutableIntKeyMap;
import sword.collections.ImmutableIntList;
import sword.collections.ImmutableIntRange;
import sword.collections.ImmutableIntSet;
import sword.collections.ImmutableList;
import sword.collections.IntKeyMap;
import sword.collections.List;
import sword.collections.MutableHashMap;
import sword.collections.MutableIntKeyMap;
import sword.collections.MutableIntValueHashMap;
import sword.collections.MutableList;
import sword.collections.MutableMap;

/**
 * Implementation for an in-memory non-permanent database.
 * <p>
 * This class can be used for data that is valid while the process is still
 * alive, as a cache, or just for testing purposes.
 */
public final class MemoryDatabase implements Database {

    private final MutableHashMap<DbTable, MutableIntKeyMap<ImmutableList<Object>>> _tableMap = MutableHashMap.empty();
    private final MutableHashMap<DbColumn, MutableIntValueHashMap<Object>> _indexes = MutableHashMap.empty();

    private static final class Result extends AbstractTransformer<List<DbValue>> implements DbResult {
        private final ImmutableList<ImmutableList<Object>> _content;
        private int _index;

        Result(ImmutableList<ImmutableList<Object>> content) {
            _content = content;
        }

        @Override
        public void close() {
            _index = _content.size();
        }

        @Override
        public int getRemainingRows() {
            return _content.size() - _index;
        }

        @Override
        public boolean hasNext() {
            return _index < _content.size();
        }

        private static DbValue rawToDbValue(Object raw) {
            return (raw instanceof Integer)? new DbIntValue((Integer) raw) : new DbStringValue((String) raw);
        }

        @Override
        public ImmutableList<DbValue> next() {
            return _content.get(_index++).map(Result::rawToDbValue);
        }
    }

    private void applyJoins(MutableList<ImmutableList<Object>> result, DbQuery query) {
        final int tableCount = query.getTableCount();
        for (int viewIndex = 1; viewIndex < tableCount; viewIndex++) {
            final DbView view = query.getView(viewIndex);
            final DbTable viewAsTable = view.asTable();
            final DbQuery viewAsQuery = view.asQuery();
            final DbQuery.JoinColumnPair joinPair = query.getJoinPair(viewIndex - 1);
            if (viewAsQuery != null) {
                final MutableList<ImmutableList<Object>> innerQueryResult = innerSelect(viewAsQuery);
                for (int row = 0; row < result.size(); row++) {
                    final ImmutableList<Object> oldRow = result.get(row);
                    final Object rawValue = oldRow.get(joinPair.left());
                    final int targetJoinColumnIndex = joinPair.right() - oldRow.size();

                    boolean somethingReplaced = false;
                    for (ImmutableList<Object> foundRow : innerQueryResult) {
                        if (equal(foundRow.valueAt(targetJoinColumnIndex), rawValue)) {
                            final ImmutableList<Object> newRow = oldRow.appendAll(foundRow);
                            if (!somethingReplaced) {
                                result.put(row, newRow);
                                somethingReplaced = true;
                            }
                            else {
                                result.insert(++row, newRow);
                            }
                        }
                    }

                    if (!somethingReplaced) {
                        result.removeAt(row--);
                    }
                }
            }
            else {
                final IntKeyMap<ImmutableList<Object>> viewContent = _tableMap.get(viewAsTable, MutableIntKeyMap.empty());

                for (int row = 0; row < result.size(); row++) {
                    final ImmutableList<Object> oldRow = result.get(row);
                    final Object rawValue = oldRow.get(joinPair.left());
                    final int targetJoinColumnIndex = joinPair.right() - oldRow.size();

                    if (targetJoinColumnIndex == 0) {
                        final int id = (Integer) rawValue;
                        final ImmutableList<Object> foundRow = viewContent.get(id, null);
                        if (foundRow != null) {
                            ImmutableList<Object> newRow = oldRow.append(id).appendAll(foundRow);
                            result.put(row, newRow);
                        }
                        else {
                            result.removeAt(row--);
                        }
                    }
                    else {
                        boolean somethingReplaced = false;
                        for (MutableIntKeyMap.Entry<ImmutableList<Object>> entry : viewContent.entries()) {
                            if (equal(entry.value().get(targetJoinColumnIndex - 1), rawValue)) {
                                final ImmutableList<Object> newRow = oldRow.append(entry.key())
                                        .appendAll(entry.value());
                                if (!somethingReplaced) {
                                    result.put(row, newRow);
                                    somethingReplaced = true;
                                }
                                else {
                                    result.insert(++row, newRow);
                                }
                            }
                        }

                        if (!somethingReplaced) {
                            result.removeAt(row--);
                        }
                    }
                }
            }
        }
    }

    private void applyColumnMatchRestrictions(
            MutableList<ImmutableList<Object>> result, Iterable<DbQuery.JoinColumnPair> pairs) {
        for (DbQuery.JoinColumnPair pair : pairs) {
            final Iterator<ImmutableList<Object>> it = result.iterator();
            while (it.hasNext()) {
                final ImmutableList<Object> row = it.next();
                final boolean matchValue = equal(row.get(pair.left()), row.get(pair.right()));
                if (pair.mustMatch() && !matchValue || !pair.mustMatch() && matchValue) {
                    it.remove();
                }
            }
        }
    }

    private void applyRestrictions(
            MutableList<ImmutableList<Object>> result,
            ImmutableIntKeyMap<DbQuery.Restriction> restrictions) {
        for (ImmutableIntKeyMap.Entry<DbQuery.Restriction> entry : restrictions.entries()) {
            final DbValue value = entry.value().value;
            if (!value.isText() || entry.value().type == DbQuery.RestrictionTypes.EXACT) {
                final Object rawValue = value.isText()? value.toText() : value.toInt();

                final Iterator<ImmutableList<Object>> it = result.iterator();
                while (it.hasNext()) {
                    final ImmutableList<Object> register = it.next();
                    if (!rawValue.equals(register.get(entry.key()))) {
                        it.remove();
                    }
                }
            }
            else {
                final int type = entry.value().type;
                final Predicate2<String, String> cmpFunc =
                        (type == DbQuery.RestrictionStringTypes.ENDS_WITH)? String::endsWith :
                        (type == DbQuery.RestrictionStringTypes.STARTS_WITH)? String::startsWith :
                        String::contains;

                final Iterator<ImmutableList<Object>> it = result.iterator();
                while (it.hasNext()) {
                    final ImmutableList<Object> register = it.next();
                    if (!cmpFunc.apply(register.get(entry.key()).toString(), value.toText())) {
                        it.remove();
                    }
                }
            }
        }
    }

    private ImmutableList<Object> getGroup(ImmutableList<Object> reg, ImmutableIntSet grouping) {
        final ImmutableList.Builder<Object> groupBuilder = new ImmutableList.Builder<>();
        for (int columnIndex : grouping) {
            groupBuilder.add(reg.get(columnIndex));
        }
        return groupBuilder.build();
    }

    private MutableList<ImmutableList<Object>> innerSelect(DbQuery query) {
        for (DbQuery.Ordered ordered : query.ordering()) {
            if (query.getJoinColumn(ordered.columnIndex).isText()) {
                throw new UnsupportedOperationException("Unimplemented");
            }
        }

        final DbView view = query.getView(0);
        final DbQuery innerQuery = view.asQuery();
        final DbTable viewAsTable = view.asTable();
        final ImmutableIntKeyMap<DbQuery.Restriction> restrictions = query.restrictions();
        MutableList<ImmutableList<Object>> unselectedResult;
        if (innerQuery != null) {
            unselectedResult = innerSelect(innerQuery);
        }
        else {
            final MutableIntKeyMap<ImmutableList<Object>> content;
            if (_tableMap.containsKey(viewAsTable)) {
                content = _tableMap.get(viewAsTable);
            }
            else {
                content = MutableIntKeyMap.empty();
                _tableMap.put(viewAsTable, content);
            }

            // Apply id restriction if found
            final MutableList.Builder<ImmutableList<Object>> unselectedResultBuilder = new MutableList.Builder<>();
            if (restrictions.keySet().contains(0)) {
                final int id = restrictions.get(0).value.toInt();
                ImmutableList<Object> rawRegister = content.get(id, null);
                if (rawRegister != null) {
                    unselectedResultBuilder.add(rawRegister.prepend(id));
                }
            }
            else {
                for (MutableIntKeyMap.Entry<ImmutableList<Object>> entry : content.entries()) {
                    final ImmutableList<Object> register = entry.value().prepend(entry.key());
                    unselectedResultBuilder.add(register);
                }
            }
            unselectedResult = unselectedResultBuilder.build();
        }

        applyJoins(unselectedResult, query);
        applyColumnMatchRestrictions(unselectedResult, query.columnValueMatchPairs());
        applyRestrictions(unselectedResult, restrictions);

        final ImmutableList<DbQuery.Ordered> ordering = query.ordering();
        unselectedResult.sort((a, b) -> {
            for (DbQuery.Ordered ordered : ordering) {
                // Assumed that they are numeric fields. Other types are not supported
                final int aValue = (Integer) a.get(ordered.columnIndex);
                final int bValue = (Integer) b.get(ordered.columnIndex);

                if (!ordered.descendantOrder && aValue < bValue || ordered.descendantOrder && bValue < aValue) {
                    return true;
                }

                if (!ordered.descendantOrder && aValue > bValue || ordered.descendantOrder && bValue > aValue) {
                    return false;
                }
            }

            return false;
        });

        // Apply column selection
        final int selectionCount = query.selection().size();
        boolean groupedSelection = query.getGroupingCount() != 0;
        if (!groupedSelection) {
            for (int i = 0; i < selectionCount; i++) {
                if (query.isMaxAggregateFunctionSelection(i) || query.isConcatAggregateFunctionSelection(i)) {
                    groupedSelection = true;
                    break;
                }
            }
        }

        final MutableList<ImmutableList<Object>> unlimitedResult;
        if (groupedSelection) {
            final MutableHashMap<ImmutableList<Object>, Integer> groups = MutableHashMap.empty();
            for (int resultRow = 0; resultRow < unselectedResult.size(); resultRow++) {
                final ImmutableList<Object> reg = unselectedResult.get(resultRow);
                final ImmutableList<Object> group = getGroup(reg, query.grouping());
                if (!groups.containsKey(group)) {
                    groups.put(group, resultRow);
                    final ImmutableList.Builder<Object> rowBuilder = new ImmutableList.Builder<>();
                    for (int selectedColumn : query.selection()) {
                        rowBuilder.add(reg.get(selectedColumn));
                    }
                    unselectedResult.put(resultRow, rowBuilder.build());
                }
                else {
                    final int oldRowIndex = groups.get(group);
                    final ImmutableList<Object> oldRow = unselectedResult.get(oldRowIndex);
                    final ImmutableList.Builder<Object> rowBuilder = new ImmutableList.Builder<>();
                    for (int selectionIndex = 0; selectionIndex < selectionCount; selectionIndex++) {
                        Object rawValue = reg.get(query.selection().valueAt(selectionIndex));
                        if (query.isMaxAggregateFunctionSelection(selectionIndex)) {
                            int oldMax = (Integer) oldRow.get(selectionIndex);
                            int value = (Integer) rawValue;
                            rowBuilder.add(value > oldMax? value : oldMax);
                        }
                        else if (query.isConcatAggregateFunctionSelection(selectionIndex)) {
                            String oldText = (String) oldRow.get(selectionIndex);
                            String value = (String) rawValue;
                            rowBuilder.add(oldText + value);
                        }
                        else {
                            rowBuilder.add(rawValue);
                        }
                    }

                    unselectedResult.put(oldRowIndex, rowBuilder.build());
                    unselectedResult.removeAt(resultRow);
                    --resultRow;
                }
            }
            unlimitedResult = unselectedResult;
        }
        else {
            final MutableList<ImmutableList<Object>> builder = MutableList.empty();
            final ImmutableIntList selection = query.selection();
            for (ImmutableList<Object> register : unselectedResult) {
                final ImmutableList.Builder<Object> regBuilder = new ImmutableList.Builder<>();
                for (int columnIndex : selection) {
                    regBuilder.add(register.get(columnIndex));
                }
                builder.append(regBuilder.build());
            }
            unlimitedResult = builder;
        }

        final ImmutableIntRange range = query.range();
        final int unlimitedSize = unlimitedResult.size();
        final boolean shorterRange = range.max() < unlimitedSize - 1;
        if (range.min() > 0 || shorterRange) {
            final int newSize = shorterRange? range.size() : unlimitedSize - range.min();
            final MutableList<ImmutableList<Object>> limitedResult = MutableList.empty((currentSize, desiredSize) -> newSize);
            for (int index = range.min(); index <= range.max() && index < unlimitedSize; index++) {
                limitedResult.append(unlimitedResult.valueAt(index));
            }

            return limitedResult;
        }
        else {
            return unlimitedResult;
        }
    }

    @Override
    public DbResult select(DbQuery query) {
        return new Result(innerSelect(query).toImmutable());
    }

    private MutableIntKeyMap<ImmutableList<Object>> obtainTableContent(DbTable table) {
        MutableIntKeyMap<ImmutableList<Object>> content = _tableMap.get(table, null);

        if (content == null) {
            content = MutableIntKeyMap.empty();
            _tableMap.put(table, content);
        }

        return content;
    }

    @Override
    public Integer insert(DbInsertQuery query) {
        final DbTable table = query.getTable();
        final MutableIntKeyMap<ImmutableList<Object>> content = obtainTableContent(table);

        final int queryColumnCount = query.getColumnCount();
        final ImmutableList<DbColumn> columns = table.columns();

        final int columnsSize = columns.size();
        final ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>((currentLength, newSize) -> columnsSize);
        final MutableHashMap<DbColumn, Object> uniqueMap = MutableHashMap.empty();
        Integer id = null;
        for (DbColumn column : columns) {
            boolean found = false;
            for (int i = 0; i < queryColumnCount; i++) {
                if (column == query.getColumn(i)) {
                    final DbValue value = query.getValue(i);
                    if (column.isPrimaryKey()) {
                        if (id != null) {
                            throw new AssertionError();
                        }
                        id = value.toInt();
                        if (content.keySet().contains(id)) {
                            // Let's avoid duplicates
                            return null;
                        }
                        found = true;
                    }
                    else {
                        final Object rawValue = value.isText()? value.toText() : value.toInt();
                        if (column.isUnique()) {
                            if (_indexes.containsKey(column) && _indexes.get(column).keySet().contains(rawValue)) {
                                // Let's avoid duplicates
                                return null;
                            }
                            uniqueMap.put(column, rawValue);
                        }

                        builder.add(rawValue);
                        found = true;
                        break;
                    }
                }
            }

            if (!found && column.isPrimaryKey()) {
                if (id != null) {
                    throw new AssertionError();
                }

                id = content.isEmpty()? 1 : content.keySet().max() + 1;
                found = true;
            }

            if (!found) {
                throw new IllegalArgumentException("Unable to find value for column " + column.name());
            }
        }
        content.put(id, builder.build());

        for (MutableMap.Entry<DbColumn, Object> entry : uniqueMap.entries()) {
            final MutableIntValueHashMap<Object> map;
            if (_indexes.containsKey(entry.key())) {
                map = _indexes.get(entry.key());
            }
            else {
                map = MutableIntValueHashMap.empty();
                _indexes.put(entry.key(), map);
            }

            map.put(entry.value(), id);
        }

        return id;
    }

    @Override
    public boolean update(DbUpdateQuery query) {
        final DbTable table = query.table();
        final MutableIntKeyMap<ImmutableList<Object>> content = obtainTableContent(table);

        final ImmutableIntKeyMap.Builder<Object> rawConstraintsBuilder = new ImmutableIntKeyMap.Builder<>();
        for (IntKeyMap.Entry<DbValue> entry : query.constraints().entries()) {
            final DbValue value = entry.value();
            rawConstraintsBuilder.put(entry.key(), value.isText()? value.toText() : value.toInt());
        }
        final ImmutableIntKeyMap<Object> rawConstraints = rawConstraintsBuilder.build();
        final int constraintsCount = rawConstraints.size();

        final ImmutableIntKeyMap.Builder<Object> rawValuesBuilder = new ImmutableIntKeyMap.Builder<>();
        for (IntKeyMap.Entry<DbValue> entry : query.values().entries()) {
            final DbValue value = entry.value();
            rawValuesBuilder.put(entry.key(), value.isText()? value.toText() : value.toInt());
        }
        final ImmutableIntKeyMap<Object> rawValues = rawValuesBuilder.build();
        final int valuesCount = rawValues.size();

        final int tableLength = content.size();
        for (int row = 0; row < tableLength; row++) {
            final ImmutableList<Object> currentValues = content.valueAt(row);
            boolean allMatches = true;
            for (int index = 0; index < constraintsCount && allMatches; index++) {
                final int column = rawConstraints.keyAt(index);
                final Object columnValue = (column == 0)? content.keyAt(row) : currentValues.get(column - 1);
                allMatches = rawConstraints.valueAt(index).equals(columnValue);
            }

            if (allMatches) {
                final MutableList<Object> values = currentValues.mutate();
                boolean modifyPrimeryKey = false;
                for (int index = 0; index < valuesCount; index++) {
                    final int column = rawValues.keyAt(index) - 1;
                    if (column >= 0) {
                        values.put(column, rawValues.valueAt(index));
                    }
                    else {
                        modifyPrimeryKey = true;
                    }
                }

                if (modifyPrimeryKey) {
                    final Object newKey = rawValues.get(0);
                    if (!(newKey instanceof Integer) || content.keySet().contains((Integer) newKey)) {
                        // Conflict. So nothing can be done
                        return false;
                    }

                    content.removeAt(row);
                    content.put((Integer) newKey, values.toImmutable());
                    return true;
                }
                else {
                    content.put(content.keyAt(row), values.toImmutable());
                }
            }
        }

        return true;
    }

    @Override
    public boolean delete(DbDeleteQuery query) {
        final ImmutableIntKeyMap<DbValue> constraints = query.constraints();
        final int constraintCount = constraints.size();
        if (constraintCount == 0) {
            return false;
        }

        final MutableIntKeyMap<ImmutableList<Object>> table = _tableMap.get(query.table(), null);
        if (table != null) {
            if (constraints.keyAt(0) == 0) {
                final int id = constraints.valueAt(0).toInt();
                final ImmutableList<Object> register = table.get(id, null);
                if (register != null) {
                    boolean matches = true;
                    for (int i = 1; i < constraintCount; i++) {
                        final DbValue value = constraints.valueAt(i);
                        final Object rawValue = value.isText()? value.toText() : value.toInt();
                        if (!equal(register.get(constraints.keyAt(i) - 1), rawValue)) {
                            matches = false;
                            break;
                        }
                    }

                    if (matches) {
                        if (!table.remove(id)) {
                            throw new AssertionError();
                        }

                        return true;
                    }
                }
            }
            else {
                boolean removed = false;
                int index = 0;
                while (index < table.size()) {
                    final ImmutableList<Object> register = table.valueAt(index);
                    boolean matches = true;
                    for (IntKeyMap.Entry<DbValue> entry : constraints.entries()) {
                        final DbValue value = entry.value();
                        final Object rawValue = value.isText() ? value.toText() : value.toInt();
                        if (!equal(register.get(entry.key() - 1), rawValue)) {
                            matches = false;
                            break;
                        }
                    }

                    if (matches) {
                        table.removeAt(index);
                        removed = true;
                    }
                    else {
                        index++;
                    }
                }

                return removed;
            }
        }

        return false;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MemoryDatabase)) {
            return false;
        }

        final MemoryDatabase that = (MemoryDatabase) other;
        return _tableMap.toImmutable().filterNot(MutableIntKeyMap::isEmpty)
                .equals(that._tableMap.toImmutable().filterNot(MutableIntKeyMap::isEmpty));
    }

    @Override
    public int hashCode() {
        return _tableMap.hashCode();
    }

    private static boolean equal(Object a, Object b) {
        return a == b || a != null && a.equals(b);
    }
}
