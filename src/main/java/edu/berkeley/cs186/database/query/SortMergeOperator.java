package edu.berkeley.cs186.database.query;

import java.util.*;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.table.Record;

class SortMergeOperator extends JoinOperator {
    SortMergeOperator(QueryOperator leftSource,
                      QueryOperator rightSource,
                      String leftColumnName,
                      String rightColumnName,
                      TransactionContext transaction) {
        super(leftSource, rightSource, leftColumnName, rightColumnName, transaction, JoinType.SORTMERGE);

        this.stats = this.estimateStats();
        this.cost = this.estimateIOCost();
    }

    @Override
    public Iterator<Record> iterator() {
        return new SortMergeIterator();
    }

    @Override
    public int estimateIOCost() {
        //does nothing
        return 0;
    }

    /**
     * An implementation of Iterator that provides an iterator interface for this operator.
     *
     * Before proceeding, you should read and understand SNLJOperator.java
     *    You can find it in the same directory as this file.
     *
     * Word of advice: try to decompose the problem into distinguishable sub-problems.
     *    This means you'll probably want to add more methods than those given (Once again,
     *    SNLJOperator.java might be a useful reference).
     *
     */
    private class SortMergeIterator extends JoinIterator {
        /**
        * Some member variables are provided for guidance, but there are many possible solutions.
        * You should implement the solution that's best for you, using any member variables you need.
        * You're free to use these member variables, but you're not obligated to.
        */
        private BacktrackingIterator<Record> leftIterator;
        private BacktrackingIterator<Record> rightIterator;
        private Record leftRecord;
        private Record nextRecord;
        private Record rightRecord;
        private boolean marked;

        private SortMergeIterator() {
            super();
            // TODO(hw3_part1): implement
            SortOperator leftSortOperator = new SortOperator(getTransaction(), this.getLeftTableName(), new LeftRecordComparator());
            SortOperator rightSortOperator = new SortOperator(getTransaction(), this.getRightTableName(), new RightRecordComparator());

            String sortedLeftTableName = leftSortOperator.sort();
            String sortedRightTableName = rightSortOperator.sort();

            this.leftIterator = SortMergeOperator.this.getRecordIterator(sortedLeftTableName);
            this.rightIterator = SortMergeOperator.this.getRecordIterator(sortedRightTableName);

            this.marked = false;
            this.leftRecord = leftIterator.hasNext() ? leftIterator.next() : null;
            this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;

            try {
                fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
        }

        /**
         * After this method is called, rightRecord will contain the last marked record in the rightSource.
         */
        private void resetRightRecord() {
            this.rightIterator.reset();
            assert(rightIterator.hasNext());
            rightRecord = rightIterator.next();
        }

        /**
         * Advances the left record.
         *
         * The thrown exception means we're done: there is no next record
         * It causes this.fetchNextRecord (the caller) to hand control to its caller.
         */
        private void nextLeftRecord() {
            if (!leftIterator.hasNext()) { throw new NoSuchElementException("All Done!"); }
            leftRecord = leftIterator.next();
        }

        /**
         * Pre-fetches what will be the next record, and puts it in this.nextRecord.
         * Pre-fetching simplifies the logic of this.hasNext() and this.next()
         */
        private void fetchNextRecord() {
            if (this.leftRecord == null) {
                throw new NoSuchElementException("No new record to fetch");
            }
            this.nextRecord = null;
            do {
                DataBox leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                DataBox rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getRightColumnIndex());

                if (!marked) {
                    while (leftJoinValue.compareTo(rightJoinValue) < 0) {
                        this.leftRecord = leftIterator.next();
                        leftJoinValue = leftRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    }
                    while (leftJoinValue.compareTo(rightJoinValue) > 0) {
                        this.rightRecord = rightIterator.next();
                        rightJoinValue = rightRecord.getValues().get(SortMergeOperator.this.getLeftColumnIndex());
                    }
                    rightIterator.markPrev();
                    marked = true;
                }
                if (leftJoinValue.equals(rightJoinValue)) {
                    List<DataBox> leftValues = new ArrayList<>(this.leftRecord.getValues());
                    List<DataBox> rightValues = new ArrayList<>(this.rightRecord.getValues());
                    leftValues.addAll(rightValues);
                    this.nextRecord = new Record(leftValues);
                    this.rightRecord = rightIterator.hasNext() ? rightIterator.next() : null;
                    // Handling corner cases.
                    // (1) Right record reaches the end.
                    // (2) Left record reaches the end.
                    if (this.rightRecord == null) {
                        try {
                            nextLeftRecord();
                        } catch (NoSuchElementException e) {
                            this.leftRecord = null;
                        }
                        resetRightRecord();
                        marked = false;
                    }
                } else {
                    nextLeftRecord();
                    resetRightRecord();
                    marked = false;
                }
            } while (!hasNext());
        }

        /**
         * Checks if there are more record(s) to yield
         *
         * @return true if this iterator has another record to yield, otherwise false
         */
        @Override
        public boolean hasNext() {
            // TODO(hw3_part1): implement
            return this.nextRecord != null;
        }

        /**
         * Yields the next record of this iterator.
         *
         * @return the next Record
         * @throws NoSuchElementException if there are no more Records to yield
         */
        @Override
        public Record next() {
            // TODO(hw3_part1): implement
            if (!this.hasNext()) {
                throw new NoSuchElementException();
            }

            Record nextRecord = this.nextRecord;
            try {
                this.fetchNextRecord();
            } catch (NoSuchElementException e) {
                this.nextRecord = null;
            }
            return nextRecord;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private class LeftRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getLeftColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getLeftColumnIndex()));
            }
        }

        private class RightRecordComparator implements Comparator<Record> {
            @Override
            public int compare(Record o1, Record o2) {
                return o1.getValues().get(SortMergeOperator.this.getRightColumnIndex()).compareTo(
                           o2.getValues().get(SortMergeOperator.this.getRightColumnIndex()));
            }
        }
    }
}
