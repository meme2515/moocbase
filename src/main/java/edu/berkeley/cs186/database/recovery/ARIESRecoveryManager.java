package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.concurrency.LockType;
import edu.berkeley.cs186.database.concurrency.LockUtil;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Lock context of the entire database.
    private LockContext dbContext;
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given transaction number.
    private Function<Long, Transaction> newTransaction;
    // Function to update the transaction counter.
    protected Consumer<Long> updateTransactionCounter;
    // Function to get the transaction counter.
    protected Supplier<Long> getTransactionCounter;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();

    // List of lock requests made during recovery. This is only populated when locking is disabled.
    List<String> lockRequests;

    public ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                                Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter) {
        this(dbContext, newTransaction, updateTransactionCounter, getTransactionCounter, false);
    }

    ARIESRecoveryManager(LockContext dbContext, Function<Long, Transaction> newTransaction,
                         Consumer<Long> updateTransactionCounter, Supplier<Long> getTransactionCounter,
                         boolean disableLocking) {
        this.dbContext = dbContext;
        this.newTransaction = newTransaction;
        this.updateTransactionCounter = updateTransactionCounter;
        this.getTransactionCounter = getTransactionCounter;
        this.lockRequests = disableLocking ? new ArrayList<>() : null;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     *
     * The master record should be added to the log, and a checkpoint should be taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor because of the cyclic dependency
     * between the buffer manager and recovery manager (the buffer manager must interface with the
     * recovery manager to block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManagerImpl(bufferManager);
    }

    // Forward Processing ////////////////////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be emitted, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // TODO(hw5): implement
        long LSN = this.logManager.appendToLog(new CommitTransactionLogRecord(transNum, this.transactionTable.get(transNum).lastLSN));
        this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
        this.transactionTable.get(transNum).lastLSN = LSN;
        this.logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be emitted, and the transaction table and transaction
     * status should be updated. No CLRs should be emitted.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // TODO(hw5): implement
        long LSN = this.logManager.appendToLog(new AbortTransactionLogRecord(transNum, this.transactionTable.get(transNum).lastLSN));
        this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.ABORTING);
        this.transactionTable.get(transNum).lastLSN = LSN;
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting.
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be emitted,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // TODO(hw5): implement
        if (this.transactionTable.get(transNum).transaction.getStatus() == Transaction.Status.ABORTING) {
            long lastLSN = this.transactionTable.get(transNum).lastLSN;
            LogRecord logRecord = this.logManager.fetchLogRecord(lastLSN);
            while (logRecord.getPrevLSN().isPresent()) {
                if (logRecord.isUndoable()) {
                    Pair<LogRecord, Boolean> CLR = logRecord.undo(lastLSN);
                    if (CLR.getSecond() == true) {
                        this.logManager.flushToLSN(CLR.getFirst().LSN);
                    }
                    this.logManager.appendToLog(CLR.getFirst());
                    CLR.getFirst().redo(diskSpaceManager, bufferManager);
                }
                logRecord = this.logManager.fetchLogRecord(logRecord.getPrevLSN().get());
            }
        }
        long LSN = this.logManager.appendToLog(new EndTransactionLogRecord(transNum, this.transactionTable.get(transNum).lastLSN));
        this.transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
        this.transactionTable.remove(transNum);
        return LSN;
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be emitted; if the number of bytes written is
     * too large (larger than BufferManager.EFFECTIVE_PAGE_SIZE / 2), then two records
     * should be written instead: an undo-only record followed by a redo-only record.
     *
     * Both the transaction table and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);

        // TODO(hw5): implement
        long lastLSN = this.transactionTable.get(transNum).lastLSN;
        long LSN;
        if (after.length > (bufferManager.EFFECTIVE_PAGE_SIZE / 2)) {
            byte[] emptyArray = {};
            this.logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, emptyArray));
            LSN = this.logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, emptyArray, after));
        } else {
            LSN = this.logManager.appendToLog(new UpdatePageLogRecord(transNum, pageNum, lastLSN, pageOffset, before, after));
        }
        this.transactionTable.get(transNum).lastLSN = LSN;
        if (!this.transactionTable.get(transNum).touchedPages.contains(pageNum)) {
            this.transactionTable.get(transNum).touchedPages.add(pageNum);
        }
        if (!this.dirtyPageTable.containsKey(pageNum)) {
            this.dirtyPageTable.put(pageNum, LSN);
        }
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be emitted, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) {
            return -1L;
        }

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN, touchedPages
        transactionEntry.lastLSN = LSN;
        transactionEntry.touchedPages.add(pageNum);
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // All of the transaction's changes strictly after the record at LSN should be undone.
        long LSN = transactionEntry.getSavepoint(name);

        // TODO(hw5): implement
        long lastLSN = this.transactionTable.get(transNum).lastLSN;
        LogRecord logRecord = this.logManager.fetchLogRecord(lastLSN);
        while ((logRecord.getPrevLSN().isPresent()) && (logRecord.LSN != LSN)) {
            if (logRecord.isUndoable()) {
                Pair<LogRecord, Boolean> CLR = logRecord.undo(lastLSN);
                if (CLR.getSecond() == true) {
                    this.logManager.flushToLSN(CLR.getFirst().LSN);
                }
                this.logManager.appendToLog(CLR.getFirst());
                CLR.getFirst().redo(diskSpaceManager, bufferManager);
            }
            logRecord = this.logManager.fetchLogRecord(logRecord.getPrevLSN().get());
        }
        return;
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible,
     * using recLSNs from the DPT, then status/lastLSNs from the transactions table,
     * and then finally, touchedPages from the transactions table, and written
     * when full (or when done).
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord(getTransactionCounter.get());
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> dpt = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> txnTable = new HashMap<>();
        Map<Long, List<Long>> touchedPages = new HashMap<>();
        int numTouchedPages = 0;

        // TODO(hw5): generate end checkpoint record(s) for DPT and transaction table
        // Iterate through dpt.
        for (Map.Entry<Long, Long> entry : this.dirtyPageTable.entrySet()) {
            if (dpt.size() >= bufferManager.EFFECTIVE_PAGE_SIZE) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt = new HashMap<>();
            }
            dpt.put(entry.getKey(), entry.getValue());
        }
        // Iterate through txnTable 1.
        for (Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()) {
            if (txnTable.size() >= bufferManager.EFFECTIVE_PAGE_SIZE) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt = new HashMap<>();
                txnTable = new HashMap<>();
            }
            txnTable.put(entry.getKey(), new Pair(entry.getValue().transaction.getStatus(), entry.getValue().lastLSN));
        }
        // Iterate through txnTable 2.
        for (Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()) {
            if (txnTable.size() >= bufferManager.EFFECTIVE_PAGE_SIZE) {
                LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                logManager.appendToLog(endRecord);
                dpt = new HashMap<>();
                txnTable = new HashMap<>();
                touchedPages = new HashMap<>();
            }
            List<Long> pages = new ArrayList<>();
            pages.addAll(entry.getValue().touchedPages);
            if (pages.size() > 0) {
                touchedPages.put(entry.getKey(), pages);
            }
        }

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            long transNum = entry.getKey();
            for (long pageNum : entry.getValue().touchedPages) {
                boolean fitsAfterAdd;
                if (!touchedPages.containsKey(transNum)) {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size() + 1, numTouchedPages + 1);
                } else {
                    fitsAfterAdd = EndCheckpointLogRecord.fitsInOneRecord(
                                       dpt.size(), txnTable.size(), touchedPages.size(), numTouchedPages + 1);
                }

                if (!fitsAfterAdd) {
                    LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
                    logManager.appendToLog(endRecord);

                    dpt.clear();
                    txnTable.clear();
                    touchedPages.clear();
                    numTouchedPages = 0;
                }

                touchedPages.computeIfAbsent(transNum, t -> new ArrayList<>());
                touchedPages.get(transNum).add(pageNum);
                ++numTouchedPages;
            }
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(dpt, txnTable, touchedPages);
        logManager.appendToLog(endRecord);

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    // TODO(hw5): add any helper methods needed

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery //////////////////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery. Recovery is
     * complete when the Runnable returned is run to termination. New transactions may be
     * started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the dirty page
     * table of non-dirty pages (pages that aren't dirty in the buffer manager) between
     * redo and undo, and perform a checkpoint after undo.
     *
     * This method should return right before undo is performed.
     *
     * @return Runnable to run to finish restart recovery
     */
    @Override
    public Runnable restart() {
        // TODO(hw5): implement
        restartAnalysis();
        restartRedo();

        class dirtyPageCheck implements BiConsumer<Long, Boolean> {
            @Override
            public void accept(Long pageNum, Boolean dirty) {
                if (!dirty) {
                    dirtyPageTable.remove(pageNum);
                }
            }
        }
        BiConsumer<Long, Boolean> dpc = new dirtyPageCheck();
        this.bufferManager.iterPageNums(dpc);

        class UndoRunnable implements Runnable {
            @Override
            public void run() {
                restartUndo();
                checkpoint();
            }
        }
        return new UndoRunnable();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the begin checkpoint record.
     *
     * If the log record is for a transaction operation:
     * - update the transaction table
     * - if it's page-related (as opposed to partition-related),
     *   - add to touchedPages
     *   - acquire X lock
     *   - update DPT (alloc/free/undoalloc/undofree always flushes changes to disk)
     *
     * If the log record is for a change in transaction status:
     * - clean up transaction (Transaction#cleanup) if END_TRANSACTION
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     *
     * If the log record is a begin_checkpoint record:
     * - Update the transaction counter
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Update lastLSN to be the larger of the existing entry's (if any) and the checkpoint's;
     *   add to transaction table if not already present.
     * - Add page numbers from checkpoint's touchedPages to the touchedPages sets in the
     *   transaction table if the transaction has not finished yet, and acquire X locks.
     *
     * Then, cleanup and end transactions that are in the COMMITING state, and
     * move all transactions in the RUNNING state to RECOVERY_ABORTING.
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        assert (record != null);
        // Type casting
        assert (record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // Get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;

        // TODO(hw5): implement
        Iterator<LogRecord> iter = this.logManager.scanFrom(LSN);
        LogRecord recordEntry = iter.next();

        HashSet<LogType> transaction = new HashSet<>();
        transaction.add(LogType.ALLOC_PAGE);
        transaction.add(LogType.UPDATE_PAGE);
        transaction.add(LogType.FREE_PAGE);
        transaction.add(LogType.ALLOC_PART);
        transaction.add(LogType.FREE_PART);
        transaction.add(LogType.UNDO_ALLOC_PAGE);
        transaction.add(LogType.UNDO_UPDATE_PAGE);
        transaction.add(LogType.UNDO_FREE_PAGE);
        transaction.add(LogType.UNDO_ALLOC_PART);
        transaction.add(LogType.UNDO_FREE_PART);

        HashSet<LogType> transactionPage = new HashSet<>();
        transactionPage.add(LogType.ALLOC_PAGE);
        transactionPage.add(LogType.UPDATE_PAGE);
        transactionPage.add(LogType.FREE_PAGE);
        transactionPage.add(LogType.UNDO_ALLOC_PAGE);
        transactionPage.add(LogType.UNDO_UPDATE_PAGE);
        transactionPage.add(LogType.UNDO_FREE_PAGE);

        HashSet<LogType> status = new HashSet<>();
        status.add(LogType.COMMIT_TRANSACTION);
        status.add(LogType.ABORT_TRANSACTION);
        status.add(LogType.END_TRANSACTION);

        HashSet<LogType> checkpoint = new HashSet<>();
        checkpoint.add(LogType.BEGIN_CHECKPOINT);
        checkpoint.add(LogType.END_CHECKPOINT);

        do {
            // If the log record is for a transaction operation:
            if (transaction.contains(recordEntry.getType())) {
                long transNum = recordEntry.getTransNum().get();
                Transaction trans = newTransaction.apply(transNum);
                // If the transaction is not in the transaction table, it should be added to the table
                // (the newTransaction function object can be used to create a Transaction object).
                if (!this.transactionTable.containsKey(recordEntry)) {
                    this.startTransaction(trans);
                }
                // The lastLSN of the transaction should be updated.
                this.transactionTable.get(transNum).lastLSN = recordEntry.LSN;

                // If the log record is about a page (as opposed to the partition-related log records),
                // the page needs to be added to the touchedPages set in the transaction table entry
                // and the transaction needs to request an X lock on it.
                if (transactionPage.contains(recordEntry.getType())) {
                    this.transactionTable.get(transNum).touchedPages.add(recordEntry.getPageNum().get());
                    acquireTransactionLock(trans, getPageLockContext(recordEntry.getPageNum().get()), LockType.X);
                    if (recordEntry.getType() == LogType.UPDATE_PAGE || recordEntry.getType() == LogType.UNDO_UPDATE_PAGE) {
                        // UpdatePage/UndoUpdatePage both may dirty a page in memory, without flushing changes to disk.
                        if (!this.dirtyPageTable.containsKey(recordEntry.getPageNum().get())) {
                            this.dirtyPageTable.put(recordEntry.getPageNum().get(), recordEntry.LSN);
                        }
                    } else {
                        // AllocPage/FreePage/UndoAllocPage/UndoFreePage all make their changes visible on
                        // disk immediately, and can be seen as flushing all changes at the time (including their own) to disk.
                        if (this.dirtyPageTable.containsKey(recordEntry.getPageNum().get())) {
                            this.dirtyPageTable.remove(recordEntry.getPageNum().get());
                        }
                    }
                }
                // If the log record is for a change in transaction status:
            } else if (status.contains(recordEntry.getType())) {
                // These three types of log records (CommitTransaction/AbortTransaction/EndTransaction)
                // all change the status of a transaction.
                //
                // When one of these records are encountered, the transaction table should be updated as described
                // in the previous section. The status of the transaction should also be set to one of
                // COMMITTING, RECOVERY_ABORTING, or COMPLETE.
                if (recordEntry.getType() == LogType.COMMIT_TRANSACTION) {
                    this.transactionTable.get(recordEntry.getTransNum().get()).transaction.setStatus(Transaction.Status.COMMITTING);
                    this.transactionTable.get(recordEntry.getTransNum().get()).lastLSN = recordEntry.LSN;
                } else if (recordEntry.getType() == LogType.ABORT_TRANSACTION) {
                    this.transactionTable.get(recordEntry.getTransNum().get()).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
                    this.transactionTable.get(recordEntry.getTransNum().get()).lastLSN = recordEntry.LSN;
                } else if (recordEntry.getType() == LogType.END_TRANSACTION) {
                    this.transactionTable.get(recordEntry.getTransNum().get()).transaction.cleanup();
                    this.transactionTable.get(recordEntry.getTransNum().get()).transaction.setStatus(Transaction.Status.COMPLETE);
                    this.transactionTable.remove(recordEntry.getTransNum().get());
                }
            } else if (checkpoint.contains(recordEntry.getType())) {
                // When a BeginCheckpoint record is encountered, the transaction counter needs to be updated (updateTransactionCounter).
                if (recordEntry.getType() == LogType.BEGIN_CHECKPOINT) {
                    updateTransactionCounter.accept(recordEntry.getMaxTransactionNum().get());
                } else {
                    // When an EndCheckpoint record is encountered, the tables stored in the record
                    // should be combined with the tables currently in memory.
                    for (Map.Entry<Long, Long> entry : recordEntry.getDirtyPageTable().entrySet()) {
                        if (this.dirtyPageTable.containsKey(entry.getKey())) {
                            this.dirtyPageTable.remove(entry.getKey());
                            this.dirtyPageTable.put(entry.getKey(), entry.getValue());
                        } else {
                            this.dirtyPageTable.put(entry.getKey(), entry.getValue());
                        }
                    }
                    for (Map.Entry<Long, Pair<Transaction.Status, Long>> entry : recordEntry.getTransactionTable().entrySet()) {
                        if (this.transactionTable.containsKey(entry.getKey())) {
                            if (this.transactionTable.get(entry.getKey()).lastLSN <= entry.getValue().getSecond()) {
                                this.transactionTable.get(entry.getKey()).lastLSN = entry.getValue().getSecond();
                            }
                        } else {
                            startTransaction(newTransaction.apply(entry.getKey()));
                            this.transactionTable.get(entry.getKey()).lastLSN = entry.getValue().getSecond();
                        }
                    }
                    for (Map.Entry<Long, List<Long>> entry : recordEntry.getTransactionTouchedPages().entrySet()) {
                        this.transactionTable.get(entry.getKey()).touchedPages.addAll(entry.getValue());
                        if (recordEntry.getTransactionTable().get(entry.getKey()).getFirst() != Transaction.Status.COMPLETE) {
                            for (Long page : entry.getValue()) {
                                Transaction trans = newTransaction.apply(entry.getKey());
                                acquireTransactionLock(trans, getPageLockContext(page), LockType.X);
                            }
                        }
                    }
                }
            }
            if (!iter.hasNext()) {
                break;
            }
            recordEntry = iter.next();
        } while (true);

        // All transactions in the COMMITTING state should be ended (cleanup(), state set to COMPLETE,
        // end transaction record written, and removed from the transaction table).
        //
        // All transactions in the RUNNING state should be moved into the RECOVERY_ABORTING state,
        // and an abort transaction record should be written.
        //
        // Nothing needs to be done for transactions in the RECOVERY_ABORTING state.
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            Transaction.Status state = entry.getValue().transaction.getStatus();
            if (state == Transaction.Status.COMMITTING) {
                entry.getValue().transaction.cleanup();
                end(entry.getValue().transaction.getTransNum());
            } else if (state == Transaction.Status.RUNNING) {
                abort(entry.getValue().transaction.getTransNum());
                this.transactionTable.get(entry.getValue().transaction.getTransNum()).
                        transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            }
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the DPT.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - about a page (Update/Alloc/Free/Undo..Page) in the DPT with LSN >= recLSN,
     *   the page is fetched from disk and the pageLSN is checked, and the record is redone.
     * - about a partition (Alloc/Free/Undo..Part), redo it.
     */
    void restartRedo() {
        // TODO(hw5): implement
        HashSet<LogType> partSet = new HashSet<>();
        partSet.add(LogType.ALLOC_PART);
        partSet.add(LogType.FREE_PART);
        partSet.add(LogType.UNDO_ALLOC_PART);
        partSet.add(LogType.UNDO_FREE_PART);

        HashSet<LogType> pageSet = new HashSet<>();
        pageSet.add(LogType.ALLOC_PAGE);
        pageSet.add(LogType.UPDATE_PAGE);
        pageSet.add(LogType.FREE_PAGE);
        pageSet.add(LogType.UNDO_ALLOC_PAGE);
        pageSet.add(LogType.UNDO_UPDATE_PAGE);
        pageSet.add(LogType.UNDO_FREE_PAGE);

        Long minLSN = Collections.min(this.dirtyPageTable.values());
        Iterator<LogRecord> iter = this.logManager.scanFrom(minLSN);
        while (iter.hasNext()) {
            LogRecord recordEntry = iter.next();
            if (recordEntry.isRedoable()) {
                if (pageSet.contains(recordEntry.type)) {
                    if (this.dirtyPageTable.containsKey(recordEntry.getPageNum().get())) {
                        if (recordEntry.LSN >= this.dirtyPageTable.get(recordEntry.getPageNum().get())) {
                            long pageLSN = this.bufferManager.fetchPage(getPageLockContext(recordEntry.getPageNum().get()),
                                    recordEntry.getPageNum().get(), false).getPageLSN();
                            if (pageLSN < recordEntry.LSN) {
                                recordEntry.redo(diskSpaceManager, bufferManager);
                            }
                        }
                    }
                } else if (partSet.contains(recordEntry.type)) {
                    recordEntry.redo(diskSpaceManager, bufferManager);
                }
            }
        }
        return;
    }

    /**
     * This method performs the redo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly;
     * - replace the entry in the set should be replaced with a new one, using the undoNextLSN
     *   (or prevLSN if none) of the record; and
     * - if the new LSN is 0, end the transaction and remove it from the queue and transaction table.
     */
    void restartUndo() {
        // TODO(hw5): implement
        HashSet<LogType> clr = new HashSet<>();
        clr.add(LogType.UNDO_ALLOC_PAGE);
        clr.add(LogType.UNDO_UPDATE_PAGE);
        clr.add(LogType.UNDO_FREE_PAGE);
        clr.add(LogType.UNDO_ALLOC_PART);
        clr.add(LogType.UNDO_FREE_PART);

        // First, a priority queue is created sorted on lastLSN of all aborting transactions.
        PriorityQueue<Pair<Long, LogRecord>> abortingQueue = new PriorityQueue<Pair<Long, LogRecord>>(new PairFirstReverseComparator());
        for (Map.Entry<Long, TransactionTableEntry> entry : this.transactionTable.entrySet()) {
            abortingQueue.add(new Pair(entry.getValue().lastLSN, this.logManager.fetchLogRecord(entry.getValue().lastLSN)));
        }
        while (abortingQueue.size() != 0) {
            Pair<Long, LogRecord> lr = abortingQueue.poll();
            // If the record is undoable, undo it, emit the appropriate CLR, and update tables accordingly.
            if (lr.getSecond().isUndoable()) {
                Pair<LogRecord, Boolean> clrRecord = lr.getSecond().undo(this.transactionTable.get(lr.getSecond().getTransNum().get()).lastLSN);
                this.logManager.appendToLog(clrRecord.getFirst());
                this.logManager.flushToLSN(clrRecord.getFirst().LSN);
                this.transactionTable.get(clrRecord.getFirst().getTransNum().get()).lastLSN = clrRecord.getFirst().LSN;
                clrRecord.getFirst().redo(diskSpaceManager, bufferManager);
            }

            // Replace the entry in the set should be replaced with a new one, using the undoNextLSN
            // (or prevLSN if none) of the record.
            if (clr.contains(lr.getSecond().type)) {
                if (lr.getSecond().getUndoNextLSN() != null) {
                    abortingQueue.add(new Pair(lr.getSecond().getUndoNextLSN(),
                            this.logManager.fetchLogRecord(lr.getSecond().getUndoNextLSN().get())));
                } else {
                    this.logManager.appendToLog(new EndTransactionLogRecord(lr.getSecond().getTransNum().get(), lr.getSecond().LSN));
                }
            } else {
                // If the new LSN is 0, end the transaction and remove it from the queue and transaction table.
                if (lr.getSecond().getPrevLSN().get() != 0) {
                    abortingQueue.add(new Pair(lr.getSecond().getPrevLSN().get(),
                            this.logManager.fetchLogRecord(lr.getSecond().getPrevLSN().get())));
                } else {
                    this.logManager.appendToLog(new EndTransactionLogRecord(lr.getSecond().getTransNum().get(), lr.getSecond().LSN));
                    this.transactionTable.get(lr.getSecond().getTransNum().get()).transaction.setStatus(Transaction.Status.COMPLETE);
                    this.transactionTable.remove(lr.getSecond().getTransNum().get());
                }
            }
        }
        return;
    }

    // TODO(hw5): add any helper methods needed

    // Helpers ///////////////////////////////////////////////////////////////////////////////

    /**
     * Returns the lock context for a given page number.
     * @param pageNum page number to get lock context for
     * @return lock context of the page
     */
    private LockContext getPageLockContext(long pageNum) {
        int partNum = DiskSpaceManager.getPartNum(pageNum);
        return this.dbContext.childContext(partNum).childContext(pageNum);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transaction transaction to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(Transaction transaction, LockContext lockContext,
                                        LockType lockType) {
        acquireTransactionLock(transaction.getTransactionContext(), lockContext, lockType);
    }

    /**
     * Locks the given lock context with the specified lock type under the specified transaction,
     * acquiring locks on ancestors as needed.
     * @param transactionContext transaction context to request lock for
     * @param lockContext lock context to lock
     * @param lockType type of lock to request
     */
    private void acquireTransactionLock(TransactionContext transactionContext,
                                        LockContext lockContext, LockType lockType) {
        TransactionContext.setTransaction(transactionContext);
        try {
            if (lockRequests == null) {
                LockUtil.ensureSufficientLockHeld(lockContext, lockType);
            } else {
                lockRequests.add("request " + transactionContext.getTransNum() + " " + lockType + "(" +
                                 lockContext.getResourceName() + ")");
            }
        } finally {
            TransactionContext.unsetTransaction();
        }
    }

    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A), in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements
        Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
