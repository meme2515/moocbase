package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock acquisition
 * for the user (you, in the second half of Part 2). Generally speaking, you should use LockUtil
 * for lock acquisition instead of calling LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring LOCKTYPE on LOCKCONTEXT.
     *
     * This method should promote/escalate as needed, but should only grant the least
     * permissive set of locks needed.
     *
     * lockType must be one of LockType.S, LockType.X, and behavior is unspecified
     * if an intent lock is passed in to this method (you can do whatever you want in this case).
     *
     * If the current transaction is null (i.e. there is no current transaction), this method should do nothing.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType lockType) {
        // TODO(hw4_part2): implement

        TransactionContext transaction = TransactionContext.getTransaction(); // current transaction
        // null transaction
        if (transaction == null) {
            return;
        }
        // lock context is at top of hierarchy
        if (lockContext.parentContext() == null) {
            fillLock(transaction, lockContext, lockType);
        } else {
            // lock context has parents
            parentFill(transaction, lockContext, lockType);
        }
        return;
    }

    // TODO(hw4_part2): add helper methods as you see fit
    private static void fillLock(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        // auto-escalate
        autoEscalate(transaction, lockContext);

        if (lockType == LockType.X && lockContext.getExplicitLockType(transaction) == LockType.S) {
            lockContext.promote(transaction, lockType);
        } else if ((lockType == LockType.X && lockContext.getExplicitLockType(transaction) == LockType.IX )||
                (lockType == LockType.S && lockContext.getExplicitLockType(transaction) == LockType.IS )) {
            lockContext.escalate(transaction);
        } else if (lockContext.getExplicitLockType(transaction) == LockType.NL) {
            lockContext.acquire(transaction, lockType);
        } else if (lockType == LockType.S && lockContext.getExplicitLockType(transaction) == LockType.IX ) {
            lockContext.promote(transaction, LockType.SIX);
        }
    }

    private static void parentFill(TransactionContext transaction, LockContext lockContext, LockType lockType) {
        List<LockContext> parentContextList = new ArrayList<>();
        LockContext tempContext = lockContext;
        // traverse to top of hierarchy
        while (tempContext.parentContext() != null) {
            tempContext = tempContext.parentContext();
            parentContextList.add(tempContext);
        }
        // acquire intent locks from back of lock context list
        int index = parentContextList.size() - 1;
        while (index >= 0) {
            tempContext = parentContextList.get(index);
            // auto-escalate
            autoEscalate(transaction, tempContext);
            if ((lockType == LockType.X || lockType == LockType.S)  && tempContext.getExplicitLockType(transaction) == LockType.X) {
                return;
            } else if (lockType == LockType.S && tempContext.getExplicitLockType(transaction) == LockType.S) {
                return;
            } else if (lockType == LockType.X && tempContext.getExplicitLockType(transaction) == LockType.S) {
                tempContext.promote(transaction, LockType.SIX);
            } else if (lockType == LockType.X && tempContext.getExplicitLockType(transaction) == LockType.IS) {
                tempContext.promote(transaction, LockType.IX);
            } else if (lockType == LockType.X && tempContext.getExplicitLockType(transaction) == LockType.NL) {
                tempContext.acquire(transaction, LockType.IX);
            } else if (lockType == LockType.S && tempContext.getExplicitLockType(transaction) == LockType.NL) {
                tempContext.acquire(transaction, LockType.IS);
            }
            index = index - 1;
        }
        fillLock(transaction, lockContext, lockType);
        return;
    }

    private static void autoEscalate(TransactionContext transaction, LockContext lockContext) {
        if (lockContext.isTable() && lockContext.saturation(transaction) >= 0.2 && lockContext.capacity() >= 10) {
            lockContext.escalate(transaction);
        }
    }
}
