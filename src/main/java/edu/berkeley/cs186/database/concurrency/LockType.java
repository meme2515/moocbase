package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Database;

import java.util.Arrays;
import java.util.List;

public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        if (a.toString() == "NL" | b.toString() == "NL") {
            return true;
        } else if (a.toString() == "IS") {
            if (b.toString() == "IS") {
                return true;
            } else if (b.toString() == "IX") {
                return true;
            } else if (b.toString() == "S") {
                return true;
            } else if (b.toString() == "SIX") {
                return true;
            }
        } else if (a.toString() == "IX") {
            if (b.toString() == "IS") {
                return true;
            } else if (b.toString() == "IX") {
                return true;
            }
        } else if (a.toString() == "S") {
            if (b.toString() == "IS") {
                return true;
            } else if (b.toString() == "S") {
                return true;
            }
        } else if (a.toString() == "SIX") {
            if (b.toString() == "IS") {
                return true;
            }
        }
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        List<String> sChild = Arrays.asList("S", "IS");
        List<String> sParent = Arrays.asList("IS", "IX");
        List<String> xChild = Arrays.asList("X", "IX", "SIX");
        List<String> xParent = Arrays.asList("IX", "SIX");
        List<String> nParent = Arrays.asList("X", "S", "IS", "IX", "SIX", "NL");

        if (sParent.contains(parentLockType.toString()) && sChild.contains(childLockType.toString())) {
            return true;
        } else if (xParent.contains(parentLockType.toString()) && xChild.contains(childLockType.toString())) {
            return true;
        } else if (nParent.contains(parentLockType.toString()) && childLockType.toString() == "NL") {
            return true;
        }
        return false;
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // TODO(hw4_part1): implement
        if (required.toString() == "NL") {
            return true;
        } else if (required.toString() == substitute.toString()) {
            return true;
        } else if (required.toString() == "S" && substitute.toString() == "X") {
            return true;
        } else if (required.toString() == "IS" && substitute.toString() == "IX") {
            return true;
        } else if ((required.toString() == "S" | required.toString() == "IX") && substitute.toString() == "SIX") {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

