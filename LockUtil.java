package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     * <p>
     * `requestType` is guaranteed to be one of: S, X, NL.
     * <p>
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     * lock type can be, and think about how ancestor looks will need to be
     * acquired or changed.
     * <p>
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement

        List<LockContext> parents = getParents(lockContext, transaction, requestType);
        Collections.reverse(parents);

        // requesting NL should do nothing
        if (requestType == LockType.NL) {
            return;
        }

        // if the context has no parent it can acquire any lock is wants
        if (parentContext == null) {
            lockContext.acquire(transaction, requestType);

        } // if it is substitutable, there is no need to check parents
        else if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }

        // lock is guaranteed to be NL, S, X and we already checked NL
        else if (requestType.equals(LockType.S)) {

            // acquire necessary parent locks (least permissive possible)
            for (int i = 0; i < parents.size(); i++) {
                if (parents.get(i).getExplicitLockType(transaction) == LockType.NL) {
                    parents.get(i).acquire(transaction, LockType.IS);
                }
            }

            // acquire intent lock if no lock
            if (explicitLockType == LockType.NL) {
                lockContext.acquire(transaction, requestType);
            } // escalate to acquire read lock at lower granularity
            else if (explicitLockType == LockType.IS) {
                lockContext.escalate(transaction);
            } // else promote to SIX
            else {
                if (!(requestType == explicitLockType)) {
                    lockContext.promote(transaction, LockType.SIX);
                }
            }
        } // for requested X
        else if (requestType == LockType.X) {

            // acquire parent locks if necessary
            for (int j = 0; j < parents.size(); j++) {
                // if NL, just acquire intent (least permissive)
                if (parents.get(j).getExplicitLockType(transaction) == LockType.NL) {
                    parents.get(j).acquire(transaction, LockType.IX);
                } // if read intent, promote
                else if (parents.get(j).getExplicitLockType(transaction) == LockType.IS) {
                    parents.get(j).promote(transaction, LockType.IX);
                } // special case of promote to SIX
                else if (parents.get(j).getExplicitLockType(transaction) == LockType.S) {
                    parents.get(j).promote(transaction, LockType.SIX);
                }
            }
            // if NL, acquire X
            if (explicitLockType == LockType.NL) {
                lockContext.acquire(transaction, requestType);
            } // if IS, escalate to S and then promote to X
            else if (explicitLockType == LockType.IS) {
                lockContext.escalate(transaction);
                lockContext.promote(transaction, LockType.X);
            } // if S just promote straight to X
            else if (explicitLockType == LockType.S) {
                lockContext.promote(transaction, LockType.X);
            } // else case is just an escalate to X from IX o
            else {
                lockContext.escalate(transaction);
            }
        }


    }

    // TODO(proj4_part2) add any helper methods you want

    // just acquires a list of parents. Probably not an efficient or simple way to divide the work, but desperate times
    public static List<LockContext> getParents(LockContext context, TransactionContext transaction, LockType type) {

        LockContext parentContext = context.parentContext();
        List<LockContext> result = new ArrayList<>();

        while (parentContext != null) {
            result.add(parentContext);
            parentContext = parentContext.parentContext();
        }

        return result;
    }
}
