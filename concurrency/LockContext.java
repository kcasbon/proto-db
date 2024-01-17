package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        // TODO(proj4_part2): implement

        // check readonly
        if (readonly) {
            throw new UnsupportedOperationException();
        }

        // check NL
        if (lockType.equals(LockType.NL)) {
            throw new InvalidLockException(String.format("You cannot acquire an NL. You should use release instead."));
        }

        // check duplicates
        boolean lockHeld = false;
        List<Lock> listLocks = lockman.getLocks(transaction);
        for (int i = 0; i < listLocks.size(); i ++) {
            if (listLocks.get(i).name.equals(name)) {
                lockHeld = true;
                break;
            }
        }

        if (lockHeld) {
            throw new DuplicateLockRequestException(String.format("%s already holds a lock on %s", transaction, name));
        }

        // check invalid
        if (parentContext() != null) {
            LockContext parContext = parentContext();
            LockType parentType = lockman.getLockType(transaction, parContext.name);

            if (!LockType.canBeParentLock(parentType, lockType)) {
                throw new InvalidLockException(String.format
                        ("%s can not be parent of %s", parentType, lockType));
            }
        }

        // acquire lock
        lockman.acquire(transaction, name, lockType);

        // update numChildLocks
        if (parentContext() != null) {
            int currChildren = parentContext().getNumChildren(transaction);
            parentContext().numChildLocks.put(transaction.getTransNum(), currChildren + 1);
        }

    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        // check readonly
        if (readonly) {
            throw new UnsupportedOperationException();
        }

        // check invalid lock
        List<Lock> allLocks = lockman.getLocks(transaction);
        List<Lock> allDesc = new ArrayList<>();

        for (int i = 0; i < allLocks.size(); i ++) {
            if (allLocks.get(i).name.isDescendantOf(name)) {
                allDesc.add(allLocks.get(i));
            }
        }

        for (int i = 0; i < allDesc.size(); i ++) {
            if (!LockType.canBeParentLock(LockType.NL, allDesc.get(i).lockType)) {
                throw new InvalidLockException("Releasing this lock violates multigranularity constraints.");
            }
        }


        // check no lock held
        boolean lockHeld = false;
        List<Lock> listLocks = lockman.getLocks(transaction);
        for (int i = 0; i < listLocks.size(); i ++) {
            if (listLocks.get(i).name.equals(name)) {
                lockHeld = true;
                break;
            }
        }

        if (!lockHeld) {
            throw new NoLockHeldException(String.format("%s does not hold a lock on %s", transaction, name));
        }

        // release lock
        lockman.release(transaction, name);

        // update numChildLocks
        if (parentContext() != null) {
            int currNumLocks = parentContext().numChildLocks.get(transaction.getTransNum());
            parentContext().numChildLocks.put(transaction.getTransNum(), currNumLocks - 1);
        }
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part2): implement

        // check readonly
        if (readonly) {
            throw new UnsupportedOperationException();
        }

        // check duplicate lock
        boolean hasLock = false;
        List<Lock> listLocks = lockman.getLocks(transaction);
        for (int i = 0; i < listLocks.size(); i ++) {
            if (listLocks.get(i).name.equals(name) && listLocks.get(i).lockType.equals(newLockType)) {
                hasLock = true;
            }
        }
        if (hasLock) {
            throw new DuplicateLockRequestException(String.format("%s already holds a lock of type %s on %s",
                    transaction, newLockType, name));
        }

        // check no lock held
        boolean isLock = false;
        List<Lock> lockList = lockman.getLocks(transaction);
        for (int i = 0; i < lockList.size(); i ++) {
            if (lockList.get(i).name.equals(name)) {
                isLock = true;
                break;
            }
        }

        if (!isLock) {
            throw new NoLockHeldException(String.format("%s does not hold a lock on %s", transaction, name));
        }

        // check invalid lock
        LockType type = getExplicitLockType(transaction);
        if (newLockType.equals(LockType.SIX)) {
            if (!(type.equals(LockType.IS) || type.equals(LockType.IX) || type.equals(LockType.S))) {
                throw new InvalidLockException(String.format("The current lock cannot be promoted to SIX."));
            }
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("This resource has an ancestor which already holds and SIX lock.");
            }
        } else {
            if (!(LockType.substitutable(newLockType, type) && !newLockType.equals(type))) {
                throw new InvalidLockException(String.format("%s cannot be promoted to type %s", type, newLockType));
            }
        }




        // promote the lock
        if (newLockType == LockType.SIX) {
            List<ResourceName> sIS = sisDescendants(transaction);
            int currChildren = getNumChildren(transaction);
            numChildLocks.put(transaction.getTransNum(), currChildren - sIS.size());
            sIS.add(name);
            lockman.acquireAndRelease(transaction, name, newLockType, sIS);

        } else {
            lockman.promote(transaction, name, newLockType);
        }
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        // TODO(proj4_part2): implement

        // check readonly
        if (readonly) {
            throw new UnsupportedOperationException();
        }

        // check no lock held
        boolean lockHeld = false;
        List<Lock> locks = lockman.getLocks(transaction);
        for (int i = 0; i < locks.size(); i ++) {
            if (locks.get(i).name.equals(name)) {
                lockHeld = true;
            }
        }

        if (!lockHeld) {
            throw new NoLockHeldException(String.format("%s does not hold a lock on %s", transaction, name));
        }

        // escalate
        LockType currType = getExplicitLockType(transaction);
        List<ResourceName> sIS = sisDescendants(transaction);
        int currChildren = getNumChildren(transaction);

        List<Lock> allLocks = lockman.getLocks(transaction);
        List<ResourceName> allDesc = new ArrayList<>();

        for (int i = 0; i < allLocks.size(); i ++) {
            if (allLocks.get(i).name.isDescendantOf(name)) {
                allDesc.add(allLocks.get(i).name);
            }
        }

        if (currType == LockType.S && sIS.size() == 0) {
            return;
        }
        else if (currType == LockType.S || currType == LockType.IS) {
            numChildLocks.put(transaction.getTransNum(), currChildren - sIS.size());
            sIS.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.S, sIS);
        }
        else if (currType == LockType.X && allDesc.size() == 0) {
            return;
        } else {
            numChildLocks.put(transaction.getTransNum(), currChildren - allDesc.size());
            allDesc.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.X, allDesc);
        }

    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement


        List<Lock> listLocks = lockman.getLocks(transaction);

        if (!listLocks.isEmpty()) {
            for (int i = 0; i < listLocks.size(); i ++) {
                if (listLocks.get(i).name.equals(name)) {
                    return listLocks.get(i).lockType;
                }
            }
        }

        return LockType.NL;
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // TODO(proj4_part2): implement

        LockType type = getExplicitLockType(transaction);

        if (type.equals(LockType.NL)) {
                if (hasSIXAncestor(transaction)) {
                    return LockType.S;
                }

                LockContext context = this;
                while (context.parentContext() != null) {
                    context = context.parentContext();
                    List<Lock> listLocks = lockman.getLocks(transaction);

                    for (int i = 0; i < listLocks.size(); i ++) {
                        if (listLocks.get(i).name.equals(context.name)) {
                            if (listLocks.get(i).lockType.equals(LockType.S)) {
                                return LockType.S;
                            } else if (listLocks.get(i).lockType.equals(LockType.X)) {
                                return LockType.X;
                            }
                        }
                    }
                }
        }

        return type;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        // TODO(proj4_part2): implement
        boolean hasSIX = false;

        LockContext context = this;
        while (context.parentContext() != null && !hasSIX) {
            context = context.parentContext();
            List<Lock> listLocks = lockman.getLocks(transaction);

            for (int i = 0; i < listLocks.size(); i ++) {
                if (listLocks.get(i).name.equals(context.name) && listLocks.get(i).lockType.equals(LockType.SIX)) {
                    hasSIX = true;
                    break;
                }
            }
        }
        return hasSIX;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    public List<ResourceName> sisDescendants(TransactionContext transaction) {
        // TODO(proj4_part2): implement

        List<Lock> lockList = lockman.getLocks(transaction);
        List<ResourceName> result = new ArrayList<>();

        for (int i = 0; i < lockList.size(); i ++) {
            if ((lockList.get(i).lockType.equals(LockType.S) || lockList.get(i).lockType.equals(LockType.IS))) {
                if (lockList.get(i).name.isDescendantOf(name)) {
                    result.add(lockList.get(i).name);
                }
            }
        }

        return result;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

