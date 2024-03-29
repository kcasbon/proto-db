package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we suggest you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        /**
         * Check if `lockType` is compatible with preexisting locks. Allows
         * conflicts for locks held by transaction with id `except`, which is
         * useful when a transaction tries to replace a lock it already has on
         * the resource.
         */
        public boolean checkCompatible(LockType lockType, long except) {
            // TODO(proj4_part1): implement

            for (int i = 0; i < locks.size(); i ++) {
                if (!LockType.compatible(lockType, this.locks.get(i).lockType)) {
                    if (!this.locks.get(i).transactionNum.equals(except)) {
                        return false;
                    }
                }
            }
            return true;
        }

        /**
         * Gives the transaction the lock `lock`. Assumes that the lock is
         * compatible. Updates lock on resource if the transaction already has a
         * lock.
         */
        public void grantOrUpdateLock(Lock lock) {
            // TODO(proj4_part1): implement

            List<Lock> lockList = new ArrayList<>();

            if (transactionLocks.size() > 0) {
                lockList = transactionLocks.get(lock.transactionNum);
                if (lockList != null) {

                } else {
                    lockList = new ArrayList<>();
                }
            }

            for (int i = 0; i < lockList.size(); i ++) {
                if (lockList.get(i).name.equals(lock.name)) {
                    Lock remLock = lockList.get(i);
                    lockList.remove(remLock);
                    locks.remove(remLock);
                }
            }

            lockList.add(lock);
            transactionLocks.put(lock.transactionNum, lockList);
            this.locks.add(lock);
        }

        /**
         * Releases the lock `lock` and processes the queue. Assumes that the
         * lock has been granted before.
         */
        public void releaseLock(Lock lock) {
            // TODO(proj4_part1): implement



            List<Lock> lockList = transactionLocks.get(lock.transactionNum);
            lockList.remove(lock);
            transactionLocks.put(lock.transactionNum, lockList);

            this.locks.remove(lock);



            processQueue();
        }

        /**
         * Adds `request` to the front of the queue if addFront is true, or to
         * the end otherwise.
         */
        public void addToQueue(LockRequest request, boolean addFront) {
            // TODO(proj4_part1): implement

            if (addFront) {
                this.waitingQueue.addFirst(request);
            } else {
                this.waitingQueue.addLast(request);
            }
        }

        /**
         * Grant locks to requests from front to back of the queue, stopping
         * when the next lock cannot be granted. Once a request is completely
         * granted, the transaction that made the request can be unblocked.
         */
        private void processQueue() {
            Iterator<LockRequest> requests = waitingQueue.iterator();

            // TODO(proj4_part1): implement

            boolean stopProcess = false;

            while (!stopProcess) {

                if (requests.hasNext()) {
                    LockRequest lockReq = requests.next();
                    if (checkCompatible(lockReq.lock.lockType, lockReq.lock.transactionNum)) {
                        grantOrUpdateLock(lockReq.lock);

                        for (int l = 0; l < lockReq.releasedLocks.size(); l++) {
                            releaseLock(lockReq.releasedLocks.get(l));
                        }
                    waitingQueue.pop();
                    lockReq.transaction.unblock();

                    } else {
                        stopProcess = true;
                    }
                } else {
                    stopProcess = true;
                }
            }
        }


        /**
         * Gets the type of lock `transaction` has on this resource.
         */
        public LockType getTransactionLockType(long transaction) {
            // TODO(proj4_part1): implement

            for (int i = 0; i < locks.size(); i ++) {
                if (locks.get(i).transactionNum == transaction) {
                    return locks.get(i).lockType;
                }
            }
            return LockType.NL;
        }

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }
    }

    // You should not modify or use this directly.
    private Map<String, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        ResourceEntry rescEnt = getResourceEntry(name);

        synchronized (this) {
            for (int i = 0; i < rescEnt.locks.size(); i ++) {
                if (rescEnt.locks.get(i).transactionNum == transaction.getTransNum()) {
                    boolean inRelease = false;
                    for (int j = 0; j < releaseNames.size(); j ++) {
                        if (releaseNames.get(j).equals(name)) {
                            inRelease = true;
                        }
                    }
                    if (!inRelease) {
                        throw new DuplicateLockRequestException(
                                String.format("%s already contains a lock held by %s.", name, transaction));
                    }
                }
            }


            for (int i = 0; i < releaseNames.size(); i ++) {
                boolean lockHeld = false;
                List<Lock> rescLocks = getLocks(releaseNames.get(i));
                for (int j = 0; j < rescLocks.size(); j ++) {
                    if (rescLocks.get(j).transactionNum == transaction.getTransNum()) {
                        lockHeld = true;
                    }
                }
                if (!lockHeld && !releaseNames.isEmpty()) {
                    throw new NoLockHeldException(String.format("%s does not hold a lock " +
                            "on any of the given resources", transaction));
                }
            }


            if (!rescEnt.checkCompatible(lockType, transaction.getTransNum())) {
                shouldBlock = true;
                transaction.prepareBlock();
                rescEnt.addToQueue(new LockRequest(transaction,
                        new Lock(name, lockType, transaction.getTransNum())), false);
            } else {
                Iterator<LockRequest> queue = rescEnt.waitingQueue.iterator();
                for (int i = 0; i < rescEnt.waitingQueue.size(); i ++) {
                    if (queue.next().transaction.equals(transaction)) {
                        shouldBlock = true;
                        transaction.prepareBlock();
                        rescEnt.addToQueue(new LockRequest(transaction,
                                new Lock(name, lockType, transaction.getTransNum())),false);
                    }
                }
            }

        }
        if (shouldBlock) {
            transaction.block();
        } else {
            rescEnt.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));

            for (int i = 0; i < releaseNames.size(); i ++) {
                if (!name.equals(releaseNames.get(i))) {
                    release(transaction, releaseNames.get(i));
                }
            }
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        ResourceEntry rescEnt = getResourceEntry(name);

        boolean shouldBlock = false;
        synchronized (this) {
            for (int i = 0; i < rescEnt.locks.size(); i ++) {
                if (rescEnt.locks.get(i).transactionNum == transaction.getTransNum()) {
                    throw new DuplicateLockRequestException(
                            String.format("%s already contains a lock held by %s.", name, transaction));
                }
            }
            if (!rescEnt.checkCompatible(lockType, transaction.getTransNum())) {
                shouldBlock = true;
                transaction.prepareBlock();
                rescEnt.addToQueue(new LockRequest(transaction,
                        new Lock(name, lockType, transaction.getTransNum())), false);
            } else {
                Iterator<LockRequest> queue = rescEnt.waitingQueue.iterator();
                //for (int i = 0; i < rescEnt.waitingQueue.size(); i ++) {
                    if (queue.hasNext()) {
                        shouldBlock = true;
                        transaction.prepareBlock();
                        rescEnt.addToQueue(new LockRequest(transaction,
                                new Lock(name, lockType, transaction.getTransNum())),false);
                    //}
                }
            }

        }
        if (shouldBlock) {
            transaction.block();
        } else {
            rescEnt.grantOrUpdateLock(new Lock(name, lockType, transaction.getTransNum()));
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {

            ResourceEntry rescEnt = getResourceEntry(name);
            List<Lock> listLocks = transactionLocks.get(transaction.getTransNum());

            if (listLocks == null) {
                throw new NoLockHeldException(String.format("%s does not hold any locks", transaction));
            } else {
                boolean throwErr = true;
                int lockInd = 0;
                for (int i = 0; i < listLocks.size(); i ++) {
                    if (listLocks.get(i).transactionNum == transaction.getTransNum() && listLocks.get(i).name.equals(name)) {
                        throwErr = false;
                        lockInd = i;
                        break;
                    }
                }
                if (throwErr) {
                    throw new NoLockHeldException(String.format(
                            "%s does not hold a lock on resource %s", transaction, name));
                }

                rescEnt.releaseLock(listLocks.get(lockInd));
            }
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        ResourceEntry rescEnt = getResourceEntry(name);
        boolean shouldBlock = false;

        synchronized (this) {
            for (int i = 0; i < rescEnt.locks.size(); i ++) {
                if (rescEnt.locks.get(i).transactionNum == transaction.getTransNum()) {
                    if (rescEnt.locks.get(i).lockType.equals(newLockType)) {
                        throw new DuplicateLockRequestException(
                                String.format("%s already contains a lock held by %s.", name, transaction));
                    }
                }
            }

            boolean lockHeld = false;
            for (int i = 0; i < getLocks(transaction).size(); i ++) {
                if (getLocks(transaction).get(i).transactionNum == transaction.getTransNum()) {
                    lockHeld = true;
                }
            }

            if (!lockHeld) {
                throw new NoLockHeldException(String.format("%s does not hold a lock on this resource", transaction));
            }

            LockType oldType = rescEnt.getTransactionLockType(transaction.getTransNum());
            if (!LockType.substitutable(newLockType, oldType)) {
                throw new InvalidLockException(String.format("%s is not a promotion for %s", newLockType, oldType));
            }

            if (!rescEnt.checkCompatible(newLockType, transaction.getTransNum())) {
                shouldBlock = true;
                transaction.prepareBlock();
                rescEnt.addToQueue(new LockRequest(transaction,
                        new Lock(name, newLockType, transaction.getTransNum())), false);
            } else {
                Iterator<LockRequest> queue = rescEnt.waitingQueue.iterator();
                for (int i = 0; i < rescEnt.waitingQueue.size(); i ++) {
                    if (queue.next().transaction.equals(transaction)) {
                        shouldBlock = true;
                        transaction.prepareBlock();
                        rescEnt.addToQueue(new LockRequest(transaction,
                                new Lock(name, newLockType, transaction.getTransNum())),false);
                    }
                }
            }

        }
        if (shouldBlock) {
            transaction.block();
        } else {
            List<Lock> lockList = getLocks(name);
            for (int i = 0; i < lockList.size(); i++) {
                if (lockList.get(i).transactionNum == transaction.getTransNum()) {
                    rescEnt.releaseLock(lockList.get(i));
                }
            }

            rescEnt.grantOrUpdateLock(new Lock(name, newLockType, transaction.getTransNum()));
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        ResourceEntry resourceEntry = getResourceEntry(name);

        for (int i = 0; i < resourceEntry.locks.size(); i ++) {
            if (resourceEntry.locks.get(i).transactionNum == transaction.getTransNum()) {
                return resourceEntry.locks.get(i).lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, name));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database");
    }
}
