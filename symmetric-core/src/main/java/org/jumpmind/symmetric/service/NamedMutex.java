package org.jumpmind.symmetric.service.impl;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class NamedMutex implements Lock {

    private static final Map<Object, WeakKeyLockPair> locks = new WeakHashMap<>();

    private final Object key;
    private final Lock lock;

    private NamedMutex(Object key, Lock lock) {
        this.key = key;
        this.lock = lock;
    }

    private static final class WeakKeyLockPair {

        private final Reference<Object> param;

        private final Lock lock;

        private WeakKeyLockPair (Object param, Lock lock) {
            this.param = new WeakReference<>(param);
            this.lock = lock;
        }
    }

    public static Lock getCanonicalLock(Object param) {
        Object canonical = null;
        Lock lock = null;

        synchronized (locks) {
            WeakKeyLockPair pair = locks.get(param);
            if (pair != null) {
                canonical = pair.param.get();
            }
            if (canonical == null) {
                canonical = param;
                pair = new WeakKeyLockPair(canonical, new ReentrantLock());
                locks.put(canonical, pair);
            }
        }

        lock = locks.get(canonical).lock;
        return new NamedMutex(canonical, lock);
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock.lockInterruptibly();
    }

    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return lock.tryLock(time, unit);
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public Condition newCondition() {
        return lock.newCondition();
    }
}