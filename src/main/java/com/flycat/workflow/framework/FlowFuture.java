package com.flycat.workflow.framework;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlowFuture implements Future<Void> {
    private Object sync = new Object();
    private AtomicBoolean isDone = new AtomicBoolean(false);

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    public void setIsDone() {
        if (isDone.compareAndSet(false, true)) {
            synchronized (sync) {
                sync.notifyAll();
            }
        }
    }

    @Override
    public boolean isDone() {
        return isDone.get();
    }

    @Override
    public Void get() {
        synchronized (sync) {
            while (!isDone.get()) {
                try {
                    sync.wait();
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

    @Override
    public Void get(long timeout, TimeUnit unit) {
        long deadline = System.currentTimeMillis() + unit.toMillis(timeout);
        long leftTime = deadline - System.currentTimeMillis();
        while (leftTime > 0 && !isDone.get()) {
            synchronized (sync) {
                try {
                    sync.wait(leftTime);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            leftTime = deadline - System.currentTimeMillis();
        }
        return null;
    }
}
