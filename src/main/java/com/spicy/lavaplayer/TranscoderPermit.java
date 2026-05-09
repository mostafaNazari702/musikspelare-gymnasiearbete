package com.spicy.lavaplayer;

import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;


public final class TranscoderPermit implements AutoCloseable {
    private final Semaphore semaphore;
    private final AtomicBoolean released = new AtomicBoolean(false);

    public TranscoderPermit(Semaphore semaphore) {
        this.semaphore = Objects.requireNonNull(semaphore, "semaphore");
    }

    public boolean isReleased() {
        return released.get();
    }

    public void release() {
        if (released.compareAndSet(false, true)) {
            semaphore.release();
        }
    }

    @Override
    public void close() {
        release();
    }
}
