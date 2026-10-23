package cz.company.guardian.logging;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * Lock-free fixed-window limiter: at most {@code permitsPerSecond} acquisitions succeed per second.
 * Rejected acquisitions are counted so the next emitted log line can report how many lines were dropped.
 */
public final class LogRateLimiter {

    private static final long WINDOW_NANOS = 1_000_000_000L;

    private final int permitsPerSecond;
    private final LongSupplier nanoClock;
    private final AtomicLong windowStart;
    private final AtomicLong usedInWindow = new AtomicLong();
    private final AtomicLong suppressed = new AtomicLong();

    public LogRateLimiter(int permitsPerSecond) {
        this(permitsPerSecond, System::nanoTime);
    }

    public LogRateLimiter(int permitsPerSecond, LongSupplier nanoClock) {
        this.permitsPerSecond = permitsPerSecond;
        this.nanoClock = nanoClock;
        this.windowStart = new AtomicLong(nanoClock.getAsLong());
    }

    public boolean tryAcquire() {
        long now = nanoClock.getAsLong();
        long start = windowStart.get();
        if (now - start >= WINDOW_NANOS && windowStart.compareAndSet(start, now)) {
            usedInWindow.set(0);
        }
        if (usedInWindow.incrementAndGet() <= permitsPerSecond) {
            return true;
        }
        suppressed.incrementAndGet();
        return false;
    }

    /** @return the number of acquisitions refused since the previous call, resetting the count */
    public long drainSuppressed() {
        return suppressed.getAndSet(0);
    }
}
