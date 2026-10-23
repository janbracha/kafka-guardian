package cz.company.guardian.logging;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class LogRateLimiterTest {

    private static final long SECOND = 1_000_000_000L;

    @Test
    void allowsUpToTheLimitPerSecondThenSuppresses() {
        AtomicLong clock = new AtomicLong(1_000);
        LogRateLimiter limiter = new LogRateLimiter(3, clock::get);

        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
        assertThat(limiter.tryAcquire()).isFalse();
        assertThat(limiter.drainSuppressed()).isEqualTo(2);
        assertThat(limiter.drainSuppressed()).isZero();
    }

    @Test
    void windowResetsAfterOneSecond() {
        AtomicLong clock = new AtomicLong(0);
        LogRateLimiter limiter = new LogRateLimiter(1, clock::get);

        assertThat(limiter.tryAcquire()).isTrue();
        assertThat(limiter.tryAcquire()).isFalse();
        clock.addAndGet(SECOND - 1);
        assertThat(limiter.tryAcquire()).isFalse();
        clock.addAndGet(1);
        assertThat(limiter.tryAcquire()).isTrue();
    }

    @Test
    void zeroDisablesLogging() {
        LogRateLimiter limiter = new LogRateLimiter(0, () -> 0);

        assertThat(limiter.tryAcquire()).isFalse();
        assertThat(limiter.drainSuppressed()).isEqualTo(1);
    }

    @Test
    void neverExceedsTheLimitUnderContention() throws InterruptedException {
        AtomicLong clock = new AtomicLong(0);
        LogRateLimiter limiter = new LogRateLimiter(100, clock::get);
        AtomicLong granted = new AtomicLong();
        Thread[] threads = new Thread[8];
        for (int t = 0; t < threads.length; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < 10_000; i++) {
                    if (limiter.tryAcquire()) {
                        granted.incrementAndGet();
                    }
                }
            });
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(granted.get()).isEqualTo(100);
        assertThat(limiter.drainSuppressed()).isEqualTo(8 * 10_000 - 100);
    }
}
