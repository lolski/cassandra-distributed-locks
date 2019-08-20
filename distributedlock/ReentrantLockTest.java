package distributedlock;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ReentrantLockTest {
    @Test
    public void lockOperationShouldSucceed_ifLockIsCurrentlyFree() {
        Lock underTest = new ReentrantLock();
        try {
            underTest.lock();
        }
        finally {
            underTest.unlock();
        }
    }

    @Test
    public void lockOperationShouldSucceed_whenAttemptedConcurrently_ifLocksAreProperlyFreed() throws InterruptedException {
        int finalCount = 1000;
        Count count = new Count(0);
        Lock underTest = new ReentrantLock();
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        for (int i = 0; i < finalCount; ++i) {
            CompletableFuture.runAsync(() -> {
                underTest.lock();
                count.set(count.get()+1);
                underTest.unlock();
            }, executorService);
        }
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        assertEquals(finalCount, count.get());
    }

    @Test
    public void tryLockOperationShouldSucceed_ifLockIsCurrentlyFree() {
        Lock underTest = new ReentrantLock();
        assertTrue(underTest.tryLock());
    }

    @Test
    public void tryLockOperationShouldFail_ifLockIsCurrentlyHeld() throws ExecutionException, InterruptedException {
        Lock underTest = new ReentrantLock();
        assertFalse(CompletableFuture.runAsync(underTest::tryLock).thenApply(e -> underTest.tryLock()).get());
    }
}

