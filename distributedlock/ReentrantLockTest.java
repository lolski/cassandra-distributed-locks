package distributedlock;

import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
    public void lockOperationShouldSucceed_whenAttemptedConcurrently_ifLocksAreProperlyFreed() throws InterruptedException, ExecutionException {
        int finalCount = 1000;
        Count count = new Count(0);
        Lock underTest = new ReentrantLock();
        CompletableFuture[] tasks = new CompletableFuture[finalCount];
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        for (int i = 0; i < finalCount; ++i) {
            tasks[i] = CompletableFuture.runAsync(() -> {
                underTest.lock();
                count.set(count.get()+1);
                underTest.unlock();
            }, executorService);
        }
        CompletableFuture.allOf(tasks).get();
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
    public void tryLockOperationShouldSucceed_whenAttemptedConcurrently() throws InterruptedException, ExecutionException {
        for (int j = 0; j < 10000; ++j) {
            int numOfConcurrentOps = 32;
            Boolean[] holdsLock = new Boolean[numOfConcurrentOps];
            CompletableFuture[] tasks = new CompletableFuture[numOfConcurrentOps];
            Lock underTest = new ReentrantLock();
            ExecutorService executorService = Executors.newFixedThreadPool(numOfConcurrentOps);
            for (int i = 0; i < numOfConcurrentOps; ++i) {
                final int i_ = i;
                tasks[i] = CompletableFuture.runAsync(() -> holdsLock[i_] = underTest.tryLock(), executorService);
            }
            CompletableFuture.allOf(tasks).get();
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
            assertEquals(1, Arrays.asList(holdsLock).stream().filter(e -> e).count());
        }
    }

    @Test
    public void tryLockOperationShouldFail_ifLockIsCurrentlyHeld() throws ExecutionException, InterruptedException {
        Lock underTest = new ReentrantLock();
        assertFalse(CompletableFuture.runAsync(underTest::tryLock).thenApply(e -> underTest.tryLock()).get());
    }
}

