package distributedlock;

import org.junit.Ignore;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CassandraFencedLockTest {
    @Test
    public void lockOperationShouldSucceed_ifLockIsCurrentlyFree() throws InterruptedException {
        CassandraFencedLock underTest = new CassandraFencedLock();
        Integer fence = null;
        try {
            fence = underTest.lock();
        }
        finally {
            if (fence != null) {
                underTest.unlock(fence);
            }
            else {
                fail("Can't release the lock - invalid fence value: '" + fence + "'");
            }
        }
    }

    @Ignore
    @Test
    public void lockOperationShouldSucceed_whenAttemptedConcurrently_ifLocksAreProperlyFreed() throws InterruptedException {
        int finalCount = 1000;
        Count count = new Count(0);
        CassandraFencedLock underTest = new CassandraFencedLock();
        ExecutorService executorService = Executors.newFixedThreadPool(32);
        for (int i = 0; i < finalCount; ++i) {
            CompletableFuture.runAsync(() -> {
                Integer fence = 0;
                try {
                    fence = underTest.lock();
                    count.set(count.get()+1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    underTest.unlock(fence);
                }
            }, executorService);
        }
        executorService.shutdown();
        executorService.awaitTermination(5, TimeUnit.SECONDS);
        assertEquals(finalCount, count.get());
    }

    @Test
    public void tryLockOperationShouldSucceed_ifLockIsCurrentlyFree() {
        CassandraFencedLock underTest = new CassandraFencedLock();
        assertTrue(underTest.tryLock().isPresent());
        underTest.tryLock().get();
    }

    @Ignore
    @Test
    public void tryLockOperationShouldSucceed_ifLockIsCurrentlyFree_2() throws InterruptedException {
        // set expire to 10ms
        Lock underTest = new ReentrantLock();
        Count count = new Count(0);
        CompletableFuture.runAsync(() -> {
            underTest.lock();
            count.set(1);
        });
        Thread.sleep(50);
        try {
            CompletableFuture.runAsync(() -> {
                underTest.lock();
                count.set(2);
            });
        }
        finally {
            underTest.unlock();
        }
    }

    @Test
    public void tryLockOperationShouldFail_ifLockIsCurrentlyHeld() throws ExecutionException, InterruptedException {
        CassandraFencedLock underTest = new CassandraFencedLock();
        Optional<Integer> fence = CompletableFuture.runAsync(underTest::tryLock).thenApply(e -> underTest.tryLock()).get();
        assertFalse(fence.isPresent());
    }


    ///


    @Test
    public void writeOperationShouldSucceed_ifLockHeldByYou() {

    }

    @Test
    public void writeOperationShouldFail_ifLockNotHeld() {

    }

    @Test
    public void writeOperationShouldFail_ifLockNotHeldByYou() {

    }

    @Test
    public void writeOperationShouldFail_ifLockNotHeld_becauseExpired() {

    }
}

class CassandraFencedLock {
    private AtomicInteger fence = new AtomicInteger();
    private Lock lock = new ReentrantLock();

    int lock() throws InterruptedException {
        for (Optional<Integer> lock = tryLock(); !lock.isPresent(); lock = tryLock()) {
            // wait
            Thread.sleep(1000);
        }
        return fence.getAndIncrement();
    }

    Optional<Integer> tryLock() {
        if (lock.tryLock()) {
            return Optional.of(fence.getAndIncrement());
        }
        else {
            return Optional.empty();
        }
    }

    void unlock(int fence) {
        lock.unlock();
    }
}