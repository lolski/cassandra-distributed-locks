package distributedlock;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import repeat.Repeat;
import repeat.RepeatRule;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CassandraFencedLockTest {
    private int keyspaceNumber;
    @Rule
    public RepeatRule rule = new RepeatRule();

    @Before
    public void setup() {
        keyspaceNumber++;
        try (Cluster localhost = Cluster.builder().addContactPoint("localhost").build(); Session connection = localhost.connect()) {
            connection.execute("CREATE KEYSPACE keyspace_" + keyspaceNumber + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;");
        }
    }

    @After
    public void teardown() {
        try (Cluster localhost = Cluster.builder().addContactPoint("localhost").build(); Session connection = localhost.connect()) {
            connection.execute("DROP KEYSPACE keyspace_" + keyspaceNumber + ";");
        }
    }

    @Test
    public void lockOperationShouldSucceed_ifLockIsCurrentlyFree() throws InterruptedException {
        tryLockAndUnlock();
    }

    @Test
    public void lockOperationShouldSucceed_whenAttemptedConcurrently_ifLocksAreProperlyFreed() throws InterruptedException, ExecutionException {
        int finalCount = 1000;
        Count count = new Count(0);
        try (CassandraFencedLock underTest = new CassandraFencedLock("keyspace_" + keyspaceNumber)) {
            ExecutorService executorService = Executors.newFixedThreadPool(32);
            CompletableFuture[] tasks = new CompletableFuture[finalCount];
            for (int i = 0; i < finalCount; ++i) {
                tasks[i] = CompletableFuture.runAsync(() -> {
                    long fence = 0;
                    try {
                        fence = underTest.lock();
                        int value = count.get() + 1;
                        count.set(value);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        underTest.unlock(fence);
                    }
                }, executorService);
            }
            CompletableFuture.allOf(tasks).get();
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        assertEquals(finalCount, count.get());
    }

    @Test
    public void tryLockOperationShouldSucceed_ifLockIsCurrentlyFree() {
        try (CassandraFencedLock underTest = new CassandraFencedLock("keyspace_" + keyspaceNumber)) {
            Optional<Long> tryLock = underTest.tryLock();
            assertTrue(tryLock.isPresent());
            tryLock.get();
        }
    }
//
//    @Ignore
//    @Test
//    public void tryLockOperationShouldSucceed_ifLockIsCurrentlyFree_2() throws InterruptedException {
//        // set expire to 10ms
//        Lock underTest = new ReentrantLock();
//        Count count = new Count(0);
//        CompletableFuture.runAsync(() -> {
//            underTest.lock();
//            count.set(1);
//        });
//        Thread.sleep(50);
//        try {
//            CompletableFuture.runAsync(() -> {
//                underTest.lock();
//                count.set(2);
//            });
//        }
//        finally {
//            underTest.unlock();
//        }
//    }
//
    @Test
    @Repeat(times = 10, threads = 1)
    public void tryLockOperationShouldSucceed_whenAttemptedConcurrently() throws InterruptedException, ExecutionException {
        int numOfConcurrentOps = 32;
        Boolean[] holdsLock = new Boolean[numOfConcurrentOps];
        CompletableFuture[] tasks = new CompletableFuture[numOfConcurrentOps];
        try (CassandraFencedLock underTest = new CassandraFencedLock("keyspace_" + keyspaceNumber)) {
            ExecutorService executorService = Executors.newFixedThreadPool(numOfConcurrentOps);
            for (int i = 0; i < numOfConcurrentOps; ++i) {
                final int i_ = i;
                tasks[i] = CompletableFuture.runAsync(() -> holdsLock[i_] = underTest.tryLock().isPresent(), executorService);
            }
            CompletableFuture.allOf(tasks).get();
            executorService.shutdown();
            executorService.awaitTermination(5, TimeUnit.SECONDS);
        }
        assertEquals("Assertion failed on iteration ", 1, Arrays.asList(holdsLock).stream().filter(e -> e).count());
    }

    @Test
    public void tryLockOperationShouldFail_ifLockIsCurrentlyHeld() throws ExecutionException, InterruptedException {
        try (CassandraFencedLock underTest = new CassandraFencedLock("keyspace_" + keyspaceNumber)) {
            Optional<Long> fence = CompletableFuture.runAsync(underTest::tryLock).thenApply(e -> underTest.tryLock()).get();
            assertFalse(fence.isPresent());
        }
    }

    @Test
    public void unlockOperationShouldSucceed_ifLockIsCurrentlyHeld() throws InterruptedException {
        tryLockAndUnlock();
    }

    @Test
    public void unlockOperationShouldFail_ifLockIsCurrentlyFree() {
        try (CassandraFencedLock underTest = new CassandraFencedLock("keyspace_" + keyspaceNumber)) {
            try {
                underTest.unlock(1);
                fail("An expected RuntimeException was not thrown.");
            }
            catch (RuntimeException e) {
                // this exception is expected
            }
        }
    }

    // TODO: ideally it should throw but it's not easy to implement given Cassandra's design. we can live with it
    @Test
    public void unlockOperationShouldSucceed_evenWhenExecutedMultipleTimes() {
        try (CassandraFencedLock underTest = new CassandraFencedLock("keyspace_" + keyspaceNumber)) {
            Optional<Long> lock = underTest.tryLock();
            underTest.unlock(lock.get());
            underTest.unlock(lock.get());
        }
    }

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

    private void tryLockAndUnlock() throws InterruptedException {
        try (CassandraFencedLock underTest = new CassandraFencedLock("keyspace_" + keyspaceNumber)) {
            Long fence = null;
            try {
                fence = underTest.lock();
            } finally {
                if (fence != null) {
                    underTest.unlock(fence);
                } else {
                    System.err.println("Can't release the lock - invalid fence value: '" + fence + "'");
                }
            }
        }
    }
}

// TODO: implement a distributed attribute lock by using fenced lock
// the counter should be stored within a keyspace in a table attribute_lock(fence: counter)
class CassandraFencedLock implements AutoCloseable {
    private String keyspace;
    private static final int TIMEOUT_MS = 10000;
    private Cluster cluster = Cluster.builder().addContactPoint("localhost").build();
    private Session session = cluster.connect();

    CassandraFencedLock(String keyspace) {
        this.keyspace = keyspace;
        // create table if not exist
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".fenced_lock(attribute TEXT PRIMARY KEY, acquired_on BIGINT, expired_on BIGINT);");
        session.execute("INSERT INTO " + keyspace + ".fenced_lock(attribute, acquired_on, expired_on) VALUES('*', 0, 0) IF NOT EXISTS;");
    }

    long lock() throws InterruptedException {
        Optional<Long> lock;
        for (lock = tryLock(); !lock.isPresent(); lock = tryLock()) {
            // wait
            Thread.sleep(1000);
        }
        return lock.get();
    }

    Optional<Long> tryLock() {
        long acquiredOn = System.currentTimeMillis();
        long expiredOn = acquiredOn + TIMEOUT_MS;
        ResultSet tryLock = session.execute("UPDATE " + keyspace + ".fenced_lock SET acquired_on = " + acquiredOn + ", expired_on = " + expiredOn + " WHERE attribute = '*' IF expired_on < " + acquiredOn);
        if (tryLock.wasApplied()) {
            return Optional.of(acquiredOn);
        }
        else {
            return Optional.empty();
        }
    }

    void unlock(long acquiredOn) {
        long expiredOn = System.currentTimeMillis();
        ResultSet unlock = session.execute("UPDATE " + keyspace + ".fenced_lock SET expired_on = " + expiredOn + " WHERE attribute = '*' IF acquired_on = " + acquiredOn);
        if (!unlock.wasApplied()) {
            throw new RuntimeException("Invalid acquiredOn: '" + acquiredOn + "'");
        }
    }

    @Override
    public void close() {
        session.close();
        cluster.close();
    }
}