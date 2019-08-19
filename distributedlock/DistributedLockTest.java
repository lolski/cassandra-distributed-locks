package distributedlock;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

public class DistributedLockTest {
    @Test
    public void lockOperationShouldSucceed_ifNotHeld_becauseFree() {
    }

    @Test
    public void lockOperationShouldSucceed_ifNotHeld_becauseExpired() {
    }

    @Test
    public void lockOperationShouldSucceed_ifNotHeld_evenWhenAttemptedConcurrently_verifyOnlyOneClientGetsIt() {
    }

    @Test
    public void lockOperationShouldFail_ifHeld() {
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
}