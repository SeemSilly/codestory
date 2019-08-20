package tech.codestory.zookeeper.watcher;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.zookeeper.WatchedEvent;
import org.testng.annotations.Test;
import lombok.extern.slf4j.Slf4j;
import tech.codestory.zookeeper.TestBase;
import tech.codestory.zookeeper.ZooKeeperBase;

/**
 * @author junyongliao
 * @date 2019/8/15
 * @since 1.0.0
 */
@Slf4j
public class ZooKeeperWatcherTest extends TestBase {

    @Test
    public void testWatcher() {
        final String zNode = "/watcher";
        try {
            ZooKeeperWatcher zooKeeperWatcher = new ZooKeeperWatcher(address, zNode);
            zooKeeperWatcher.run();
        } catch (Exception e) {
            log.error("new ZooKeeperExecutor()", e);
        }
    }
}
