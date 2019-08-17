package tech.codestory.zookeeper.watcher;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;
import static org.testng.Assert.*;

/**
 * @author junyongliao
 * @date 2019/8/15
 * @since 1.0.0
 */
@Slf4j
public class ZooKeeperWatcherTest {
    @Test
    public void testWatcher() {
        String hostPort = "192.168.5.128:2181";
        String zNode = "/watcher";
        try {
            new ZooKeeperWatcher(hostPort, zNode).run();
        } catch (Exception e) {
            log.error("new ZooKeeperExecutor()", e);
        }
    }
}
