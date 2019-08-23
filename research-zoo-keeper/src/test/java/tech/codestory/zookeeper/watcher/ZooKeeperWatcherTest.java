package tech.codestory.zookeeper.watcher;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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
    final String zNodeName = "/watcher-" + System.currentTimeMillis();
    final String zNodeValuePrefix = "Hello World ";

    @Test
    public void testWatcher() {
        try {
            ZooKeeperWatcher zooKeeperWatcher = new ZooKeeperWatcher(address, zNodeName);
            ZooKeeperWatcherWriter zooKeeperWatcherWriter = new ZooKeeperWatcherWriter(address);
            Thread watcherThread = new Thread(zooKeeperWatcher);
            Thread writerThread = new Thread(zooKeeperWatcherWriter);
            watcherThread.start();
            writerThread.start();

            writerThread.join();
            watcherThread.join();

            List<WatchedEvent> eventList = zooKeeperWatcher.getWatchedEventList();
            log.info("event list size = {}", eventList.size());

            assert eventList.size() >= 7;
            assert Watcher.Event.EventType.NodeCreated.equals(eventList.get(0).getType());
            for (int i = 1; i <= 5; i++) {
                assert Watcher.Event.EventType.NodeDataChanged.equals(eventList.get(i).getType());
            }
            assert Watcher.Event.EventType.NodeDeleted.equals(eventList.get(6).getType());
        } catch (Exception e) {
            log.error("new ZooKeeperExecutor()", e);
        }
    }

    /**
     * 用于操作ZooKeeper数据的线程
     */
    private class ZooKeeperWatcherWriter extends ZooKeeperBase implements Runnable {
        public ZooKeeperWatcherWriter(String address) throws IOException {
            super(address);
        }

        @Override
        public void run() {
            Random random = new SecureRandom();
            try {
                // 创建节点
                getZooKeeper().create(zNodeName, (zNodeValuePrefix + 0).getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                // 多次修改节点
                Stat stat = getZooKeeper().exists(zNodeName, false);
                for (int i = 1; i <= 5; i++) {
                    stat = getZooKeeper().setData(zNodeName, (zNodeValuePrefix + i).getBytes(),
                            stat.getVersion());
                    Thread.sleep(random.nextInt(10) * 1000);
                }
                // 删除节点
                getZooKeeper().delete(zNodeName, stat.getVersion());
            } catch (KeeperException e) {
                log.error("KeeperException", e);
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            }
        }
    }
}
