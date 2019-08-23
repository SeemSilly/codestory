package tech.codestory.zookeeper.children;

import java.security.SecureRandom;
import java.util.Random;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.testng.annotations.Test;
import lombok.extern.slf4j.Slf4j;
import tech.codestory.zookeeper.TestBase;
import tech.codestory.zookeeper.ZooKeeperBase;

/**
 * @author junyongliao
 * @date 2019/8/22
 * @since 1.0.0
 */
@Slf4j
public class SequentialTest extends TestBase {
    @Test
    public void testSequential() throws Exception {
        String rootNodeName = "/container-" + System.currentTimeMillis();
        ZooKeeperBase zooKeeper = new ZooKeeperBase(address);
        zooKeeper.createRootNode(rootNodeName, CreateMode.CONTAINER);

        Random random = new SecureRandom();
        long lastNumber = -1L;
        String[] prefixs = new String[] {"/a", "/b", "/c", "/d", "/e", "/f", "/g"};
        for (int i = 0; i < 10; i++) {
            int index = random.nextInt(prefixs.length);
            String childNodeName = rootNodeName + prefixs[index];
            String fullNodeName = zooKeeper.getZooKeeper().create(childNodeName, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            long number = Long.parseLong(fullNodeName.substring(childNodeName.length()));

            log.info("{} -> {}", fullNodeName, number);

            assert number == lastNumber + 1;
            lastNumber = number;
        }
    }
}
