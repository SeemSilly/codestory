package tech.codestory.zookeeper.children;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.StringUtils;
import org.testng.annotations.Test;
import tech.codestory.zookeeper.TestBase;
import tech.codestory.zookeeper.ZooKeeperBase;
import tech.codestory.zookeeper.lock.ChildrenNodeLock;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.List;
import java.util.Random;

/**
 * 用于测试同一个节点下创建的多个有序子节点，是否是顺序获取
 * 
 * @author junyongliao
 * @date 2019/8/21
 */
@Slf4j
public class ChildrenNodeOrderTest extends TestBase {
    String rootNodeName = "/children-" + System.currentTimeMillis();
    String childNodePrefix = "element-";
    Random random = new SecureRandom();

    @Test
    public void testChildrenOrder() throws InterruptedException {
        int threadCount = 10;
        Thread[] orderThreads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            try {
                ChildrenNodeProcess processor = new ChildrenNodeProcess(address);
                orderThreads[i] = new Thread(processor);
            } catch (IOException e) {
                log.error("IOException", e);
            }
        }
        for (int i = 0; i < threadCount; i++) {
            orderThreads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            orderThreads[i].join();
        }
    }

    /**
     * 随机写入子节点，并过段时间自动删除
     */
    private class ChildrenNodeProcess extends ChildrenNodeLock implements Runnable {
        public ChildrenNodeProcess(String address) throws IOException {
            super(address);

            super.createRootNode(rootNodeName, CreateMode.CONTAINER);
        }

        @Override
        protected boolean isLockSuccess() throws KeeperException, InterruptedException {
            return false;
        }

        @Override
        public void run() {
            try {
                for (int i = 0; i < 10; i++) {
                    // 创建子节点
                    String fullChildNodeName =
                            getZooKeeper().create(rootNodeName + "/" + childNodePrefix, new byte[0],
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

                    List<String> elementNameList = getOrderedChildren(rootNodeName, false);
                    log.info(StringUtils.joinStrings(elementNameList, ","));
                    // 假设获取的子列表应该是递增的
                    long serial = -1L;
                    for (String elementName : elementNameList) {
                        long curSerial =
                                Long.parseLong(elementName.substring(childNodePrefix.length()));
                        assert curSerial > serial;
                        serial = curSerial;
                    }

                    Thread.sleep(random.nextInt(100) * 10);
                    getZooKeeper().delete(fullChildNodeName, 0);
                }
            } catch (KeeperException e) {
                log.error("KeeperException", e);
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            }
        }
    }
}
