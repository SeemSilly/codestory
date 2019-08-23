package tech.codestory.zookeeper.lock;

import lombok.extern.slf4j.Slf4j;
import org.testng.annotations.Test;
import tech.codestory.zookeeper.TestBase;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 测试利用节点实现的非阻塞锁
 * 
 * @author code story
 * @date 2019/8/19
 */
@Slf4j
public class NodeBlocklessLockTest extends TestBase {

    /**
     * 测试单线程，添加删除是否正常
     */
    @Test
    public void testNodeBlockless() throws IOException {
        String guidNodeName = "/single-" + System.currentTimeMillis();
        String clientGuid = "client-0";

        NodeBlocklessLock zooKeeperLock = new NodeBlocklessLock(address);

        boolean assertResult = zooKeeperLock.exists(guidNodeName) == false;
        log.info("锁还未生成，不存在。");
        assert assertResult;

        assertResult = zooKeeperLock.lock(guidNodeName, clientGuid);
        log.info("获取分布式锁应该成功。");
        assert assertResult;

        assertResult = zooKeeperLock.exists(guidNodeName) == true;
        log.info("锁已经生成。");
        assert assertResult;

        assertResult = zooKeeperLock.release(guidNodeName, "error value") == false;
        log.info("clientGuid 不同，应该无法释放锁。");
        assert assertResult;

        // clientGuid 相同，释放锁
        assertResult = zooKeeperLock.release(guidNodeName, clientGuid) == true;
        log.info("正常释放锁，应该成功。");
        assert assertResult;

        assertResult = zooKeeperLock.exists(guidNodeName) == false;
        log.info("锁已被删除，应该不存在。");
        assert assertResult;
    }

    /**
     * 测试多线程，添加删除是否正常
     */
    @Test
    public void testNodeBlocklessMultiThread() throws IOException, InterruptedException {
        String guidNodeName = "/multi-" + System.currentTimeMillis();
        int threadCount = LockClientThread.threadCount;
        LockClientThread.successLockSemaphore = new CountDownLatch(1);

        LockClientThread[] threads = new LockClientThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            NodeBlocklessLock nodeBlocklessLock = new NodeBlocklessLock(address);
            threads[i] = new LockClientThread(nodeBlocklessLock, guidNodeName, "client-" + (i + 1));
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].join();
        }
        assert LockClientThread.successLockSemaphore.getCount() == 0;
    }
}
