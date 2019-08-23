package tech.codestory.zookeeper.lock;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.testng.annotations.Test;
import lombok.extern.slf4j.Slf4j;
import tech.codestory.zookeeper.TestBase;

/**
 * 测试利用子节点实现的阻塞锁
 * 
 * @author code story
 * @date 2019/8/19
 */
@Slf4j
public class ChildrenBlockingLockTest extends TestBase {

    /**
     * 测试单线程，添加删除是否正常
     */
    @Test
    public void testChildrenBlocking() throws IOException {
        String guidNodeName = "/guid-" + System.currentTimeMillis();
        String clientGuid = "client-0";

        ChildrenBlockingLock zooKeeperLock = new ChildrenBlockingLock(address);

        boolean assertResult = zooKeeperLock.exists(guidNodeName) == false;
        log.info("锁还未生成，不存在。");
        assert assertResult;

        assertResult = zooKeeperLock.lock(guidNodeName, clientGuid);
        log.info("获取分布式锁应该成功。");
        assert assertResult;

        assertResult = zooKeeperLock.exists(guidNodeName) == true;
        log.info("锁已经生成。");
        assert assertResult;

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
    public void testChildrenBlockingMultiThread() throws IOException, InterruptedException {
        String guidNodeName = "/multi-" + System.currentTimeMillis();
        int threadCount = LockClientThread.threadCount;
        LockClientThread.successLockSemaphore = new CountDownLatch(threadCount);

        LockClientThread[] threads = new LockClientThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            ChildrenBlockingLock nodeBlocklessLock = new ChildrenBlockingLock(address);
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
