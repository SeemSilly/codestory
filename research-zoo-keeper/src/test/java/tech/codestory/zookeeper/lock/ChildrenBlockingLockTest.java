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

        ChildrenBlockingLock nodeLock = new ChildrenBlockingLock(address);

        boolean assertResult = nodeLock.lock(guidNodeName, clientGuid);
        log.info("获取分布式锁应该成功。");
        assert assertResult;

        // clientGuid 相同，释放锁
        assertResult = nodeLock.release(guidNodeName, clientGuid) == true;
        log.info("释放分布式锁，应该成功。");
        assert assertResult;
    }


    /**
     * 测试多线程，添加删除是否正常
     */
    @Test
    public void testChildrenBlockingMultiThread() throws IOException {
        String guidNodeName = "/multi-" + System.currentTimeMillis();
        int threadCount = LockClientThread.threadCount;
        LockClientThread.threadSemaphore = new CountDownLatch(threadCount);
        LockClientThread.successLockSemaphore = new CountDownLatch(1);

        LockClientThread[] threads = new LockClientThread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            ChildrenBlockingLock nodeBlocklessLock = new ChildrenBlockingLock(address);
            threads[i] = new LockClientThread(nodeBlocklessLock, guidNodeName, "client-" + (i + 1));
        }
        for (int i = 0; i < threadCount; i++) {
            threads[i].start();
        }
        try {
            LockClientThread.threadSemaphore.await();
            assert LockClientThread.successLockSemaphore.getCount() == 0;
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
    }

}
