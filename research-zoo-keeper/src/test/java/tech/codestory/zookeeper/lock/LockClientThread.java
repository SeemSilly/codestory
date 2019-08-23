package tech.codestory.zookeeper.lock;

import lombok.extern.slf4j.Slf4j;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 用于模拟客户端
 */
@Slf4j
public class LockClientThread extends Thread {
    public static int threadCount = 5;

    /** 只有一个线程能够成功拿到分布式锁 */
    public static CountDownLatch successLockSemaphore = new CountDownLatch(1);

    ZooKeeperLock zooKeeperLock;

    String guidNodeName;
    String clientGuid;

    public LockClientThread(ZooKeeperLock zooKeeperLock, String guidNodeName, String clientGuid)
            throws IOException {
        this.zooKeeperLock = zooKeeperLock;
        this.guidNodeName = guidNodeName;
        this.clientGuid = clientGuid;
    }

    @Override
    public void run() {
        log.info("{} lock() ...", clientGuid);
        boolean locked = zooKeeperLock.lock(guidNodeName, clientGuid);
        if (locked) {
            log.info("{} lock() success，拿到锁了，假装忙2秒", clientGuid);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            }
            log.info("{} release() ...", clientGuid);
            boolean released = zooKeeperLock.release(guidNodeName, clientGuid);
            log.info("{} release() result ： {}", clientGuid, released);
            successLockSemaphore.countDown();
        } else {
            log.info("{} lock() fail", clientGuid);
        }
    }
}
