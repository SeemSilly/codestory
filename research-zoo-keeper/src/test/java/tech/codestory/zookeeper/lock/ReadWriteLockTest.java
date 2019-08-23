package tech.codestory.zookeeper.lock;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;
import org.testng.annotations.Test;
import lombok.extern.slf4j.Slf4j;
import tech.codestory.zookeeper.TestBase;

/**
 * 读写锁的测试
 * 
 * @author javacodestory@gmail.com
 * @date 2019/8/21
 */
@Slf4j
public class ReadWriteLockTest extends TestBase {
    String guidNodeName = "/ReadWriteLock-" + System.currentTimeMillis();
    Random random = new SecureRandom();

    /**
     * 连续两个读锁，应该同时获取到
     * 
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testReadRead() throws IOException, InterruptedException {
        ReadLockClient readLock1 = new ReadLockClient();
        ReadLockClient readLock2 = new ReadLockClient();

        readLock1.start();
        readLock2.start();

        readLock1.join();
        readLock2.join();
    }

    /**
     * 先读锁，再写锁。写锁应该等待读锁完成
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testReadWrite() throws IOException, InterruptedException {
        ReadLockClient readLock1 = new ReadLockClient();
        WriteLockClient writeLock1 = new WriteLockClient();

        readLock1.start();
        Thread.sleep(50);
        writeLock1.start();

        readLock1.join();
        writeLock1.join();
    }

    /**
     * 先写锁，再读锁。写锁应该等待读锁完成
     *
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testWriteRead() throws IOException, InterruptedException {
        ReadLockClient readLock1 = new ReadLockClient();
        WriteLockClient writeLock1 = new WriteLockClient();

        writeLock1.start();
        Thread.sleep(50);
        readLock1.start();

        writeLock1.join();
        readLock1.join();
    }

    /** 测试多个随机读写锁 */
    @Test
    public void testRandomReadWriteLock() throws IOException, InterruptedException {
        int threadCount = 20;
        Thread[] lockThreads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            // 一定概率是写锁
            boolean writeLock = random.nextInt(5) == 0;
            if (writeLock) {
                lockThreads[i] = new WriteLockClient();
            } else {
                lockThreads[i] = new ReadLockClient();
            }
            lockThreads[i].start();
        }

        for (int i = 0; i < threadCount; i++) {
            lockThreads[i].join();
        }
    }

    /**
     * 写锁线程
     */
    class WriteLockClient extends Thread {
        ZooKeeperWriteLock writeLock;

        public WriteLockClient() {
            try {
                this.writeLock = new ZooKeeperWriteLock(address);
            } catch (IOException e) {
                log.error("IOException", e);
            }
        }

        @Override
        public void run() {
            writeLock.lock(guidNodeName, this.getName());
            try {
                Thread.sleep(1000 + random.nextInt(20) * 100);
            } catch (InterruptedException e) {
            }
            writeLock.release(guidNodeName, this.getName());
        }
    }

    /**
     * 读锁线程
     */
    class ReadLockClient extends Thread {
        ZooKeeperReadLock readLock;

        public ReadLockClient() {
            try {
                this.readLock = new ZooKeeperReadLock(address);
            } catch (IOException e) {
                log.error("IOException", e);
            }
        }

        @Override
        public void run() {
            readLock.lock(guidNodeName, this.getName());
            try {
                Thread.sleep(1000 + random.nextInt(20) * 100);
            } catch (InterruptedException e) {
            }
            readLock.release(guidNodeName, this.getName());
            try {
                readLock.getZooKeeper().close();
            } catch (InterruptedException e) {
            }
        }
    }
}
