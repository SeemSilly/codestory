package tech.codestory.zookeeper.queue;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.Test;
import lombok.extern.slf4j.Slf4j;
import tech.codestory.zookeeper.TestBase;

/**
 * ZooKeeperQueue测试
 * 
 * @author code story
 * @date 2019/8/16
 */
@Slf4j
public class ZooKeeperQueueTest extends TestBase {
    final String queueName = "/queue";
    final Random random = new SecureRandom();
    // 随机生成10-20之间的个数
    final int count = 10 + random.nextInt(10);

    int producerCount = 0;
    int consumerCount = 0;

    @Test
    public void testQueue() throws InterruptedException {
        log.info("开始ZooKeeper队列测试，本次将测试 {} 个数据", count);
        QueueProducer queueProducer = new QueueProducer();
        QueueConsumer queueConsumer = new QueueConsumer();
        queueProducer.start();
        queueConsumer.start();

        queueProducer.join();
        queueConsumer.join();

        assert producerCount == count;
        assert consumerCount == count;
    }

    /**
     * 队列的生产者
     */
    class QueueProducer extends Thread {
        @Override
        public void run() {
            try {
                ZooKeeperQueue queue = new ZooKeeperQueue(address, queueName);
                for (int i = 0; i < count; i++) {
                    int elementValue = i + 1;

                    long waitTime = random.nextInt(50) * 100;
                    log.info("生产对象 : {} , 然后等待 {} 毫秒", elementValue, waitTime);
                    queue.produce(elementValue);
                    producerCount++;
                    Thread.sleep(waitTime);
                }
            } catch (IOException e) {
                log.error("IOException", e);
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            } catch (KeeperException e) {
                log.error("KeeperException", e);
            }
        }
    }

    /**
     * 队列的消费者
     */
    class QueueConsumer extends Thread {
        @Override
        public void run() {
            try {
                ZooKeeperQueue queue = new ZooKeeperQueue(address, queueName);

                for (int i = 0; i < count; i++) {
                    try {
                        int elementValue = queue.consume();

                        long waitTime = random.nextInt(50) * 100;
                        log.info("消费对象 : {} , 然后等待 {} 毫秒", elementValue, waitTime);
                        Thread.sleep(waitTime);

                        consumerCount++;
                    } catch (KeeperException e) {
                        i--;
                        log.error("KeeperException", e);
                    } catch (InterruptedException e) {
                        log.error("InterruptedException", e);
                    }
                }
            } catch (IOException e) {
                log.error("IOException", e);
            }
        }
    }
}
