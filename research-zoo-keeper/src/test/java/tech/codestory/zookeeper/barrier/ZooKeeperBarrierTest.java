package tech.codestory.zookeeper.barrier;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.testng.annotations.Test;
import tech.codestory.zookeeper.TestBase;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import static org.testng.Assert.*;

/**
 * 测试 ZooKeeperBarrier
 * 
 * @author code story
 * @date 2019/8/15
 */
@Slf4j
public class ZooKeeperBarrierTest extends TestBase {
    Random random = new SecureRandom();

    @Test
    public void testBarrierTest() throws IOException {
        /** 等待连接建立成功的信号 */

        String barrierName = "/table-" + random.nextInt(10);
        int barrierSize = 4;

        CountDownLatch countDown = new CountDownLatch(barrierSize);
        String[] customerNames = {"张三", "李四", "王五", "赵六"};
        for (int i = 0; i < barrierSize; i++) {
            String customerName = customerNames[i];
            new Thread() {
                @Override
                public void run() {
                    log.info("{}: 准备吃饭", customerName);
                    ZooKeeperBarrier barrier = null;
                    try {
                        barrier = new ZooKeeperBarrier(address, barrierName, barrierSize,
                                customerName);

                        boolean flag = barrier.enter();
                        log.info("{}: 坐在了可以容纳 {} 人的饭桌", customerName, barrierSize);
                        if (!flag) {
                            log.info("{}: 想坐在饭桌时出错了", customerName);
                        }
                    } catch (KeeperException e) {
                        log.error("KeeperException", e);
                    } catch (InterruptedException e) {
                        log.error("InterruptedException", e);
                    } catch (IOException e) {
                        log.error("IOException", e);
                    }

                    // 假装在吃饭，随机时间
                    randomWait();

                    // 假装吃完了，离开barrier
                    try {
                        if (barrier != null) {
                            barrier.leave();
                        }
                    } catch (KeeperException e) {
                        log.error("KeeperException", e);
                    } catch (InterruptedException e) {
                        log.error("InterruptedException", e);
                    }
                    countDown.countDown();
                }
            }.start();

            // 等一会儿再开始下一个进程
            randomWait();
        }

        try {
            countDown.await();
            log.info("这一桌吃完了，散伙");
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
    }

    /** 随机等待 */
    private void randomWait() {
        int r = random.nextInt(100);
        for (int j = 0; j < r; j++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                log.error("InterruptedException", e);
            }
        }
    }
}
