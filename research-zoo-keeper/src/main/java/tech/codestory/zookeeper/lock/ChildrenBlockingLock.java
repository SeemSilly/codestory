package tech.codestory.zookeeper.lock;

import java.io.IOException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import lombok.extern.slf4j.Slf4j;

/**
 * ZooKeeper中基于子节点功能加锁<br>
 * 用于 阻塞锁 ： 加锁失败会等待其他客户端释放锁
 * 
 * @author code story
 * @date 2019/8/19
 */
@Slf4j
public class ChildrenBlockingLock extends ChildrenNodeLock {
    /** 前一个节点被删除的信号 */
    static Integer mutex = Integer.valueOf(-1);

    public ChildrenBlockingLock(String address) throws IOException {
        super(address);
    }

    @Override
    protected void processNodeDeleted(WatchedEvent event) {
        synchronized (mutex) {
            // 节点被删除，通知退出线程
            mutex.notify();
        }
    }

    /**
     * 是否加锁成功
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    protected boolean isLockSuccess() {
        boolean lockSuccess;
        try {
            while (true) {
                String prevElementName = getPrevElementName();
                if (prevElementName == null) {
                    lockSuccess = true;
                    break;
                } else {
                    // 有更小的节点，说明当前节点没抢到锁，注册前一个节点的监听
                    getZooKeeper().exists(this.guidNodeName + "/" + prevElementName, true);
                    synchronized (mutex) {
                        mutex.wait();
                        log.info("{} 被删除，看看是不是轮到自己了", prevElementName);
                    }
                }
            }
        } catch (KeeperException e) {
            lockSuccess = false;
        } catch (InterruptedException e) {
            lockSuccess = false;
        }
        return lockSuccess;
    }
}
