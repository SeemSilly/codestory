package tech.codestory.zookeeper.watcher;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;

import lombok.extern.slf4j.Slf4j;
import tech.codestory.zookeeper.ZooKeeperBase;

/**
 * 用于测试 ZooKeeper的 WatchedEvent用法
 * 
 * @author code story
 * @date 2019/8/13
 * @since 1.0.0
 */
@Slf4j
public class ZooKeeperWatcher extends ZooKeeperBase implements Runnable {
    /** 退出系统的信号 */
    static Integer quitSemaphore = Integer.valueOf(-1);
    String zNode;

    public ZooKeeperWatcher(String address, String zNode) throws KeeperException, IOException {
        super(address);

        this.zNode = zNode;

        // 先读当前的数据
        readNodeData();
    }

    @Override
    protected void processNodeCreated(WatchedEvent event) {
        String path = event.getPath();
        if (path != null && path.equals(zNode)) {
            // 创建节点
            readNodeData();
        }
    }

    @Override
    protected void processNodeDataChanged(WatchedEvent event) {
        String path = event.getPath();
        if (path != null && path.equals(zNode)) {
            // 节点数据被修改
            readNodeData();
        }
    }

    @Override
    protected void processNodeDeleted(WatchedEvent event) {
        String path = event.getPath();
        if (path != null && path.equals(zNode)) {
            synchronized (quitSemaphore) {
                // 节点被删除，通知退出线程
                quitSemaphore.notify();
            }
        }
    }

    /** 读节点数据 */
    private void readNodeData() {
        try {
            Stat stat = new Stat();
            byte[] data = getZooKeeper().getData(zNode, true, stat);
            if (data != null) {
                log.info("{}, value={}, version={}", zNode, new String(data), stat.getVersion());
            }
        } catch (KeeperException e) {
            log.info("{} 不存在", zNode);
            try {
                // 目的是添加Watcher
                getZooKeeper().exists(zNode, true);
            } catch (KeeperException ex) {
            } catch (InterruptedException ex) {
            }
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
    }

    @Override
    public void run() {
        try {
            synchronized (quitSemaphore) {
                quitSemaphore.wait();
                log.info("{} 被删除，退出", zNode);
            }
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
    }
}
