package tech.codestory.zookeeper.lock;

import java.io.IOException;
import java.util.List;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import lombok.extern.slf4j.Slf4j;
import tech.codestory.zookeeper.ZooKeeperBase;

/**
 * ZooKeeper中基于子节点功能加锁<br>
 * 用于 非阻塞锁 ： 一旦加锁失败则放弃
 * 
 * @author code story
 * @date 2019/8/19
 */
@Slf4j
public class ChildrenBlocklessLock extends ChildrenNodeLock {
    public ChildrenBlocklessLock(String address) throws IOException {
        super(address);
    }

    /**
     * 是否加锁成功
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    protected boolean isLockSuccess() throws KeeperException, InterruptedException {
        boolean lockSuccess = false;
        String prevElementName = getPrevElementName();
        if (prevElementName != null) {
            // 有更小的节点，说明当前节点没抢到锁，删掉自己并退出
            getZooKeeper().delete(elementNodeFullName, 0);
        } else {
            lockSuccess = true;
        }
        return lockSuccess;
    }
}
