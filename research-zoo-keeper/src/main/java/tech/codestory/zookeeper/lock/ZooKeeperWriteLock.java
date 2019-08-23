package tech.codestory.zookeeper.lock;

import java.io.IOException;
import org.apache.zookeeper.KeeperException;
import lombok.extern.slf4j.Slf4j;

/**
 * 基于子节点阻塞锁实现的写锁
 * 
 * @author junyongliao
 * @date 2019/8/21
 */
@Slf4j
public class ZooKeeperWriteLock extends ChildrenBlockingLock {
    /** 写锁的标记 */
    public static final String FLAG = "w-lock-";

    public ZooKeeperWriteLock(String address) throws IOException {
        super(address);
    }

    /**
     * 获取前一个锁。因为写锁前面不能有任何锁，所以直接继承 ZooKeeperWriteLock 的实现。
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    protected String getPrevElementName() throws KeeperException, InterruptedException {
        return super.getPrevElementName();
    }

    /**
     * 返回写锁的前缀
     * 
     * @return
     */
    @Override
    protected String getChildPrefix() {
        return FLAG;
    }

    @Override
    public boolean lock(String guidNodeName, String clientGuid) {
        boolean success = super.lock(guidNodeName, clientGuid);
        log.info("{} get write lock : {}", elementNodeName, success);
        return success;
    }

    @Override
    public boolean release(String guidNodeName, String clientGuid) {
        log.info("{} release write lock", elementNodeName);
        boolean success = super.release(guidNodeName, clientGuid);
        return success;
    }
}
