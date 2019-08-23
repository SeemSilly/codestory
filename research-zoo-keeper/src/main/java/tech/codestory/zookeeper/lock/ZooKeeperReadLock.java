package tech.codestory.zookeeper.lock;

import java.io.IOException;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;

/**
 * ZooKeeper实现读锁。<br>
 * 读锁的前面没有任何写锁，就是获取锁成功。
 * 
 * @author javacodestory@gmail.com
 * @date 2019/8/21
 */
@Slf4j
public class ZooKeeperReadLock extends ChildrenBlockingLock {
    /** 读锁的标记 */
    public static final String FLAG = "r-lock-";

    public ZooKeeperReadLock(String address) throws IOException {
        super(address);
    }

    /**
     * 读取前一个写锁
     * 
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    protected String getPrevElementName() throws KeeperException, InterruptedException {
        List<String> elementNames = super.getOrderedChildren(this.guidNodeName, false);
        super.traceOrderedChildren(this.guidNodeName, elementNames);
        String prevWriteElementName = null;
        for (String oneElementName : elementNames) {
            if (this.elementNodeFullName.endsWith(oneElementName)) {
                // 已经到了当前节点
                break;
            }
            if (isWriteLock(oneElementName)) {
                prevWriteElementName = oneElementName;
            }
        }
        return prevWriteElementName;
    }

    /**
     * 返回读锁的前缀
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
        log.info("{} get read lock : {}", elementNodeName, success);
        return success;
    }

    @Override
    public boolean release(String guidNodeName, String clientGuid) {
        log.info("{} release read lock", elementNodeName);
        boolean success = super.release(guidNodeName, clientGuid);
        return success;
    }

    /**
     * 是写锁
     * 
     * @param elementName
     * @return
     */
    private boolean isWriteLock(String elementName) {
        boolean writeLock = elementName.startsWith(ZooKeeperWriteLock.FLAG);
        return writeLock;
    }

    /**
     * 是读锁
     *
     * @param elementName
     * @return
     */
    private boolean isReadLock(String elementName) {
        boolean readLock = elementName.startsWith(ZooKeeperReadLock.FLAG);
        return readLock;
    }
}
