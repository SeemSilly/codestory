package tech.codestory.zookeeper.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.codestory.zookeeper.ZooKeeperBase;
import java.io.IOException;
import java.util.List;

/**
 * @author junyongliao
 * @date 2019/8/19
 * @since 1.0.0
 */
public abstract class ChildrenNodeLock extends ZooKeeperBase implements ZooKeeperLock {
    private Logger log;

    /** 用于加锁的唯一节点名 */
    protected String guidNodeName;
    /** 子节点的前缀 */
    protected String childPrefix = "element";
    /** 用于记录所创建子节点的完整路径 */
    protected String elementNodeFullName;

    public ChildrenNodeLock(String address) throws IOException {
        super(address);
        log = LoggerFactory.getLogger(getClass().getName());
    }

    /**
     * 获取当前节点的前一个节点，如果为空表示自己是第一个
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected String getPrevElementName() throws KeeperException, InterruptedException {
        List<String> elementNames = getZooKeeper().getChildren(this.guidNodeName, false);
        long curElementSerial = Long.valueOf(
                elementNodeFullName.substring((this.guidNodeName + "/" + childPrefix).length()));
        String prevElementName = null;
        long prevElementSerial = -1;
        for (String oneElementName : elementNames) {
            long oneElementSerial = Long.parseLong(oneElementName.substring(childPrefix.length()));
            if (oneElementSerial < curElementSerial) {
                // 比当前节点小
                if (oneElementSerial > prevElementSerial) {
                    prevElementSerial = oneElementSerial;
                    prevElementName = oneElementName;
                }
            }
        }
        return prevElementName;
    }

    /**
     * 尝试获取锁
     *
     * @param guidNodeName 用于加锁的唯一节点名
     * @param clientGuid 用于唯一标识当前客户端的ID
     * @return
     */
    @Override
    public boolean lock(String guidNodeName, String clientGuid) {
        boolean result = false;
        this.guidNodeName = guidNodeName;

        // 确保根节点存在，并且创建为容器节点
        super.createRootNode(this.guidNodeName, CreateMode.CONTAINER);

        try {
            // 创建子节点并返回带序列号的节点名
            elementNodeFullName = getZooKeeper().create(this.guidNodeName + "/" + childPrefix,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

            boolean lockSuccess = isLockSuccess();
            result = lockSuccess;
        } catch (KeeperException e) {
            log.info("获取分布式锁失败", e);
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
        return result;
    }


    /**
     * 释放锁
     *
     * @param guidNodeName 用于加锁的唯一节点名
     * @param clientGuid 用于唯一标识当前客户端的ID
     * @return
     */
    @Override
    public boolean release(String guidNodeName, String clientGuid) {
        boolean result = true;
        try {
            // 删除子节点
            getZooKeeper().delete(elementNodeFullName, 0);
        } catch (KeeperException e) {
            result = false;
            log.error("KeeperException", e);
        } catch (InterruptedException e) {
            result = false;
            log.error("InterruptedException", e);
        }
        return result;
    }

    /**
     * 是否加锁成功
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected abstract boolean isLockSuccess() throws KeeperException, InterruptedException;
}
