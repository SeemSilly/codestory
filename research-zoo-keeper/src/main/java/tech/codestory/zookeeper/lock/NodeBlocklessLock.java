package tech.codestory.zookeeper.lock;

import java.io.IOException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.data.Stat;
import tech.codestory.zookeeper.ZooKeeperBase;

/**
 * ZooKeeper中基于节点本身加锁，类似redis的实现<br>
 * 用于 非阻塞锁 ： 一旦加锁失败则放弃
 * 
 * @author code story
 * @date 2019/8/19
 */
@Slf4j
public class NodeBlocklessLock extends ZooKeeperBase implements ZooKeeperLock {
    /** 用于加锁的唯一节点名 */
    String guidNodeName;
    /** 用于唯一标识当前客户端的ID */
    String clientGuid;

    public NodeBlocklessLock(String address) throws IOException {
        super(address);
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
        this.clientGuid = clientGuid;

        try {
            if (getZooKeeper().exists(guidNodeName, false) == null) {
                getZooKeeper().create(guidNodeName, clientGuid.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                byte[] data = getZooKeeper().getData(guidNodeName, false, null);
                if (data != null && clientGuid.equals(new String(data))) {
                    result = true;
                } else {
                    log.info("创建node成功，但值不是自己添加的，理论上不应该出现这种情况");
                }
            }
        } catch (KeeperException.NodeExistsException e) {
            // 节点已经存在，说明自己失败，什么都不做，直接返回 false
        } catch (KeeperException e) {
            log.error("获取分布式锁失败", e);
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
        boolean result = false;
        Stat stat = new Stat();
        try {
            byte[] data = getZooKeeper().getData(guidNodeName, false, stat);
            if (data != null && clientGuid.equals(new String(data))) {
                getZooKeeper().delete(guidNodeName, stat.getVersion());
                result = true;
            }
        } catch (KeeperException e) {
            log.error("KeeperException", e);
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
        return result;
    }

    /**
     * 锁是否已经存在
     *
     * @param guidNodeName 用于加锁的唯一节点名
     * @return
     */
    @Override
    public boolean exists(String guidNodeName) {
        boolean result = false;
        try {
            Stat stat = getZooKeeper().exists(guidNodeName, false);
            result = stat != null;
        } catch (KeeperException e) {
            log.error("KeeperException", e);
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
        return result;
    }
}
