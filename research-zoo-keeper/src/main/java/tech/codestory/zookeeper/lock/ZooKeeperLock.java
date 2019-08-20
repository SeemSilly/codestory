package tech.codestory.zookeeper.lock;

/**
 * @author junyongliao
 * @date 2019/8/19
 * @since 1.0.0
 */
public interface ZooKeeperLock {
    /**
     * 尝试获取锁
     *
     * @param guidNodeName 用于加锁的唯一节点名
     * @param clientGuid 用于唯一标识当前客户端的ID
     * @return
     */
    public boolean lock(String guidNodeName, String clientGuid);

    /**
     * 释放锁
     *
     * @param guidNodeName 用于加锁的唯一节点名
     * @param clientGuid 用于唯一标识当前客户端的ID
     * @return
     */
    public boolean release(String guidNodeName, String clientGuid);
}
