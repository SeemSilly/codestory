package tech.codestory.zookeeper.lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.codestory.zookeeper.ZooKeeperBase;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 基于子节点实现的分布式锁基类<br>
 * 为了方便排序，子节点的前缀应该有相同的字符串长度
 *
 * @author javacodestory@gmail.com
 * @date 2019/8/19
 */
public abstract class ChildrenNodeLock extends ZooKeeperBase implements ZooKeeperLock {
    private Logger log;

    /** 用于加锁的唯一节点名 */
    protected String guidNodeName;
    /** 子节点的前缀 */
    protected String childPrefix = "element";

    /** 用于记录所创建子节点的路径 */
    protected String elementNodeName;

    /** 用于记录所创建子节点的完整路径 */
    protected String elementNodeFullName;

    public ChildrenNodeLock(String address) throws IOException {
        super(address);
        log = LoggerFactory.getLogger(getClass().getName());
    }

    /**
     * 获取排好序的子节点列表
     *
     * @param path
     * @param watch
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    final public List<String> getOrderedChildren(String path, boolean watch)
            throws KeeperException, InterruptedException {
        List<String> children = getZooKeeper().getChildren(path, watch);
        Collections.sort(children, new StringCompare());
        return children;
    }

    /**
     * 在日志中输出子节点
     *
     * @param path
     * @param children
     */
    final public void traceOrderedChildren(String path, List<String> children) {
        if (log.isTraceEnabled()) {
            log.trace("children : {}", StringUtils.joinStrings(children, ","));
        }
    }

    /**
     * 在日志中输出子节点
     *
     * @param path
     * @param children
     */
    final public void infoOrderedChildren(String path, List<String> children) {
        if (log.isInfoEnabled()) {
            log.info("children : {}", StringUtils.joinStrings(children, ","));
        }
    }

    /**
     * 获取当前节点的前一个节点，如果为空表示自己是第一个
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected String getPrevElementName() throws KeeperException, InterruptedException {
        List<String> elementNames = getOrderedChildren(this.guidNodeName, false);
        traceOrderedChildren(this.guidNodeName, elementNames);

        String prevElementName = null;
        for (String oneElementName : elementNames) {
            if (this.elementNodeFullName.endsWith(oneElementName)) {
                // 已经到了当前节点
                break;
            }
            prevElementName = oneElementName;
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
            String fullNodeName = this.guidNodeName + "/" + getChildPrefix();
            byte[] nodeValue = clientGuid == null ? new byte[0] : clientGuid.getBytes();
            elementNodeFullName = getZooKeeper().create(fullNodeName, nodeValue,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            elementNodeName = elementNodeFullName.substring(guidNodeName.length() + 1);

            log.trace("{} 尝试获取锁", elementNodeName);

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
     * 锁是否已经存在，容器节点存在，并且有子节点，则说明锁已经存在
     *
     * @param guidNodeName 用于加锁的唯一节点名
     * @return
     */
    @Override
    public boolean exists(String guidNodeName) {
        boolean exists = false;
        Stat stat = new Stat();
        try {
            getZooKeeper().getData(guidNodeName, false, stat);
            if (stat.getNumChildren() > 0) {
                exists = true;
            }
        } catch (KeeperException.NoNodeException e) {
            exists = false;
        } catch (KeeperException e) {
            log.error("KeeperException", e);
        } catch (InterruptedException e) {
            log.error("InterruptedException", e);
        }
        return exists;
    }

    /**
     * 子节点的前缀，子类可以重载
     * 
     * @return
     */
    protected String getChildPrefix() {
        return childPrefix;
    }

    /**
     * 是否加锁成功
     *
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    protected abstract boolean isLockSuccess() throws KeeperException, InterruptedException;

    /**
     * 子节点名称比较，取最后10位进行比较
     */
    private class StringCompare implements Comparator<String> {
        @Override
        public int compare(String string1, String string2) {
            return string1.substring(string1.length() - 10)
                    .compareTo(string2.substring(string2.length() - 10));
        }
    }
}
