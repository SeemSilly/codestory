package tech.codestory.zookeeper.barrier;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;

import lombok.extern.slf4j.Slf4j;
import tech.codestory.zookeeper.ZooKeeperBase;

/**
 * @author junyongliao
 * @date 2019/8/13
 * @since 1.0.0
 */
@Slf4j
public class ZooKeeperBarrier extends ZooKeeperBase {
    /** 子节点发生变化的信号 static */
    Integer mutex = Integer.valueOf(-1);
    /** 避免重复构建餐桌 */
    static Integer tableSerialInitial = Integer.valueOf(1);

    /** 餐桌容量 */
    int tableCapacity;
    /** 餐桌编号 */
    String tableSerial;

    /** 客人姓名 */
    String customerName;

    /**
     * 构造函数，用于创建zk客户端，以及记录记录barrier的名称和容量
     * 
     * @param address ZooKeeper服务器地址
     * @param tableSerial 餐桌编号
     * @param tableCapacity 餐桌容量
     * @param customerName 客人姓名
     */
    ZooKeeperBarrier(String address, String tableSerial, int tableCapacity, String customerName)
            throws IOException {
        super(address);

        this.tableSerial = createRootNode(tableSerial);
        this.tableCapacity = tableCapacity;
        this.customerName = customerName;
    }

    @Override
    protected void processNodeChildrenChanged(WatchedEvent event) {
        log.info("{} 接收到了通知 : {}", customerName, event.getType());
        // 子节点有变化
        synchronized (mutex) {
            mutex.notify();
        }
    }

    /**
     * 客人坐在饭桌上
     *
     * @return 当等到餐桌坐满时返回 true
     * @throws KeeperException
     * @throws InterruptedException
     */
    boolean enter() throws KeeperException, InterruptedException {
        String nodeName = tableSerial + "/" + customerName;
        log.info("{}: 自己坐下来 {}", customerName, nodeName);
        // 属于客人自己的节点，如果会话结束没删掉会自动删除
        getZooKeeper().create(nodeName, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        while (true) {
            synchronized (mutex) {
                // 读出子节点列表，并继续监听
                List<String> list = getZooKeeper().getChildren(tableSerial, true);
                if (list.size() < tableCapacity) {
                    log.info("{}: 当前人数 = {} , 总人数 = {}, 人还不够: 吃饭不积极，一定有问题...", customerName,
                            list.size(), tableCapacity);
                    mutex.wait();
                } else {
                    log.info("{}: 人终于够了，开饭...", customerName);
                    return true;
                }
            }
        }
    }

    /**
     * 客人吃完饭了，可以离开
     *
     * @return 所有客人都吃完，再返回true
     * @throws KeeperException
     * @throws InterruptedException
     */
    boolean leave() throws KeeperException, InterruptedException {
        String nodeName = tableSerial + "/" + customerName;
        log.info("{}: 已经吃完，准备离席，删除节点 {}", customerName, nodeName);
        getZooKeeper().delete(nodeName, 0);
        while (true) {
            // 读出子节点列表，并继续监听
            List<String> list = getZooKeeper().getChildren(tableSerial, true);
            if (list.size() > 0) {
                log.info("{}: 还有 {} 人没吃完，你们吃快点...", customerName, list.size());
                synchronized (mutex) {
                    mutex.wait();
                }
            } else {
                log.info("{}: 所有人都吃完了，准备散伙", customerName);
                return true;
            }
        }
    }
}
