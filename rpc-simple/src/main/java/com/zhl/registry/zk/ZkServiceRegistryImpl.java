package com.zhl.registry.zk;

import com.zhl.registry.ServiceRegistry;
import com.zhl.registry.zk.util.CuratorUtils;
import org.apache.curator.framework.CuratorFramework;

import java.net.InetAddress;
import java.net.InetSocketAddress;

/**
 * <p>
 *  Zookeeper 实现服务注册
 * @author zhl
 * @since 2024-07-14 9:48
 */
public class ZkServiceRegistryImpl implements ServiceRegistry {
    @Override
    public void registryService(String rpcServiceName, InetSocketAddress inetSocketAddress) {
        // 实际上向 注册中心注册的服务是 /my-rpc/rpcServiceName(interfaceName + group + version)/ip:port
        // 注意 /my-rpc/rpcServiceName(interfaceName + group + version) 可能有多个节点，还要选出一个 ip:port
        String path = CuratorUtils.ZK_REGISTER_ROOT_PATH + "/" + rpcServiceName + inetSocketAddress.toString();
        CuratorFramework zkClient = CuratorUtils.getZkClient();
        CuratorUtils.createEphemeralNode(zkClient, path);
    }
}
