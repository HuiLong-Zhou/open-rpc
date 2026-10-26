package com.zhl.registry.zk;

import com.zhl.enums.LoadBalanceEnum;
import com.zhl.enums.RpcErrorMessageEnum;
import com.zhl.exception.RpcException;
import com.zhl.extensions.ExtensionLoader;
import com.zhl.loadbalance.LoadBalance;
import com.zhl.registry.ServiceDiscovery;
import com.zhl.registry.zk.util.CuratorUtils;
import com.zhl.remoting.dto.RpcRequest;
import com.zhl.utils.CollectionUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;

/**
 * <p>
 *
 * @author zhl
 * @since 2024-07-14 9:57
 */
@Slf4j
public class ZkServiceDiscoveryImpl implements ServiceDiscovery {

    private final LoadBalance loadBalance;

    public ZkServiceDiscoveryImpl() {
        loadBalance= ExtensionLoader.getExtensionLoader(LoadBalance.class).getExtension(LoadBalanceEnum.LOAD_BALANCE.getName());
    }

    @Override
    public InetSocketAddress lookupService(RpcRequest rpcRequest) {
        String rpcServiceName = rpcRequest.getRpcServiceName();
        CuratorFramework zkClient = CuratorUtils.getZkClient();
        // 使用 Zookeeper 获取 当前服务名下的所有服务节点，并注册 Watcher
        List<String> serviceAddresses = CuratorUtils.getChildrenNodes(zkClient, rpcServiceName);
        if (CollectionUtil.isEmpty(serviceAddresses)) {
            throw new RpcException(RpcErrorMessageEnum.SERVICE_CAN_NOT_BE_FOUND, rpcServiceName);
        }
        String serviceAddress = loadBalance.selectServiceAddress(serviceAddresses, rpcRequest);
        log.info("Successfully found the service address:[{}]", serviceAddress);
        String[] address = serviceAddress.split(":");
        String host = address[0];
        int port = Integer.parseInt(address[1]);
        return new InetSocketAddress(host, port);
    }
}
