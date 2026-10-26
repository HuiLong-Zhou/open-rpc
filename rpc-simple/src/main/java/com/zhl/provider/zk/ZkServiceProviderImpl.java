package com.zhl.provider.zk;

import com.esotericsoftware.minlog.Log;
import com.zhl.config.RpcServiceConfig;
import com.zhl.enums.RpcErrorMessageEnum;
import com.zhl.enums.ServiceRegistryEnum;
import com.zhl.exception.RpcException;
import com.zhl.extensions.ExtensionLoader;
import com.zhl.provider.ServiceProvider;
import com.zhl.registry.ServiceDiscovery;
import com.zhl.registry.ServiceRegistry;
import com.zhl.registry.zk.ZkServiceDiscoveryImpl;
import com.zhl.registry.zk.ZkServiceRegistryImpl;
import com.zhl.remoting.constants.RpcServiceConstant;
import io.protostuff.Rpc;
import lombok.extern.slf4j.Slf4j;

import javax.xml.ws.WebServiceRefs;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>
 *  使用 zookeeper 实现服务的提供
 * @author zhl
 * @since 2024-07-15 14:58
 */
@Slf4j
public class ZkServiceProviderImpl implements ServiceProvider {
    /**
     * 服务注册
     */
    private final ServiceRegistry serviceRegistry;
    /**
     * 存储服务缓存
     * key:rpcService
     * value:Service
     */
    private final Map<String, Object> serviceMap;
    /**
     * 存储已注册的服务
     */
    private final Set<String> registeredService;

    public ZkServiceProviderImpl() {
        registeredService = ConcurrentHashMap.newKeySet();
        serviceMap = new ConcurrentHashMap<>();
        serviceRegistry = ExtensionLoader.getExtensionLoader(ServiceRegistry.class).getExtension(ServiceRegistryEnum.ZK.getName());
    }

    @Override
    public void addService(RpcServiceConfig rpcServiceConfig) {
        String rpcServiceName = rpcServiceConfig.getRpcServiceName();
        if (registeredService.contains(rpcServiceName)) {
            return;
        }
        registeredService.add(rpcServiceName);
        serviceMap.put(rpcServiceName, rpcServiceConfig.getService());
        log.info("Add service: {} and interfaces:{}", rpcServiceName, rpcServiceConfig.getService().getClass().getInterfaces());
    }

    @Override
    public Object getService(String rpcServiceName) {
        Object service = serviceMap.get(rpcServiceName);
        if(service==null) {
            throw new RpcException(RpcErrorMessageEnum.SERVICE_CAN_NOT_BE_FOUND);
        }
        return service;
    }

    @Override
    public void publishService(RpcServiceConfig rpcServiceConfig) {
        try {
            String host = InetAddress.getLocalHost().getHostAddress();
            this.addService(rpcServiceConfig);
            serviceRegistry.registryService(rpcServiceConfig.getRpcServiceName(), new InetSocketAddress(host, RpcServiceConstant.SERVICE_PORT));
        } catch (UnknownHostException e) {
            log.error("occur exception when getHostAddress", e);
        }
    }
}
