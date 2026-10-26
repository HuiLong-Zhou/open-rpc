package com.zhl.remoting.transport.socket;

import com.zhl.config.CustomShutdownHook;
import com.zhl.config.RpcServiceConfig;
import com.zhl.factory.SingletonFactory;
import com.zhl.provider.ServiceProvider;
import com.zhl.provider.zk.ZkServiceProviderImpl;
import com.zhl.registry.ServiceRegistry;
import com.zhl.registry.zk.ZkServiceRegistryImpl;
import com.zhl.remoting.constants.RpcServiceConstant;
import com.zhl.utils.concurrent.threadpool.ThreadPoolFactoryUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Predicate;

/**
 * <p>
 *  基于 Socket 的 rpc 服务端
 * @author zhl
 * @since 2024-07-15 16:06
 */
@Slf4j
public class SocketRpcServer {
    private final ServiceProvider serviceProvider;
    private final ExecutorService threadPool;


    public SocketRpcServer() {
        threadPool = ThreadPoolFactoryUtil.createCustomThreadPoolIfAbsent("socket-server-rpc-pool");
        serviceProvider = SingletonFactory.getInstance(ZkServiceProviderImpl.class);
    }

    public void registerService(RpcServiceConfig rpcServiceConfig) {
        serviceProvider.publishService(rpcServiceConfig);
    }

    public void start() {
        try(ServerSocket serverSocket = new ServerSocket()) {
            InetSocketAddress inetSocketAddress = new InetSocketAddress(InetAddress.getLocalHost().getHostAddress(), RpcServiceConstant.SERVICE_PORT);
            serverSocket.bind(inetSocketAddress);
            // 建立钩子方法，在服务端关闭时，关闭线程池资源清除 zookeeper 上注册的服务
            CustomShutdownHook.getCustomShutdownHook().clearAll();
            Socket socket;
            // 监听并建立和客户端的连接
            while ((socket=serverSocket.accept())!=null) {
                log.info("client connected [{}]", socket.getInetAddress());
                // 把真正处理 客户端的请求交给另一个线程 SocketRpcRequestHandlerRunnable.java 做
                threadPool.execute(new SocketRpcServerRunnable(socket));
            }
            threadPool.shutdown();
        } catch (IOException e) {
            log.error("occur IOException:", e);
        }
    }
}
