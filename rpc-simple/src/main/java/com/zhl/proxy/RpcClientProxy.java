package com.zhl.proxy;

import com.zhl.config.RpcServiceConfig;
import com.zhl.enums.RpcErrorMessageEnum;
import com.zhl.enums.RpcResponseCodeEnum;
import com.zhl.exception.RpcException;
import com.zhl.registry.ServiceDiscovery;
import com.zhl.remoting.dto.RpcRequest;
import com.zhl.remoting.dto.RpcResponse;
import com.zhl.remoting.transport.RpcRequestTransport;
import com.zhl.remoting.transport.netty.client.NettyRpcClient;
import com.zhl.remoting.transport.socket.SocketRpcClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * <p>
 *  动态代理类。动态代理对象在调用方法时，其实就是调用下面的invoke方法。
 *  正是因为动态代理，客户端调用远程方法就像调用本地方法一样（中间过程被屏蔽了）
 * @author zhl
 * @since 2024-07-16 9:39
 */
@Slf4j
public class RpcClientProxy implements InvocationHandler {
    private static final String INTERFACE_NAME = "interfaceName";
    /**
     * 客户端发送信息的接口，具体实现可以是 Socket 或 Netty
     */
    private final RpcRequestTransport rpcRequestTransport;
    /**
     * 获取服务的 rpcServiceName 和 目标服务
     */
    private final RpcServiceConfig rpcServiceConfig;

    public RpcClientProxy(RpcRequestTransport rpcRequestTransport, RpcServiceConfig rpcServiceConfig) {
        this.rpcRequestTransport = rpcRequestTransport;
        this.rpcServiceConfig = rpcServiceConfig;
    }

    public RpcClientProxy(RpcRequestTransport rpcRequestTransport) {
        this.rpcRequestTransport = rpcRequestTransport;
        this.rpcServiceConfig=new RpcServiceConfig();
    }

    public <T>T getProxy(Class<T> clazz) {
        // this ===> 重写 InvocationHandler 的 invoke 方法
        return (T) Proxy.newProxyInstance(clazz.getClassLoader(), new Class<?>[]{clazz}, this);
    }


    @SneakyThrows
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
        log.info("invoked method: [{}]", method.getName());
        RpcRequest rpcRequest = RpcRequest.builder()
            .methodName(method.getName())
            .requestId(UUID.randomUUID().toString())
            .group(rpcServiceConfig.getGroup())
            .version(rpcServiceConfig.getVersion())
            .interfaceName(method.getDeclaringClass().getName())
            .parameters(args)
            .paramTypes(method.getParameterTypes())
            .build();
        RpcResponse<Object> rpcResponse = null;
        if(rpcRequestTransport instanceof NettyRpcClient) {
            // 使用 CompletableFuture 进行优化，异步处理
            CompletableFuture<RpcResponse<Object>> completableFuture =
                (CompletableFuture<RpcResponse<Object>>) rpcRequestTransport.sendRpcRequest(rpcRequest);
            // 同步阻塞接受结果
            rpcResponse = completableFuture.get();
        }
        if (rpcRequestTransport instanceof SocketRpcClient) {
            rpcResponse = (RpcResponse<Object>) rpcRequestTransport.sendRpcRequest(rpcRequest);
            log.info("rpcResponse={}", rpcResponse);
        }
        this.check(rpcResponse, rpcRequest);
        return rpcResponse.getData();
    }

    private void check(RpcResponse<Object> rpcResponse, RpcRequest rpcRequest) {
        if (rpcResponse == null) {
            throw new RpcException(RpcErrorMessageEnum.SERVICE_INVOCATION_FAILURE, INTERFACE_NAME + ":" + rpcRequest.getInterfaceName());
        }

        if (!rpcRequest.getRequestId().equals(rpcResponse.getRequestId())) {
            throw new RpcException(RpcErrorMessageEnum.REQUEST_NOT_MATCH_RESPONSE, INTERFACE_NAME + ":" + rpcRequest.getInterfaceName());
        }

        if (rpcResponse.getCode() == null || !rpcResponse.getCode().equals(RpcResponseCodeEnum.SUCCESS.getCode())) {
            throw new RpcException(RpcErrorMessageEnum.SERVICE_INVOCATION_FAILURE, INTERFACE_NAME + ":" + rpcRequest.getInterfaceName());
        }
    }

}
