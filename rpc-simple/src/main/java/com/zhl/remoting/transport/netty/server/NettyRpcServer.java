package com.zhl.remoting.transport.netty.server;

import com.zhl.config.CustomShutdownHook;
import com.zhl.config.RpcServiceConfig;
import com.zhl.factory.SingletonFactory;
import com.zhl.provider.ServiceProvider;
import com.zhl.provider.zk.ZkServiceProviderImpl;
import com.zhl.remoting.constants.RpcServiceConstant;
import com.zhl.remoting.transport.netty.codec.RpcMessageDecoder;
import com.zhl.remoting.transport.netty.codec.RpcMessageEncoder;
import com.zhl.remoting.transport.netty.handler.NettyRpcServerHandler;
import com.zhl.utils.concurrent.threadpool.RuntimeUtil;
import com.zhl.utils.concurrent.threadpool.ThreadPoolFactoryUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.concurrent.TimeUnit;
/**
 * <p>
 * 基于 Netty 的RpcServer
 *  将 NettyRpcServer 交给 Spring 管理
 * @author zhl
 * @since 2024-07-16 16:43
 */
@Slf4j
@Component
public class NettyRpcServer {
    private final ServiceProvider serviceProvider = SingletonFactory.getInstance(ZkServiceProviderImpl.class);
    /**
     * 初始化执行器组  网络 I/O 操作较多，线程数为 CPU * 2
     */
    private final EventExecutorGroup serviceHandlerGroup = new DefaultEventExecutorGroup(
        RuntimeUtil.cpus() * 2,
        ThreadPoolFactoryUtil.createThreadFactory("service-handler-group", false)
    );

    public void registerService(RpcServiceConfig rpcServiceConfig) {
        serviceProvider.publishService(rpcServiceConfig);
    }

    @SneakyThrows
    public void start() {
        CustomShutdownHook.getCustomShutdownHook().clearAll();
        String host = InetAddress.getLocalHost().getHostAddress();
        ServerBootstrap serverBootstrap = new ServerBootstrap();
        NioEventLoopGroup boss = new NioEventLoopGroup(1);
        NioEventLoopGroup worker = new NioEventLoopGroup();
        try {
            serverBootstrap
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                // 启用 TCP_NODELAY，关闭 Nagle 算法，降低延迟
                .childOption(ChannelOption.TCP_NODELAY, true)
                // 开启 TCP 心跳机制 --> TCP 保活机制
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                //表示系统用于临时存放已完成三次握手的请求的队列的最大长度,如果连接建立频繁，服务器处理创建新连接较慢，可以适当调大这个参数
                .option(ChannelOption.SO_BACKLOG, 128)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ch.pipeline()
                            .addLast(new IdleStateHandler(30, 0, 0, TimeUnit.SECONDS))
                            .addLast(new RpcMessageDecoder())
                            .addLast(new RpcMessageEncoder())
                            .addLast(serviceHandlerGroup, new NettyRpcServerHandler());
                    }
                });
            // 绑定端口，同步等待
            Channel channel = serverBootstrap.bind(host, RpcServiceConstant.SERVICE_PORT).sync().channel();
            // 等待服务端监听端口关闭
            channel.closeFuture().sync();
        } catch (InterruptedException e) {
            log.error("occur exception when start server:", e);
        } finally {
            log.error("shutdown bossGroup and workerGroup");
            boss.shutdownGracefully();
            worker.shutdownGracefully();
            serviceHandlerGroup.shutdownGracefully();
        }
    }
}
