package com.zhl.remoting.transport.netty.handler;

import com.zhl.enums.CompressEnum;
import com.zhl.enums.RpcResponseCodeEnum;
import com.zhl.enums.SerializationTypeEnum;
import com.zhl.factory.SingletonFactory;
import com.zhl.remoting.constants.RpcConstants;
import com.zhl.remoting.dto.RpcMessage;
import com.zhl.remoting.dto.RpcRequest;
import com.zhl.remoting.dto.RpcResponse;
import com.zhl.remoting.handler.RpcRequestHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;

/**
 * <p>
 *  接收客户端发送过来的消息，并返回消息给客户端
 * @author zhl
 * @since 2024-07-18 12:52
 */
@Slf4j
public class NettyRpcServerHandler extends ChannelInboundHandlerAdapter {
    private final RpcRequestHandler rpcRequestHandler = SingletonFactory.getInstance(RpcRequestHandler.class);

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        try {
            if (msg instanceof RpcMessage) {
                log.info("server receive msg: [{}] ", msg);
                // 根据消息类型处理对应的消息类型
                byte messageType = ((RpcMessage) msg).getMessageType();
                RpcMessage rpcMessage = new RpcMessage();
                rpcMessage.setCodec(SerializationTypeEnum.HESSIAN.getCode());
                rpcMessage.setCompress(CompressEnum.GZIP.getCode());
                // 客户端发送心跳数据
                if(messageType== RpcConstants.HEARTBEAT_REQUEST_TYPE) {
                    rpcMessage.setMessageType(RpcConstants.HEARTBEAT_RESPONSE_TYPE);
                    rpcMessage.setData(RpcConstants.PONG);
                }
                // 客户端发送正常请求
                if (messageType==RpcConstants.REQUEST_TYPE){
                    RpcRequest rpcRequest = (RpcRequest) ((RpcMessage) msg).getData();
                    // -----
                    Object result = rpcRequestHandler.handle(rpcRequest);
                    log.info(String.format("server get result: %s", result.toString()));
                    // 如果 channel 正常连接，且当前 I/O 线程正在执行写请求，返回对应的服务
                    if(ctx.channel().isActive() && ctx.channel().isWritable()) {
                        rpcMessage.setMessageType(RpcConstants.RESPONSE_TYPE);
                        RpcResponse<Object> rpcResponse = RpcResponse.success(result, rpcRequest.getRequestId());
                        rpcMessage.setData(rpcResponse);
                    }else {
                        RpcResponse<Object> rpcResponse = RpcResponse.fail(RpcResponseCodeEnum.FAIL);
                        rpcMessage.setData(rpcResponse);
                        log.error("not writable now, message dropped");
                    }
                }
                ctx.writeAndFlush(rpcMessage).addListener(ChannelFutureListener.CLOSE_ON_FAILURE);
            }
        } finally {
            ReferenceCountUtil.release(msg);
        }
    }

    /**
     * 触发了服务端的读事件（超过 30s）关闭 channel 连接
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            IdleState state = ((IdleStateEvent) evt).state();
            if(state==IdleState.READER_IDLE) {
                log.info("idle check happen, so close the connection");
                // 客户端超过 30s 未向服务端发送心跳(写事件)，则服务端直接关闭和客户端的连接
                ctx.close();
            } else {
                super.userEventTriggered(ctx, evt);
            }
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        log.error("server catch exception");
        cause.printStackTrace();
        ctx.close();
    }
}
