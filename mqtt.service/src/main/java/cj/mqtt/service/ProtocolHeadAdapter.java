package cj.mqtt.service;

import java.util.concurrent.TimeUnit;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;

public class ProtocolHeadAdapter extends ChannelInboundHandlerAdapter {
	// 1 GET 请求指定的页面信息，并返回实体主体。
	// 2 HEAD 类似于get请求，只不过返回的响应中没有具体的内容，用于获取报头
	// 3 POST
	// 向指定资源提交数据进行处理请求（例如提交表单或者上传文件）。数据被包含在请求体中。POST请求可能会导致新的资源的建立和/或已有资源的修改。
	// 4 PUT 从客户端向服务器传送的数据取代指定的文档的内容。
	// 5 DELETE 请求服务器删除指定的页面。
	// 6 CONNECT HTTP/1.1协议中预留给能够将连接改为管道方式的代理服务器。
	// 7 OPTIONS 允许客户端查看服务器的性能。
	// 8 TRACE 回显服务器收到的请求，主要用于测试或诊断。
	final static String[] http_head = { "GET ", "HEAD ", "POST ", "PUT ", "DELETE ", "CONNECT ", "OPTIONS ", "TRACE " };
	final static byte[] mqtt_head = { 0x10, 0x00, 0x00, 0x00, 'M', 'Q', 'T', 'T' };

	private MqttTopicTree mqttTopicTree = null;

	public ProtocolHeadAdapter(MqttTopicTree mqttTopicTree) {
		super();
		this.mqttTopicTree = mqttTopicTree;
	}

	void callDecode(ChannelHandlerContext ctx, ByteBuf in) {
		if (!in.isReadable())
			return;
		if (in.readableBytes() < 10) {
			return;
		}
		in.markReaderIndex();
		byte[] head = new byte[10];
		in.readBytes(head);
		in.resetReaderIndex();
		int i;
		int sum = 0;

		// mqtt
		for (i = 0; i < mqtt_head.length; i++) {
			if (mqtt_head[i] == head[i] || mqtt_head[i] == 0) {
				sum++;
			}
		}
		ChannelPipeline pipeline = ctx.pipeline();
		if (sum == mqtt_head.length) {
			pipeline.addFirst(MqttServiceHandler.PIPE_TIMEOUT_CHECK, new IdleStateHandler(5, 0, 0, TimeUnit.SECONDS));
			pipeline.addLast(MqttServiceHandler.PIPE_DECODE, new MqttDecoder());
			pipeline.addLast(MqttServiceHandler.PIPE_ENCODE, MqttEncoder.INSTANCE);
			pipeline.addLast(MqttServiceHandler.PIPE_HANDLE, new MqttServiceHandler(mqttTopicTree));
		} else {
			pipeline.addLast(new HttpServerCodec());
			pipeline.addLast(new HttpObjectAggregator(65536));
			pipeline.addLast(new ChunkedWriteHandler());
			pipeline.addLast(new HttpStaticFileServerHandler());
		}
		pipeline.remove(this);
	}

	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		if (msg instanceof ByteBuf) {
			try {
				ByteBuf data = (ByteBuf) msg;
				callDecode(ctx, data);
			} catch (DecoderException e) {
				throw e;
			} catch (Throwable t) {
				throw new DecoderException(t);
			} finally {

			}
		} else {
			ctx.fireChannelRead(msg);
		}
		super.channelRead(ctx, msg);
	}
}
