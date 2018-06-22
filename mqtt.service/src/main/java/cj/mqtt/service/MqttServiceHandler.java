package cj.mqtt.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.mqtt.MqttConnAckMessage;
import io.netty.handler.codec.mqtt.MqttConnAckVariableHeader;
import io.netty.handler.codec.mqtt.MqttConnectMessage;
import io.netty.handler.codec.mqtt.MqttConnectPayload;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttConnectVariableHeader;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.codec.mqtt.MqttMessageIdVariableHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPubAckMessage;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.handler.codec.mqtt.MqttSubAckMessage;
import io.netty.handler.codec.mqtt.MqttSubAckPayload;
import io.netty.handler.codec.mqtt.MqttSubscribeMessage;
import io.netty.handler.codec.mqtt.MqttSubscribePayload;
import io.netty.handler.codec.mqtt.MqttTopicSubscription;
import io.netty.handler.codec.mqtt.MqttUnsubAckMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribeMessage;
import io.netty.handler.codec.mqtt.MqttUnsubscribePayload;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

public class MqttServiceHandler extends SimpleChannelInboundHandler<MqttMessage> {
	private static Logger logger = LoggerFactory.getLogger(MqttServiceHandler.class);
	private MqttSession mqttSession = null;
	private MqttTopicTree mqttTopicTree = null;

	public MqttServiceHandler(MqttTopicTree mqttTopicTree) {
		super();
		this.mqttTopicTree = mqttTopicTree;
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
		cause.printStackTrace();
		ctx.close();
		System.out.println("--->exception:" + cause.getMessage());
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception { // (5)
		Channel channel = ctx.channel();
		logger.debug("Channel:" + channel.id() + channel.remoteAddress() + "在线");
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception { // (6)
		Channel channel = ctx.channel();
		logger.debug("Channel:" + channel.id() + "," + "掉线 " + mqttSession.toString());
		mqttTopicTree.close(mqttSession);
		mqttSession = null;
	}

	/**
	 * ctx.channel().pipeline().addFirst("TimeCheck", new IdleStateHandler(3, 0, 0, TimeUnit.SECONDS));
	 * 
	 * pipe添加了IdleStateHandler可以在userEventTriggered中判断超时
	 */
	@Override
	public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
		if (evt instanceof IdleStateEvent) {
			IdleStateEvent event = (IdleStateEvent) evt;
			if (event.state() == IdleState.READER_IDLE) {
				/* 读超时 */
				logger.debug("Channel: timeout");
				ctx.channel().close();
			} else if (event.state() == IdleState.WRITER_IDLE) {

			} else if (event.state() == IdleState.ALL_IDLE) {
				/* 总超时 */
			}
		}
		super.userEventTriggered(ctx, evt);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
		switch (msg.fixedHeader().messageType()) {// 消息类型判断
		case CONNECT:
			handlerConnect(ctx, msg);
			break;
		case CONNACK:
			break;
		case PUBLISH:
			handlerPublish(ctx, msg);
			break;
		case PUBACK:
			break;
		case PUBREC:
			break;
		case PUBREL:
			break;
		case PUBCOMP:
			break;
		case SUBSCRIBE:
			handlerSubscribe(ctx, msg);
			break;
		case SUBACK:
			break;
		case UNSUBSCRIBE:
			handlerUnSubscribe(ctx, (MqttUnsubscribeMessage) msg);
			break;
		case UNSUBACK:
			break;
		case PINGREQ:
			handlerPingReq(ctx, msg); // 返回心跳消息
			break;
		case PINGRESP:
			break;
		case DISCONNECT:
			handlerDisConnect(ctx, msg);
			break;
		}
	}

	/**
	 * 处理心跳Ping
	 * 
	 * @param ctx
	 * @param msg
	 */
	void handlerPingReq(ChannelHandlerContext ctx, MqttMessage msg) {
		logger.debug("心跳消息-PINGREQ: \r\n" + msg.toString());
		// 返回心跳消息
		byte[] pingresp = { (byte) 0xd0, 0x00 };
		ByteBuf buff = ctx.alloc().buffer();
		buff.writeBytes(pingresp);
		ctx.writeAndFlush(buff);
	}

	void handlerDisConnect(ChannelHandlerContext ctx, MqttMessage msg) {
		logger.debug("断开连接-DISCONNECT: \r\n" + msg.toString());
		// 返回断开连接消息
		byte[] message = { (byte) 0xe0, 0x00 };
		ByteBuf buff1 = ctx.alloc().buffer();
		buff1.writeBytes(message);
		ctx.writeAndFlush(buff1);
		ctx.close();
	}

	/**
	 * 处理连接登录请求
	 * 
	 * @param ctx
	 * @param msg
	 */
	void handlerConnect(ChannelHandlerContext ctx, MqttMessage msg) {
		MqttConnectMessage connMsg = (MqttConnectMessage) msg;
		MqttConnectReturnCode connectReturnCode = MqttConnectReturnCode.CONNECTION_ACCEPTED;

		MqttConnectVariableHeader connUserInfo = connMsg.variableHeader();
		// 用户身份验证
		MqttConnectPayload mqttConnectPayload = connMsg.payload();
		switch (connUserInfo.version()) {
		case 3:
		case 4:
			break;
		default:
			connectReturnCode = MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION;
			break;
		}

		String clientId = mqttConnectPayload.clientIdentifier();
		int keepTimeSeconds = connUserInfo.keepAliveTimeSeconds();

		ctx.channel().pipeline().addFirst("TimeCheck", new IdleStateHandler(keepTimeSeconds, 0, 0, TimeUnit.SECONDS));

		if (connUserInfo.hasUserName() && connUserInfo.hasPassword()) {
			String username = mqttConnectPayload.userName();
			String password = mqttConnectPayload.password();
			logger.debug("connect " + username + "," + password + ",ID:" + clientId);
		} else {
			logger.debug("connect ID: " + clientId);
		}
		logger.debug("keep time:" + keepTimeSeconds);

		{
			mqttSession = new MqttSession();
			mqttSession.setClientId(clientId);
			if (connUserInfo.hasUserName())
				mqttSession.setUserName(mqttConnectPayload.userName());
			mqttSession.setMsgOutChannel(ctx.channel());

		}

		MqttConnAckVariableHeader mqttConnAckVariableHeader = new MqttConnAckVariableHeader(connectReturnCode, true);
		MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.CONNACK, false, MqttQoS.FAILURE, false,
				0x02);
		MqttConnAckMessage mqttConnAckMessage = new MqttConnAckMessage(mqttFixedHeader, mqttConnAckVariableHeader);

		ctx.writeAndFlush(mqttConnAckMessage);
		if (mqttConnAckMessage.variableHeader().connectReturnCode() != MqttConnectReturnCode.CONNECTION_ACCEPTED) {
			ctx.close();
		}
	}

	/**
	 * 处理发布消息
	 * 
	 * @param ctx
	 * @param msg
	 */
	void handlerPublish(ChannelHandlerContext ctx, MqttMessage msg) {
		MqttPublishMessage publishMessage = (MqttPublishMessage) msg;
		MqttFixedHeader mqttFixedHeader = publishMessage.fixedHeader();
		MqttPublishVariableHeader variableHeader = publishMessage.variableHeader();
		String topicName = variableHeader.topicName();

		ByteBuf payload = publishMessage.payload();
		byte[] dst = new byte[payload.readableBytes()];
		payload.markReaderIndex(); // 我们标记一下当前的readIndex的位置
		payload.readBytes(dst);
		payload.resetReaderIndex();

		logger.debug("publish:" + mqttFixedHeader.qosLevel() + "," + variableHeader.messageId() + "," + topicName + ","
				+ new String(dst));

		// 需要转发给其他客服端
		mqttTopicTree.publish(topicName, variableHeader.messageId(), mqttFixedHeader.qosLevel(), payload);

		switch (mqttFixedHeader.qosLevel()) {
		case AT_MOST_ONCE:
			break;
		case AT_LEAST_ONCE: {
			int messageId = variableHeader.messageId();
			MqttFixedHeader a = new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0);
			MqttPubAckMessage ack = new MqttPubAckMessage(a, MqttMessageIdVariableHeader.from(messageId));
			ctx.writeAndFlush(ack);
		}
		case EXACTLY_ONCE: // 2 not
			break;
		case FAILURE:
			break;
		default:
			break;
		}
	}

	/**
	 * 处理订阅消息
	 * 
	 * @param ctx
	 * @param msg
	 */
	void handlerSubscribe(ChannelHandlerContext ctx, MqttMessage msg) {
		MqttSubscribeMessage subMsg = (MqttSubscribeMessage) msg;

		MqttMessageIdVariableHeader mqttMessageIdVariableHeader = subMsg.variableHeader();
		// 或许可以单独抽出来！！！
		Integer messageId = mqttMessageIdVariableHeader.messageId();
		// save messageId <==> clientId
		MqttSubscribePayload mqttSubscribePayload = subMsg.payload();
		List<MqttTopicSubscription> topicSubscriptions = mqttSubscribePayload.topicSubscriptions();
		logger.debug("subscribe: " + topicSubscriptions);

		for (MqttTopicSubscription topicSubscription : topicSubscriptions) {
			String topicName = topicSubscription.topicName();
			// topicSubscription.qualityOfService().value();
			mqttTopicTree.subscribe(topicName, mqttSession);
		}

		// save topic-names and Qos
		List<Integer> grantedQoSLevels = new ArrayList<Integer>();

		// service return list
		MqttSubAckPayload mqttSubAckPayload = new MqttSubAckPayload(grantedQoSLevels);
		MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE,
				false, messageId);
		MqttSubAckMessage mqttSubAckMessage = new MqttSubAckMessage(mqttFixedHeader, mqttMessageIdVariableHeader,
				mqttSubAckPayload);
		ctx.writeAndFlush(mqttSubAckMessage);
	}

	/**
	 * 处理取消订阅消息
	 * 
	 * @param ctx
	 * @param msg
	 */
	void handlerUnSubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
		MqttMessageIdVariableHeader mqttMessageIdVariableHeader = msg.variableHeader();
		MqttUnsubscribePayload mqttUnsubscribePayload = msg.payload();

		List<String> topics = mqttUnsubscribePayload.topics();
		logger.debug("unsubscribe" + topics);
		for (String string : topics) {
			mqttTopicTree.unSub(string, mqttSession);
		}

		Integer messageId = mqttMessageIdVariableHeader.messageId();

		MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE,
				false, 0);
		MqttUnsubAckMessage ack = new MqttUnsubAckMessage(mqttFixedHeader, MqttMessageIdVariableHeader.from(messageId));
		ctx.writeAndFlush(ack);
	}
}
