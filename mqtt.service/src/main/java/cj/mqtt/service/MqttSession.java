package cj.mqtt.service;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttSession {
	private static Logger logger = LoggerFactory.getLogger(MqttSession.class);

	private String clientId;
	private String userName;
	private Channel channel;

	private ArrayList<Object> topicNodeList = new ArrayList<>();

	interface TopicNodeInterface {
		void onItem(MqttSession session, Object obj);
	}

	public synchronized void clearTopicNode(TopicNodeInterface callback) {
		for (Object obj : topicNodeList) {
			if (callback != null)
				callback.onItem(this, obj);
		}
		topicNodeList.clear();
	}

	public synchronized void removeTopicNode(Object obj) {
		topicNodeList.remove(obj);
	}

	public void addTopicNode(Object obj) {
		topicNodeList.add(obj);
	}

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append('[').append(channel.remoteAddress()).append(", CID:").append(clientId).append(",u:").append(userName)
				.append(']');
		return sb.toString();
	}

	volatile int publishMessageId = 1;

	class Message {
		String topic;
		int messgaeId;
		MqttQoS qos;
		ByteBuf data;
		WeakReference<PubACKReceiver> receiver;
	}

	ConcurrentLinkedQueue<Message> publishMessageCacheQueue = new ConcurrentLinkedQueue<Message>();
	boolean waitPublishAck = false;
	long waitPublishAckStartTime = 0;

	/**
	 * Qos需要从MqttSession中获取到此topic对应的Qos,如果为1，客户端会上报ack
	 */
	public synchronized void publish(String topic, ByteBuf data, int qos, PubACKReceiver receiver) {
		MqttQoS pubQos = MqttQoS.valueOf(qos);
		// 在等待PublishACK, 消息队列数量大于0
		if (publishMessageCacheQueue.size() > 0 || waitPublishAck) {
			Message msg = new Message();
			msg.data = data.copy();
			msg.qos = pubQos;
			msg.messgaeId = publishMessageId++;
			msg.topic = topic;
			msg.receiver = new WeakReference<MqttSession.PubACKReceiver>(receiver);

			publishMessageCacheQueue.add(msg);
			logger.debug("need wiat ack");
			if (publishMessageCacheQueue.size() > 20) { // 如果缓存有20条以上，並且

			}
			return;
		}

		/**
		 * 消息QOS=0不等待客户端回复ACK
		 */
		if (pubQos == MqttQoS.AT_MOST_ONCE) {
			MqttPublishMessage msg = new MqttPublishMessage(
					new MqttFixedHeader(MqttMessageType.PUBLISH, false, pubQos, false, 0),
					new MqttPublishVariableHeader(topic, publishMessageId++), data.copy());
			channel.writeAndFlush(msg);
		}

		/**
		 * 消息QOS=1需要等客户端回ack,才能发送下一个消息
		 */
		if (pubQos == MqttQoS.AT_LEAST_ONCE) {

			Message msg = new Message();
			msg.data = data.copy();
			msg.messgaeId = publishMessageId++;
			msg.qos = pubQos;
			msg.topic = topic;
			msg.receiver = new WeakReference<MqttSession.PubACKReceiver>(receiver);

			publishMessageCacheQueue.add(msg);
			if (publishMessageCacheQueue.size() == 1) {
				publistNext();
			}
		}
	}

	/**
	 * 发送下一条消息，一般收到客户端的pubAck之后再发送下一条
	 */
	public void publistNext() {
		Message msg = publishMessageCacheQueue.poll();
		if (msg == null)
			return;

		MqttPublishMessage pmsg = new MqttPublishMessage(
				new MqttFixedHeader(MqttMessageType.PUBLISH, false, msg.qos, false, 0),
				new MqttPublishVariableHeader(msg.topic, msg.messgaeId), msg.data);

		if (pmsg.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE) {
			waitPublishAck = true;

			registerPubACKReceiver(msg.messgaeId, msg.receiver.get());
			waitPublishAckStartTime = System.currentTimeMillis();
			logger.debug("need pubACK, size=" + publishMessageCacheQueue.size());
		}
		channel.writeAndFlush(pmsg);
	}

	interface PubACKReceiver {
		void onPublishACK(int messageId);
	}

	Map<Integer, PubACKReceiver> pubackBacks = new ConcurrentHashMap<>();

	private void registerPubACKReceiver(int messageId, PubACKReceiver receiver) {
		if (receiver == null)
			return;
		pubackBacks.put(messageId, receiver);
	}

	/**
	 * 处理pubACK的消息ID
	 * 
	 * @param messageId
	 */
	public void handlerACKMessageId(int messageId) {
		waitPublishAck = false;
		// 1秒=1000毫秒=1000000微秒
		logger.debug("Puback Time(millis):" + (System.currentTimeMillis() - waitPublishAckStartTime));
		PubACKReceiver pubACKReceiver = pubackBacks.get(messageId);
		if (pubACKReceiver != null) {
			pubackBacks.remove(messageId);
			pubACKReceiver.onPublishACK(messageId);
		}
		publistNext();
	}
}
