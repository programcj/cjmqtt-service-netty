package cj.mqtt.service;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttSession {
	private String clientId;
	private String userName;
	private Channel msgOutChannel;

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

	public void setMsgOutChannel(Channel msgOutChannel) {
		this.msgOutChannel = msgOutChannel;
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
		sb.append('[').append(msgOutChannel.remoteAddress()).append(", CID:").append(clientId).append(",u:")
				.append(userName).append(']');
		return sb.toString();
	}

	volatile int publishMessageId = 1;

	class Message {
		String topic;
		ByteBuf data;
		MqttQoS qos;
	}

	ConcurrentLinkedQueue<Message> pubQueue = new ConcurrentLinkedQueue<Message>();

	/**
	 * Qos需要从MqttSession中获取到此topic对应的Qos,如果为1，客户端会上报ack
	 */
	public void publish(String topic, ByteBuf data, int qos) {

		MqttQoS pubQos = MqttQoS.valueOf(qos);

		if (pubQos == MqttQoS.AT_MOST_ONCE) {
			MqttPublishMessage msg = new MqttPublishMessage(
					new MqttFixedHeader(MqttMessageType.PUBLISH, false, pubQos, false, 0),
					new MqttPublishVariableHeader(topic, publishMessageId++), data.copy());
			msgOutChannel.writeAndFlush(msg);
		}

		if (pubQos == MqttQoS.AT_LEAST_ONCE) { // 需要等客户端回ack,才能发送下一个消息
			Message msg = new Message();
			msg.data = data.copy();
			msg.qos = pubQos;
			msg.topic = topic;

			pubQueue.add(msg);
			if (pubQueue.size() == 1) {
				publistNext();
			}
		}
	}

	/**
	 * 发送下一条消息，一般收到客户端的pubAck之后再发送下一条
	 */
	public void publistNext() {
		Message msg = pubQueue.poll();
		if (msg == null)
			return;

		MqttPublishMessage pmsg = new MqttPublishMessage(
				new MqttFixedHeader(MqttMessageType.PUBLISH, false, msg.qos, false, 0),
				new MqttPublishVariableHeader(msg.topic, publishMessageId++), msg.data);
		msgOutChannel.writeAndFlush(pmsg);
	}
}
