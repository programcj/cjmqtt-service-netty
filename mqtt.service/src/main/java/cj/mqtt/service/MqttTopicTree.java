package cj.mqtt.service;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttFixedHeader;
import io.netty.handler.codec.mqtt.MqttMessageType;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.handler.codec.mqtt.MqttPublishVariableHeader;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttTopicTree {
	Map<String, List<WeakReference<MqttSession>>> topicSubMap = new HashMap<String, List<WeakReference<MqttSession>>>();

	public void close(MqttSession mqttSession) {
		// 从topicSubMap中清除自己
		for (Object object : mqttSession.nodeList) {
			List<WeakReference<MqttSession>> list = (List<WeakReference<MqttSession>>) object;

			Iterator<WeakReference<MqttSession>> iterator = list.iterator();
			while (iterator.hasNext()) {
				WeakReference<MqttSession> item = iterator.next();
				MqttSession session = item.get();
				if (session == null) {
					iterator.remove();
					continue;
				}
				if (session == mqttSession) {
					iterator.remove();
					// mqttSession.nodeList.remove(list);
				}
			}
		}
	}

	public void publish(String topicName, int messageId, MqttQoS Qos, ByteBuf payload) {
		List<WeakReference<MqttSession>> list = topicSubMap.get(topicName);
		if (list == null)
			return;
		Iterator<WeakReference<MqttSession>> iterator = list.iterator();

		while (iterator.hasNext()) {
			WeakReference<MqttSession> item = iterator.next();
			MqttSession mqttSession = item.get();
			if (mqttSession == null) {
				iterator.remove();
				continue;
			}
			MqttPublishMessage msg = new MqttPublishMessage(
					new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, 0),
					new MqttPublishVariableHeader(topicName, messageId), payload.copy());
			mqttSession.getMsgOutChannel().writeAndFlush(msg);
		}
	}

	public void unSub(String topicName, MqttSession mqttSession) {
		List<WeakReference<MqttSession>> list = topicSubMap.get(topicName);
		if (list == null) {
			return;
		}
		Iterator<WeakReference<MqttSession>> iterator = list.iterator();
		while (iterator.hasNext()) {
			WeakReference<MqttSession> item = iterator.next();
			MqttSession session = item.get();
			if (session == null) {
				iterator.remove();
				continue;
			}
			if (session == mqttSession) {
				iterator.remove();
				mqttSession.nodeList.remove(list);
			}
		}
	}

	public void subscribe(String topicName, MqttSession mqttSession) {
		List<WeakReference<MqttSession>> list = topicSubMap.get(topicName);
		if (list == null) {
			list = new ArrayList<>();
			topicSubMap.put(topicName, list);
		}

		Iterator<WeakReference<MqttSession>> iterator = list.iterator();
		while (iterator.hasNext()) {
			WeakReference<MqttSession> item = iterator.next();
			MqttSession session = item.get();
			if (session == null) {
				iterator.remove();
				continue;
			}
			if (session == mqttSession)
				return;
		}

		list.add(new WeakReference<MqttSession>(mqttSession));

		mqttSession.nodeList.add(list);
	}

	public void debug() {
		System.out.println("==== topic list begin =====");
		Iterator<Entry<String, List<WeakReference<MqttSession>>>> iterator = topicSubMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, List<WeakReference<MqttSession>>> next = iterator.next();

			System.out.println(">" + next.getKey());

			List<WeakReference<MqttSession>> value = next.getValue();
			for (WeakReference<MqttSession> weakReference : value) {
				MqttSession mqttSession = weakReference.get();
				if (mqttSession != null) {
					System.out.println("\t" + mqttSession.toString());
				}
			}
		}

		System.out.println("==== topic list end =====");
	}
}
