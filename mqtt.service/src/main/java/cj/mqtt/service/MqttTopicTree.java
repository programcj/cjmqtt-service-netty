package cj.mqtt.service;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttTopicTree {

	// 主题订阅者
	class TopicItem {
		WeakReference<MqttSession> mqttSession;
		int subQos;

		public TopicItem(MqttSession mqttSession, int subQos) {
			super();
			this.mqttSession = new WeakReference<MqttSession>(mqttSession);
			this.subQos = subQos;
		}

		public TopicItem() {
		}
	}

	// Qos ?
	Map<String, List<TopicItem>> topicSubMap = new ConcurrentHashMap<String, List<TopicItem>>();

	@SuppressWarnings("unchecked")
	public void close(MqttSession mqttSession) {
		// 从topicSubMap中清除自己

		mqttSession.clearTopicNode(new MqttSession.TopicNodeInterface() {

			@Override
			public void onItem(MqttSession mqttSession, Object obj) {
				List<TopicItem> list = (List<TopicItem>) obj;

				synchronized (list) {
					Iterator<TopicItem> iterator = list.iterator();
					while (iterator.hasNext()) {
						TopicItem item = iterator.next();
						MqttSession session = item.mqttSession.get();
						if (session == null) {
							iterator.remove();
							continue;
						}
						if (session == mqttSession) {
							iterator.remove();
						}
					}
				}
				/// end
			}
		});

	}

	public void publish(String topicName, int messageId, MqttQoS Qos, ByteBuf payload) {
		List<TopicItem> list = topicSubMap.get(topicName);
		if (list == null)
			return;
		synchronized (list) {
			Iterator<TopicItem> iterator = list.iterator();
			while (iterator.hasNext()) {
				TopicItem item = iterator.next();
				MqttSession mqttSession = item.mqttSession.get();
				if (mqttSession == null) {
					iterator.remove();
					continue;
				}
				mqttSession.publish(topicName, payload, item.subQos);
			}
		}
	}

	public void unSub(String topicName, MqttSession mqttSession) {
		List<TopicItem> list = topicSubMap.get(topicName);
		if (list == null) {
			return;
		}
		synchronized (list) {
			Iterator<TopicItem> iterator = list.iterator();
			while (iterator.hasNext()) {
				TopicItem item = iterator.next();
				MqttSession session = item.mqttSession.get();
				if (session == null) {
					iterator.remove();
					continue;
				}
				if (session == mqttSession) {
					iterator.remove();
					mqttSession.removeTopicNode(list);
				}
			}
		}
	}

	public void subscribe(String topicName, MqttSession mqttSession, int qos) {
		List<TopicItem> list = null;
		synchronized (topicSubMap) {
			list = topicSubMap.get(topicName);
			if (list == null) {
				list = Collections.synchronizedList(new ArrayList<TopicItem>());
				topicSubMap.put(topicName, list);
			}
		}

		synchronized (list) {
			Iterator<TopicItem> iterator = list.iterator();
			while (iterator.hasNext()) {
				TopicItem item = iterator.next();
				MqttSession session = item.mqttSession.get();
				if (session == null) {
					iterator.remove();
					continue;
				}
				if (session == mqttSession)
					return;
			}

			list.add(new TopicItem(mqttSession, qos));
		}

		mqttSession.addTopicNode(list);
	}

	public void debug() {
		System.out.println("==== topic list begin =====");
		Iterator<Entry<String, List<TopicItem>>> iterator = topicSubMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<String, List<TopicItem>> next = iterator.next();

			System.out.println(">" + next.getKey());

			List<TopicItem> value = next.getValue();
			for (TopicItem item : value) {
				MqttSession mqttSession = item.mqttSession.get();
				if (mqttSession != null) {
					System.out.println("\t" + mqttSession.toString());
				}
			}
		}

		System.out.println("==== topic list end =====");
	}
}
