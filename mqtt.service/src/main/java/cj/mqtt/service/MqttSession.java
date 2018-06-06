package cj.mqtt.service;

import java.util.ArrayList;

import io.netty.channel.Channel;

public class MqttSession {
	String clientId;
	String userName;
	Channel msgOutChannel;

	ArrayList<Object> nodeList = new ArrayList<>();

	public String getClientId() {
		return clientId;
	}

	public void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public Channel getMsgOutChannel() {
		return msgOutChannel;
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
		sb.append('[').append(getMsgOutChannel().remoteAddress()).append(", CID:").append(clientId).append(",u:")
				.append(userName).append(']');
		return sb.toString();
	}
}
