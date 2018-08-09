package cj.mqtt.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttQoS;

/**
 * Hello world!
 *
 */
public class App {
	private static Logger logger = LoggerFactory.getLogger(App.class);
	private static MqttTopicTree mqttTopicTree = new MqttTopicTree();

	public static void main(String[] args) throws InterruptedException {
		String workpath = System.getProperty("user.dir");
		try {
			PropertyConfigurator.configure(new File(workpath, "log4j.properties").getCanonicalPath());
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		new Thread(new Runnable() {
			public void run() {
				BufferedReader read = new BufferedReader(new InputStreamReader(System.in));
				try {
					String value = null;
					while ((value = read.readLine()) != null) {
						if (value.startsWith("debug-tree")) {
							mqttTopicTree.debug();
						}
						if (value.startsWith("gc")) {
							logger.debug("gc");
							System.gc();
						}
						if (value.equals("publish-test")) {
							String msg = "hello my is server!";
							ByteBuf payload = Unpooled.buffer(30);

							for (int i = 0; i < 10; i++) {
								msg = "hello my is server!" + i;
								payload.writeBytes(msg.getBytes());
								 mqttTopicTree.publish("broadtopic",MqttQoS.AT_LEAST_ONCE, payload);
								final Object wait = new Object();

								MqttSession mqttSession = mqttTopicTree.getMqttSession("MQTT_FX_Client");
								
								if (mqttSession != null) {
									mqttSession.publish("clienttopic", payload, MqttQoS.AT_LEAST_ONCE.value(),
											new MqttSession.PubACKReceiver() {

												@Override
												public void onPublishACK(int messageId) {
													logger.debug("----------------------------------");
													synchronized (wait) {
														wait.notifyAll();
													}
												}
											});
								} else {
									logger.debug("not find client");
								}

								synchronized (wait) {
									try {
										wait.wait(1000 * 5);
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								}

								// need wiat ACK
								payload.clear();
							}
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}).start();

		logger.info("start ....");

		int port = 1883;

		EventLoopGroup bossGroup = new NioEventLoopGroup(); // (1)
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap(); // (2)

			b.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class) // (3)
					.childHandler(new ChannelInitializer<SocketChannel>() { // (4)
						@Override
						public void initChannel(SocketChannel ch) throws Exception {
							ChannelPipeline pipeline = ch.pipeline();
							pipeline.addLast("protocol-HeadAdapter", new ProtocolHeadAdapter(mqttTopicTree));
						}
					}).option(ChannelOption.SO_BACKLOG, 128) // (5)
					.childOption(ChannelOption.SO_KEEPALIVE, true); // (6)

			// Bind and start to accept incoming connections.
			ChannelFuture f = b.bind(port).sync(); // (7)

			// Wait until the server socket is closed.
			// In this example, this does not happen, but you can do that to
			// gracefully
			// shut down your server.
			f.channel().closeFuture().sync();
		} finally {
			workerGroup.shutdownGracefully();
			bossGroup.shutdownGracefully();
			System.out.println("Server 关闭了");
		}
	}
}
