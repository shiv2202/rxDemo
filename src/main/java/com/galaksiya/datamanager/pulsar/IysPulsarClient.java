package com.galaksiya.datamanager.pulsar;

import com.galaksiya.datamanager.utils.Constants;
import com.galaksiya.logger.GLogger;
import com.galaksiya.logger.OperationLog;
import com.google.gson.JsonObject;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.Level;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.policies.data.TopicStats;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * This class is used to perform pulsar operations. Using the IysPulsarClient object, one can create a PulsarClient
 * which is used to connect to Pulsar. Also, this class stores all pulsar producers which were created in a map.
 * IysPulsarClient objects can be used to send any message to any pulsar topic.
 *
 * @author galaksiya
 */
public class IysPulsarClient {

	private static final String BLOCK_IF_QUEUE_FULL = "BLOCK_IF_QUEUE_FULL";
	public static final String LISTENER_THREAD_POOL_SIZE = "LISTENER_THREAD_POOL_SIZE";
	public static final String IO_THREAD_POOL_SIZE = "IO_THREAD_POOL_SIZE";
	public static final String CONNECTION_POOL_SIZE_PER_BROKER = "CONNECTION_POOL_SIZE_PER_BROKER";
	private static final String PRODUCER_QUEUE_SIZE = "PRODUCER_QUEUE_SIZE";
	private static final String PRODUCER_QUEUE_SIZE_ACROSS_PARTITIONS = "PRODUCER_QUEUE_SIZE_ACROSS_PARTITIONS";

	private static final String TOPIC = "topic";

	private static final String CREATE_PULSAR_CONSUMER = "createPulsarConsumer";

	private static final String CREATE_PULSAR_CLIENT = "createPulsarClient";

	private static final String PULSAR_ENDPOINT = "pulsarEndpoint";

	private static final String CREATE_NAMESPACE = "createNamespace";

	private static final String NAMESPACES = "namespaces";

	/**
	 * Logger instance.
	 */
	private final GLogger gLogger = new GLogger(IysPulsarClient.class);

	private PulsarClient client;
	private final Map<String, Producer<String>> producers = new HashMap<>();
	private PulsarAdmin pulsarAdminClient = null;
	private int numPartitions = 1;

	/**
	 * Initialize pulsar connection
	 */
	public IysPulsarClient(String pulsarEndpoint, String pulsarAdminEndpoint, int numPartitions) {
		this.numPartitions = numPartitions;
		createPulsarClient(pulsarEndpoint);
		createPulsarAdminClient(pulsarAdminEndpoint);

		createNamespace(pulsarEndpoint, pulsarAdminEndpoint);
		createTenant(pulsarEndpoint, pulsarAdminEndpoint);
		setRetentionPolicies(pulsarEndpoint, pulsarAdminEndpoint);
	}

	private void createTenant(String pulsarEndpoint, String pulsarAdminEndpoint) {
		OperationLog log = gLogger.startOperation("createTenant").addField(PULSAR_ENDPOINT, pulsarEndpoint)
				.addField("pulsarAdminEndpoint", pulsarAdminEndpoint);
		try {
			List<String> tenants = pulsarAdminClient.tenants().getTenants();
			log.addField("tenants", tenants.toString());
			if (!tenants.contains("public")) {
				TenantInfo tenantInfo = new TenantInfo(new HashSet<>(Collections.emptyList()),
						new HashSet<>(pulsarAdminClient.clusters().getClusters()));
				pulsarAdminClient.tenants().createTenant("public", tenantInfo);
			}
			tenants = pulsarAdminClient.tenants().getTenants();
			log.addField("newTenants", tenants.toString()).succeed();
		} catch (PulsarAdminException e) {
			log.fail(e);
		}
	}

	private void createNamespace(String pulsarEndpoint, String pulsarAdminEndpoint) {
		OperationLog log = gLogger.startOperation(CREATE_NAMESPACE).addField(PULSAR_ENDPOINT, pulsarEndpoint)
				.addField("pulsarAdminEndpoint", pulsarAdminEndpoint);
		try {
			List<String> namespaces = pulsarAdminClient.namespaces().getNamespaces("public");
			if (!namespaces.contains("public/default")) {
				pulsarAdminClient.namespaces().createNamespace("public/default");
			}
			if (!namespaces.contains("public/random")) {
				pulsarAdminClient.namespaces().createNamespace("public/random");
			}
			namespaces = pulsarAdminClient.namespaces().getNamespaces("public");
			log.addField("newNamespaces", namespaces.toString());
			log.addField(NAMESPACES, namespaces.toString()).succeed();
		} catch (PulsarAdminException e) {
			log.fail(e);
		}
	}

	private void setRetentionPolicies(String pulsarEndpoint, String pulsarAdminEndpoint) {
		OperationLog log = gLogger.startOperation("pulsarAdminClient").addField(PULSAR_ENDPOINT, pulsarEndpoint)
				.addField("pulsarAdminEndpoint", pulsarAdminEndpoint);
		try {
			int retentionTime = 10; // 10 minutes
			int retentionSize = 1073741; // 500 megabytes
			RetentionPolicies policies = new RetentionPolicies(retentionTime, retentionSize);
			pulsarAdminClient.namespaces().setRetention("public/random", policies);
			log.succeed();
		} catch (Exception e) {
			log.fail(e);
		}
	}

	private void createPulsarClient(String pulsarEndpoint) {
		OperationLog log = gLogger.startOperation(CREATE_PULSAR_CLIENT).addField(PULSAR_ENDPOINT, pulsarEndpoint);
		if (this.client == null) {
			try {
				this.client = PulsarClient.builder().serviceUrl(pulsarEndpoint)
						.connectionsPerBroker(getConnectionPoolSizePerBroker())
						.listenerThreads(getListenerThreadPoolSize()).ioThreads(getIoThreadPoolSize()).build();
				log.succeed();
			} catch (PulsarClientException e) {
				log.fail(e);
			}
		} else {
			log.succeed();
		}
	}

	public int getListenerThreadPoolSize() {
		int size = 1;
		try {
			String queueSize = System.getenv(LISTENER_THREAD_POOL_SIZE);
			if (queueSize != null) {
				size = Integer.parseInt(queueSize);
				gLogger.info("Listener thread pool size: %s", size);
			} else {
				gLogger.warn("Listener thread pool size not given, using default!");
			}
		} catch (Throwable t) {
			gLogger.warn("Listener thread pool size not given, using default!", t);
		}
		return size;
	}

	public int getIoThreadPoolSize() {
		int size = 1;
		try {
			String queueSize = System.getenv(IO_THREAD_POOL_SIZE);
			if (queueSize != null) {
				size = Integer.parseInt(queueSize);
				gLogger.info("IO Thread pool size: %s", size);
			} else {
				gLogger.warn("IO Thread pool size not given, using default!");
			}
		} catch (Throwable t) {
			gLogger.warn("IO Thread pool size not given, using default!", t);
		}
		return size;
	}

	public int getConnectionPoolSizePerBroker() {
		int size = 1;
		try {
			String queueSize = System.getenv(CONNECTION_POOL_SIZE_PER_BROKER);
			if (queueSize != null) {
				size = Integer.parseInt(queueSize);
				gLogger.info("Connection pool size per broker: %s", size);
			} else {
				gLogger.warn("Connection pool size per broker not given, using default!");
			}
		} catch (Throwable t) {
			gLogger.warn("Connection pool size per broker not given, using default!", t);
		}
		return size;
	}

	/**
	 * Pulsar client getter
	 *
	 * @return
	 */
	public PulsarClient getPulsarClient() {
		return this.client;
	}

	/**
	 * This method is used to get the pulsar producer which are related to given topic name. If the producer was created
	 * before, the method returns it from the producer map, otherwise the new producer is created.
	 *
	 * @param topicName it indicates the topic name for getting pulsar producer which is mapped to this topic.
	 * @return the method returns pulsar producer which is used to send messages to topics.
	 */
	public Producer<String> getPulsarProducer(String topicName) {
		if (!getProducers().containsKey(topicName)) {
			OperationLog log = gLogger.startOperation("getPulsarProducer").addField("topicName", topicName);
			try {
				// try {
				// this.pulsarAdminClient.topics().createPartitionedTopic(topicName, numPartitions);
				// log.addField("partitionSize", numPartitions);
				// } catch (PulsarAdminException e) {
				// log.addField("partitionError", ExceptionUtils.getStackTrace(e));
				// }
				getProducers().put(topicName,
						getPulsarClient().newProducer(Schema.STRING).maxPendingMessages(getProducerQueueSize())
								.maxPendingMessagesAcrossPartitions(getProducerQueueSizeAcrossPartitions())
								.blockIfQueueFull(getBlockIfQueueFull()).topic(topicName).create());
				log.succeed();
				// this.pulsarAdminClient.topics().createPartitionedTopic(topicName, 3);
				return getProducers().get(topicName);
			} catch (PulsarClientException e) {
				log.fail(e);
			}
		} else {
			return getProducers().get(topicName);
		}
		return null;
	}

	public boolean getBlockIfQueueFull() {
		boolean block = false;
		try {
			String blockIfQueueFull = System.getenv(BLOCK_IF_QUEUE_FULL);
			if (blockIfQueueFull != null) {
				block = Boolean.parseBoolean(blockIfQueueFull);
				gLogger.info("Block if queue full: %s", block);
			} else {
				gLogger.warn("Block if queue full property not given, using default!");
			}
		} catch (Throwable t) {
			gLogger.warn("Block if queue full property not given, using default!", t);
		}
		return block;
	}

	public int getProducerQueueSize() {
		int size = 1000;
		try {
			String queueSize = System.getenv(PRODUCER_QUEUE_SIZE);
			if (queueSize != null) {
				size = Integer.parseInt(queueSize);
				gLogger.info("Producer queue size: %s", size);
			} else {
				gLogger.warn("Producer queue size not given, using default!");
			}
		} catch (Throwable t) {
			gLogger.warn("Producer queue size not given, using default!", t);
		}
		return size;
	}

	public int getProducerQueueSizeAcrossPartitions() {
		int size = 50000;
		try {
			String queueSize = System.getenv(PRODUCER_QUEUE_SIZE_ACROSS_PARTITIONS);
			if (queueSize != null) {
				size = Integer.parseInt(queueSize);
				gLogger.info("Producer queue size across partitions: %s", size);
			} else {
				gLogger.warn("Producer queue size across partitions not given, using default!");
			}
		} catch (Throwable t) {
			gLogger.warn("Producer queue size across partitions not given, using default!", t);
		}
		return size;
	}

	public Map<String, Producer<String>> getProducers() {
		return producers;
	}

	/**
	 * This method is use to save given message data to given topic in pulsar using the pulsar producer which is related
	 * to topic name.
	 * <p>
	 * If pulsar producer is available, it is used otherwise, the new pulsar producer is created.
	 * </p>
	 *
	 * @param topic   the parameter indicates the topic which the data will be sent.
	 * @param message the message parameter is the data that will be sent to Pulsar topic.
	 * @param delay   Delay in milliseconds to tell the pulsar to make the message to be able to get consumed after.
	 * @throws Exception
	 */
	public void sendMessage(final String topic, final JsonObject message, final long delay) throws Exception {
		String requestId = message.get(Constants.BatchTransaction.REQUEST_ID).getAsString();
		OperationLog log = gLogger.startOperation("MessageSendingToTopic", requestId).addField("topic", topic)
				.addField("message", message.toString(), Level.DEBUG).addField("delay", delay);
		try {
			if (delay != 0) {
				getPulsarProducer(topic).newMessage().value(message.toString())
						.deliverAfter(delay, TimeUnit.MILLISECONDS).send();
			} else {
				getPulsarProducer(topic).newMessage().value(message.toString()).send();
			}
			log.succeed();
		} catch (Exception e) {
			log.fail(e);
			// throw exception to return error message to user
			throw e;
		}
	}

	/**
	 * This method is use to save given message data to given topic in pulsar using the pulsar producer which is related
	 * to topic name.
	 * <p>
	 * If pulsar producer is available, it is used otherwise, the new pulsar producer is created.
	 * </p>
	 *
	 * @param topic   the parameter indicates the topic which the data will be sent.
	 * @param message the message parameter is the data that will be sent to Pulsar topic.
	 * @throws Exception
	 */
	public void sendMessageAsync(final String topic, final JsonObject message) throws Exception {
		String requestId = message.get(Constants.BatchTransaction.REQUEST_ID).getAsString();
		OperationLog log = gLogger.startOperation("MessageSendingToTopic", requestId).addField("topic", topic)
				.addField("message", message.toString(), Level.DEBUG);
		try {
			getPulsarProducer(topic).newMessage().value(message.toString()).sendAsync()
					.thenAccept(messageId -> gLogger.startOperation("messageSendingAsyncSucceed")
							.addField("messageId", messageId).addField("message", message.toString(), Level.DEBUG)
							.succeed())
					.exceptionally(e -> {
						gLogger.startOperation("messageSendingAsyncFail")
								.addField("message", message.toString(), Level.DEBUG).fail(e);
						return null;
					});
			log.succeed();
		} catch (Exception e) {
			log.fail(e);
			// throw exception to return error message to user
			throw e;
		}
	}

	/**
	 * This method is use to save given message data to given topic in pulsar using the pulsar producer which is related
	 * to topic name.
	 * <p>
	 * If pulsar producer is available, it is used otherwise, the new pulsar producer is created.
	 * </p>
	 *
	 * @param topic   the parameter indicates the topic which the data will be sent.
	 * @param message the message parameter is the data that will be sent to Pulsar topic.
	 * @throws Exception
	 */
	public void sendMessage(final String topic, final JsonObject message) throws Exception {
		if (topic.equals(Constants.PulsarUtils.retailerTopic)
				|| topic.equals(Constants.PulsarUtils.retailerBatchTopic)) {
			sendMessage(topic, message, 1500);
		} else if (topic.equals(Constants.PulsarUtils.actionHistoryTopic)) {
			sendMessage(topic, message, 1000);
		} else if (topic.equals(Constants.PulsarUtils.encryptBatchTopic)) {
			sendMessageAsync(topic, message);
		} else {
			sendMessage(topic, message, 0);
		}
	}

	public Consumer<?> createPulsarConsumer(String topic, String subscription, int messageNumber) {
		OperationLog log = gLogger.startOperation(CREATE_PULSAR_CONSUMER).addField(TOPIC, topic)
				.addField("subscription", subscription);
		try {
			// BatchReceivePolicy policy = BatchReceivePolicy.builder()
			// .maxNumMessages(100)
			// .timeout(5, TimeUnit.MILLISECONDS)
			// .build();
			// try {
			// this.pulsarAdminClient.topics().createPartitionedTopic(topic, numPartitions);
			// log.addField("partitionSize", numPartitions);
			// } catch (PulsarAdminException e) {
			// log.addField("partitionError", ExceptionUtils.getStackTrace(e));
			// }
			Consumer<?> consumer = client.newConsumer().topic(topic).subscriptionName(subscription)
					.subscriptionType(SubscriptionType.Shared).batchReceivePolicy(BatchReceivePolicy.builder()
							.maxNumMessages(messageNumber).timeout(5, TimeUnit.MILLISECONDS).build())
					.subscribe();
			log.succeed();
			return consumer;
		} catch (Throwable t) {
			log.fail(t);
		}
		return null;
	}

	/**
	 * This method creates pulsar admin client which will be used for topic deletion operations.
	 *
	 * @return
	 */
	private void createPulsarAdminClient(String pulsarAdminHostname) {
		boolean tlsAllowInsecureConnection = false;
		String tlsTrustCertsFilePath = null;
		if (this.pulsarAdminClient == null) {
			OperationLog log = gLogger.startOperation("createPulsarAdminClient").addField("pulsarHostname",
					pulsarAdminHostname);
			try {
				this.pulsarAdminClient = PulsarAdmin.builder().serviceHttpUrl(pulsarAdminHostname)
						.tlsTrustCertsFilePath(tlsTrustCertsFilePath)
						.allowTlsInsecureConnection(tlsAllowInsecureConnection).build();
				this.pulsarAdminClient.namespaces().setSubscriptionExpirationTime("public/random", 5);
				log.succeed();
			} catch (Exception e) {
				log.fail(e);
			}
		}
	}

	public PulsarAdmin getPulsarAdminClient() {
		return this.pulsarAdminClient;
	}

	/**
	 * This method returns the message size in the given topic name at the current time.
	 *
	 * @param topicName it is the name of the topic which will be analyzed.
	 * @return msgInCounter value that indicates the number of the messages in the topic.
	 * @throws PulsarAdminException
	 */
	public long getMsgCountInCounter(String namespace, String topicName) throws PulsarAdminException {
		try {
			String destination = "persistent://public/" + namespace + "/" + topicName;
			TopicStats stats = this.pulsarAdminClient.topics().getStats(destination);
			return stats.msgInCounter;
		} catch (Exception e) {
			return 0;
		}
	}

}
