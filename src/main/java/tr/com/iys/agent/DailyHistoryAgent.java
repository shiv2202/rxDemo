package tr.com.iys.agent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClientException;

import com.galaksiya.datamanager.elasticsearch.ElasticClient;
import com.galaksiya.datamanager.elasticsearch.ElasticInstance;
import com.galaksiya.datamanager.pulsar.IysPulsarClient;
import com.galaksiya.datamanager.pulsar.PulsarInstance;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class DailyHistoryAgent {
	String pulsarEndpoint = "pulsar://" + "localhost" + ":" + "6650";

	String pulsarAdminEndpoint = "http://" + "localhost" + ":" + "8082";
	private static final String ELASTIC_HOST = "localhost";
	private static final int ELASTIC_PORT = 9200;
	private IysPulsarClient iysPulsarClient = null;
	private ElasticClient elasticClient = null;

	public static final String dailyHistoryTopic = "iys.module.dailyhistoryagent.action.add-daily-history";
	public static final String DAILY_HISTORY_SUBSCRIPTION = "daily-history-subscription";

	private Consumer<?> dailyHistoryConsumer = null;

	private List<Consumer<?>> consumerList = new ArrayList<>();

	public static void main(String[] args) {
		DailyHistoryAgent dh = new DailyHistoryAgent();
		dh.init();

		try {

			Flowable<List<Messages<?>>> repeat = Flowable.range(0, 10)
					.flatMap(id -> dh.getMessagesAsync(id).subscribeOn(Schedulers.computation())).buffer(10)
					.repeatUntil(() -> {
						return false;
					});
			Disposable subscribe = repeat.subscribe(foo -> {
				System.out.println("clazz: " + foo.size());
				List<JsonObject> list = new ArrayList<>();
				for (Messages<?> msgs : foo) {
					for (Message<?> message : msgs) {
						JsonObject messageObject = dh.getMessageObject(message);
						System.out.println(messageObject.get("messageId").getAsString());
						// System.out.println(Thread.currentThread()+" msg seq:
						// "+messageObject.get("messageId").getAsString()+" rx id: "+id);
					}
				}
			});
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void init() {
		// TODO pulsar consume
		JsonObject elasticProperties = new JsonObject();
		this.iysPulsarClient = PulsarInstance.getIysPulsarClient(pulsarEndpoint, pulsarAdminEndpoint, 0);
		for (int i = 0; i < 10; i++) {
			Consumer<?> tempConsumer = iysPulsarClient.createPulsarConsumer(dailyHistoryTopic,
					DAILY_HISTORY_SUBSCRIPTION, 100);
			consumerList.add(tempConsumer);
		}
//		this.elasticClient = ElasticInstance.getClient(ELASTIC_HOST, ELASTIC_PORT, new HashMap<String,String>(), 1,elasticProperties);

		//
	}

	private Flowable<Messages<?>> getMessagesAsync(int id) throws Exception {
		return Flowable.fromCallable(() -> {
			return getMessages(id);
		});
	}

	private Messages<?> getMessages(int id) throws PulsarClientException {
		// Messages<?> batchReceive = this.dailyHistoryConsumer.batchReceive();
		// TODO ???
		System.out.println("Getting messages with requester id: " + id);
		Messages<?> msgs = this.consumerList.get(id).batchReceive();
		System.out.println("Getting messages with requester id: " + id + " msg count: " + msgs.size());
		return msgs;
	}

	private JsonObject getMessageObject(Message<?> msg) {
		JsonObject json = null;
		try {
			json = new Gson().fromJson(new String(msg.getData()), JsonObject.class);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return json;
	}

}
