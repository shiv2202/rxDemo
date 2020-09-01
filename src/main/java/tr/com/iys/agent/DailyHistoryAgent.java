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
import io.reactivex.rxjava3.schedulers.Schedulers;

public class DailyHistoryAgent {
	String pulsarEndpoint = "pulsar://" + "localhost" + ":"
			+ "6650";

	String pulsarAdminEndpoint = "http://" + "localhost" + ":"
			+ "8082";
	private static final String ELASTIC_HOST = "localhost";
	private static final int ELASTIC_PORT = 9200;
	private IysPulsarClient iysPulsarClient = null;
	private ElasticClient elasticClient = null; 
	
	public static final String dailyHistoryTopic = "iys.module.dailyhistoryagent.action.add-daily-history";
	public static final String DAILY_HISTORY_SUBSCRIPTION = "daily-history-subscription";
	
	private Consumer<?> dailyHistoryConsumer = null;
	
	public static void main(String[] args) {
		DailyHistoryAgent dh = new DailyHistoryAgent();
		dh.init();
		
		try {
			
			Flowable.range(0, 10).flatMap(id -> dh.getMessagesAsync(id).subscribeOn(Schedulers.io())).subscribe();
			
			//Thread.sleep(1000000L);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private  void init() {
		//TODO pulsar consume
		JsonObject elasticProperties = new JsonObject();
		this.iysPulsarClient = PulsarInstance.getIysPulsarClient(pulsarEndpoint, pulsarAdminEndpoint, 0);
		//this.elasticClient = ElasticInstance.getClient(ELASTIC_HOST, ELASTIC_PORT, new HashMap<String,String>(), 1,elasticProperties);
		
		this.dailyHistoryConsumer = iysPulsarClient.createPulsarConsumer(dailyHistoryTopic, DAILY_HISTORY_SUBSCRIPTION, 100);
	}
	
	private Flowable<Messages<?>> getMessagesAsync(int id) throws Exception {
		  return Flowable.fromCallable(() -> {return getMessages(id);});
	}
	
	private Messages<?> getMessages(int id) throws PulsarClientException {
		//Messages<?> batchReceive = this.dailyHistoryConsumer.batchReceive();
		//TODO ???
		 Messages<?> msgs = this.dailyHistoryConsumer.batchReceive();
		 for (Message<?> message : msgs) {
			 JsonObject messageObject = getMessageObject(message);
			System.out.println(Thread.currentThread()+" msg seq: "+messageObject.get("messageId").getAsString()+" rx id: "+id);
		}
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
