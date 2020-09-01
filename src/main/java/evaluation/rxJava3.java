package evaluation;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.parallel.ParallelFlowable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class rxJava3 {

	public static void main(String[] args) {
		
//		Flowable<String> flow = Flowable.just("Shivam", "Ogu");
//		
//		
//		Flowable.range(1, 10)
//		.subscribeOn(Schedulers.computation())
//		.subscribe(name -> {
//			sleep(1L);
//			System.out.println(Thread.currentThread() + " "+name);
//		});
	
		
		
Flowable<Integer> flow = Flowable.fromStream(IntStream.range(1, 10).boxed());

ExecutorService newFixedThreadPool = Executors.newFixedThreadPool(5);

flow
.buffer(100)
.flatMap(data -> Flowable.just(data)
        .subscribeOn(Schedulers.from(newFixedThreadPool)))
          .subscribe(val -> System.out.println("Subscriber received "
                  + val + " on "
                  + Thread.currentThread()));
		
		try {
			Thread.sleep(100000L);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
//		ExecutorService consumerPool =Executors.newFixedThreadPool(1);
//		ExecutorService elasticPool =Executors.newFixedThreadPool(10);
//		ExecutorService producerPool =Executors.newFixedThreadPool(1);
//		
//		for (int k = 0; k < 2; k++) {
//		CompletableFuture.supplyAsync(() -> {
//			System.out.println("Consumer " +Thread.currentThread());
//		    return "Some Result";
//		},consumerPool).thenApplyAsync(result -> {
//			System.out.println("Elastic " +Thread.currentThread());
//		    return "Processed Result";
//		},elasticPool).thenAcceptAsync(result -> {
//			System.out.println("Producer " +Thread.currentThread());
//			System.out.println("Ack Send");
//		},producerPool);
//		
//		System.out.println(k);
//		System.out.println(Thread.currentThread());	
//		}
//		//test();
	
	}
	
	public static void test () {
		ExecutorService producerPool =Executors.newFixedThreadPool(2);
		for (int k = 0; k < 2; k++) {
			
			Set<Callable<Integer>> callable = new HashSet<>();
		for (int i=0;i<10;i++) {
			final int dummy = i;
			callable.add(() -> {
				Thread.sleep(1000);
				System.out.println("iteration "+dummy);
				return null;
		});}
		if (callable.size() > 0) {
			try {
				producerPool.invokeAll(callable);
				System.out.println(k+" "+Thread.currentThread());
			} catch (InterruptedException e) {
				//gLogger.error("", e);
			}
		}}
	
	}
	
	public static void sleep (Long val) {
		
	try {
		val = new Random().nextInt(5000)+1000L;
		Thread.sleep(val);
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
}
