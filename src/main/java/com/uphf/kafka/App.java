package com.uphf.kafka;

import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.uphf.kafka.constants.IKafkaConstants;
import com.uphf.kafka.consumer.ConsumerCreator;
import com.uphf.kafka.producer.ProducerCreator;
import java.time.Duration;
import org.apache.kafka.common.errors.WakeupException;

public class App {
	public static void main(String[] args) {
		runProducer();
		runConsumer();
	}

        /**********************************
        * CONSOMATEUR
        **********************************/
	static void runConsumer() {
		//Consumer<Long, String> consumer = ConsumerCreator.createConsumer();
                Consumer<String, String> consumer = ConsumerCreator.createConsumer();

                // get a reference to the current thread
                final Thread mainThread = Thread.currentThread();

                // adding the shutdown hook
                Runtime.getRuntime().addShutdownHook(new Thread() 
                {
                    public void run() {
                        System.out.println("Detection d'un shutdown, quitter par l'appel de la methode wakeup() du consumer...");
                        consumer.wakeup();

                        // joindre le thread principal pour autoriser l'execution du code dasn le thread principal
                        try {mainThread.join();} catch (InterruptedException e) {e.printStackTrace();}
                                        }
                });

                
                
		int noMessageToFetch = 0;

                try{
		while (true) 
                    {
			//final ConsumerRecords<Long, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                        final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
			if (consumerRecords.count() == 0) 
                        {
				noMessageToFetch++;
				if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
					break;
				else
					continue;
			}

			consumerRecords.forEach(record -> {
				System.out.println("Record Key " + record.key());
				System.out.println("Record value " + record.value());
				System.out.println("Record partition " + record.partition());
				System.out.println("Record offset " + record.offset());
			});
			consumer.commitAsync();
                    }
                } catch (WakeupException e) {
                    System.out.println("Wake up exception!");
                    // nous ignorons cette exception qui est zattendu pour une fermeture propre
                } catch (Exception e) {
                    System.out.println("Unexpected exception \r\n"+ e);
                } finally {
                    consumer.close(); // this will also commit the offsets if need be.
                    System.out.println("Le consumer est maintenant proprement stoppé...");
                }
	}

        /**********************************
         * PRODUCTEUR
         **********************************/
	static void runProducer() {
		//Producer<Long, String> producer = ProducerCreator.createProducer();
                Producer<String, String> producer = ProducerCreator.createProducer();

		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			//final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,"Enregistrement N° " + index);
                        final ProducerRecord<String, String> record = new ProducerRecord<>(IKafkaConstants.TOPIC_NAME,"ID="+Integer.toString(index,10),"Enregistrement N° " + index);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Enregistrement envoyer avec clè " + index + " vers la partition " + metadata.partition()
						+ " Et l'offset " + metadata.offset());
			} catch (ExecutionException | InterruptedException e) {
				System.out.println("Erreur dans l'envoi de l'enregistrement");
				System.out.println(e);
			}
		}
	}
}
