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

public class App {
	public static void main(String[] args) {
		runProducer();
		runConsumer();
	}

	static void runConsumer() {
		Consumer<Long, String> consumer = ConsumerCreator.createConsumer();

		int noMessageToFetch = 0;

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
			if (consumerRecords.count() == 0) {
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
		consumer.close();
	}

	static void runProducer() {
		Producer<Long, String> producer = ProducerCreator.createProducer();

		for (int index = 0; index < IKafkaConstants.MESSAGE_COUNT; index++) {
			final ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(IKafkaConstants.TOPIC_NAME,
					"Enregistrement N° " + index);
			try {
				RecordMetadata metadata = producer.send(record).get();
				System.out.println("Enregistrement envoyer avec clè " + index + " vers la partition " + metadata.partition()
						+ " Et l'offset " + metadata.offset());
			} catch (ExecutionException e) {
				System.out.println("Erreur dans l'envoi de l'enregistrement");
				System.out.println(e);
			} catch (InterruptedException e) {
				System.out.println("Erreur dans l'envoi de l'enregistrement");
				System.out.println(e);
			}
		}
	}
}