package com.uphf.kafka.serializer;

import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uphf.kafka.pojo.CustomObject;

public class CustomSerializer implements Serializer<CustomObject> {

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {

	}

	@Override
	public byte[] serialize(String topic, CustomObject data) {
		byte[] retVal = null;
		ObjectMapper objectMapper = new ObjectMapper();
		try {
			retVal = objectMapper.writeValueAsString(data).getBytes();
		} catch (Exception exception) {
			System.out.println("Erreur de sérialization de l objet " + data);
		}
		return retVal;
	}

	@Override
	public void close() {

	}

}