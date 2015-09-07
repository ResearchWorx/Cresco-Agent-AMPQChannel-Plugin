package channels;


import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.concurrent.ConcurrentHashMap;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import plugincore.PluginEngine;
import shared.MsgEvent;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;


public class AMPQAgentControlChannel  {

	private Connection connection;
	private QueueingConsumer consumer;
	private Marshaller CmdEventMarshaller;
	private Unmarshaller CmdEventUnmarshaller;
	private Channel channel;
	private ConcurrentHashMap<String,Channel> agentChannelMap;
	//
	
	public AMPQAgentControlChannel() throws Exception {
		
		//Create the AMPQ connection for each channel to share
		ConnectionFactory factory = new ConnectionFactory();
	    factory.setHost(PluginEngine.config.getAMPQControlHost());
		factory.setUsername(PluginEngine.config.getAMPQControlUser());
		factory.setPassword(PluginEngine.config.getAMPQControlPassword());
		factory.setConnectionTimeout(10000);
	    connection = factory.newConnection();
	    channel = connection.createChannel();
	    
	    //Create XML marshal objects
	    JAXBContext jaxbContext = JAXBContext.newInstance(MsgEvent.class);
	    CmdEventUnmarshaller = jaxbContext.createUnmarshaller();
	    CmdEventMarshaller = jaxbContext.createMarshaller();
	    
	    //Create hash for all agent channels
	    agentChannelMap = new ConcurrentHashMap<String,Channel>();    
	}
	
	public MsgEvent call(MsgEvent ce) throws Exception {  
		
		MsgEvent CmdResponse = null; //set response to null
	    
        //String requestQueueName = PluginEngine.config.getRegion() + "_control_" + ce.getMsgAgent(); 
		String requestQueueName = PluginEngine.region + "_control_" + ce.getMsgAgent(); 
        String replyQueueName = channel.queueDeclare().getQueue();
        
        consumer = new QueueingConsumer(channel);
        channel.basicConsume(replyQueueName, true, consumer);
        
	    String response = null;
		String corrId = java.util.UUID.randomUUID().toString();

	    BasicProperties props = new BasicProperties
	                                .Builder()
	                                .correlationId(corrId)
	                                .replyTo(replyQueueName)
	                                .build();

	    //unmarshal message
	    StringWriter CmdEventXMLString = new StringWriter();
        QName qName = new QName("com.researchworx.cresco.shared", "MsgEvent");
        JAXBElement<MsgEvent> rootM = new JAXBElement<MsgEvent>(qName, MsgEvent.class, ce);
        
        CmdEventMarshaller.marshal(rootM, CmdEventXMLString);
        
        channel.basicPublish("", requestQueueName, props, CmdEventXMLString.toString().getBytes());
        
        boolean hasRPCMessage = false;
	    while (!hasRPCMessage) {
	        QueueingConsumer.Delivery delivery = consumer.nextDelivery();
	        
	        response = new String(delivery.getBody());
            
            
	        if (delivery.getProperties().getCorrelationId().equals(corrId)) {
	            response = new String(delivery.getBody());
	            
	            InputStream stream = new ByteArrayInputStream(response.getBytes());		        
			    JAXBElement<MsgEvent> rootUm = CmdEventUnmarshaller.unmarshal(new StreamSource(stream), MsgEvent.class);		        
			    CmdResponse = rootUm.getValue();
			    //response = rce.getCmdType() + "," + rce.getCmdArg() + "," +  rce.getCmdResult();
			    hasRPCMessage = true;
	        }
	    }
	    
	    //channel.queueDelete(replyQueueName, true, true); //del if empty and unused
	    channel.queueDelete(replyQueueName); //del reguardless of status
	    
	    return CmdResponse; 
	}
	public void close() throws Exception {
	    connection.close();
	}
	
}
