package channels;
import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import plugincore.PluginEngine;
import shared.MsgEvent;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;


public class AMPQLogConsumer implements Runnable {

	private QueueingConsumer consumer;
 	private Unmarshaller LogEventUnmarshaller;
    

 	public AMPQLogConsumer() throws JAXBException
	{
		JAXBContext jaxbContext = JAXBContext.newInstance(MsgEvent.class);
	    LogEventUnmarshaller = jaxbContext.createUnmarshaller();    
	}
	
	
	public void run() {
        
    	try
    	{
    		Channel channel = PluginEngine.connection.createChannel();
    		channel.exchangeDeclare(PluginEngine.LOG_CHANNEL_NAME, "fanout");
    		String queueName = channel.queueDeclare().getQueue();
    		channel.queueBind(queueName, PluginEngine.LOG_CHANNEL_NAME, "");

    		consumer = new QueueingConsumer(channel);
    		channel.basicConsume(queueName, true, consumer); 
    		
    		PluginEngine.LogConsumerEnabled = true;
    	}
    	catch(Exception ex)
    	{
    		System.err.println("LogConsumer Initialization Failed:  Exiting");
    		System.err.println(ex);
    		return;
    	}
    	
    	while (true) 
    	{
    		try 
        	{
        		if(PluginEngine.LogConsumerActive)
        		{
        			
        			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
        			String message = new String(delivery.getBody());
        			
        			InputStream stream = new ByteArrayInputStream(message.getBytes());		        
    			    JAXBElement<MsgEvent> rootUm = LogEventUnmarshaller.unmarshal(new StreamSource(stream), MsgEvent.class);		        
    			    MsgEvent ie  = rootUm.getValue();
    			    //generate logs
    			    ie.setMsgAgent(null); //set as a log message
    			    ie.setMsgPlugin(null); //set as a log message
    			    PluginEngine.msgInQueue.offer(ie);
        		}
        		
				Thread.sleep(100);
        	}
        	catch (Exception ex)
        	{
        		System.out.println("ERROR : Log Consumer : " + ex.toString());
        	}
        		    	
			
        }
    	
    }
	
}
