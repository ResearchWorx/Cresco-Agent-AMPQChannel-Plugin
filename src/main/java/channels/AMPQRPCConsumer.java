package channels;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;

import plugincore.PluginEngine;
import shared.MsgEvent;
import shared.MsgEventType;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.QueueingConsumer;


public class AMPQRPCConsumer implements Runnable {

	private Marshaller CmdEventMarshaller;
	private Unmarshaller CmdEventUnmarshaller;
    
	
		public AMPQRPCConsumer() 
		{

		}

	
	public void run()
	{
		try
		{
			QueueingConsumer consumer = new QueueingConsumer(PluginEngine.rpc_channel);
			
			PluginEngine.rpc_channel.basicConsume(PluginEngine.RPC_CHANNEL_NAME, false, consumer);

		
			//XML Output
			JAXBContext jaxbContext = JAXBContext.newInstance(MsgEvent.class);
			CmdEventUnmarshaller = jaxbContext.createUnmarshaller();
			CmdEventMarshaller = jaxbContext.createMarshaller();
			CmdEventMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
		
		
			PluginEngine.clog.log("AMPQRPCChannel Consumer Starting Channel=" + PluginEngine.rpc_channel);
			PluginEngine.RPCConsumerEnabled = true;
			while (true) 
			{
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				
				PluginEngine.rpc_channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false); //ack delivery
				
				String message = new String(delivery.getBody());
				
				InputStream stream = new ByteArrayInputStream(message.getBytes());		        
				JAXBElement<MsgEvent> rootUm = CmdEventUnmarshaller.unmarshal(new StreamSource(stream), MsgEvent.class);		        
		    
				MsgEvent ce = rootUm.getValue();
				
				PluginEngine.msgInQueue.offer(ce);
			}
		}
		catch(Exception ex)
		{
			PluginEngine.clog.error("ERROR : AMPQConsumer : " + ex.toString());
			return;
		}
	}
}
