package channels;
import java.io.IOException;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import plugincore.PluginEngine;
import shared.MsgEvent;


public class AMPQLogProducer implements Runnable {

    private Marshaller LogEventMarshaller;
    
    public AMPQLogProducer() 
    {
    
    }
    
    public void run() {
        
    	try
    	{
    		//XML Output
    		JAXBContext context = JAXBContext.newInstance(MsgEvent.class); 
    		LogEventMarshaller = context.createMarshaller();
    		LogEventMarshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    		
    	}
    	catch(Exception ex)
    	{
    		System.err.println("LogProducer Initialization Failed:  Exiting");
    		System.err.println(ex);
    		return; //don't kill app if log can't be established
    	}
    
    	PluginEngine.ProducerEnabled = true;
    	
    	String msg = "AMPQChannel LogProducer Started";
    	PluginEngine.clog.log(msg);
    	
    	while (PluginEngine.ProducerEnabled) 
    	{
    		try 
        	{
        		
        		if(PluginEngine.ProducerActive)
        		{
        			log();
        		}
        		
				Thread.sleep(100);
	        } 
        	catch (IOException e1) 
        	{
				e1.printStackTrace();
				System.out.println(e1);
			} 
        	catch (InterruptedException e) 
        	{
        		String msgStop = "AMPQChannel LogProducer Stopped";
            	PluginEngine.clog.log(msgStop);
            	
				try
		    	{
		    		if(PluginEngine.log_channel.isOpen())
		    		{
		    			PluginEngine.log_channel.close();
		    		}
		    	}
		    	catch(Exception ex)
		    	{
		    		String msgError = "AMPQChannel LogProducer Stopped";
		    		PluginEngine.clog.error(msgError);
		    	}		    	
			}
        	
        }
    	String msgStop = "AMPQChannel LogProducer Disabled";
    	PluginEngine.clog.log(msgStop);
    	
    	try 
    	{
			log(); //one last call
			
		} 
    	catch (IOException e) 
    	{
			e.printStackTrace();
		}
    	return;
    }

    private void log() throws IOException 
    {
    	
    	try
    	{
    		if(!PluginEngine.log_channel.isOpen())
    		{
    			System.out.println("Reconnecting Channel");
    			PluginEngine.log_channel = PluginEngine.connection.createChannel();
    			PluginEngine.log_channel.exchangeDeclare(PluginEngine.LOG_CHANNEL_NAME, "fanout");
    		}
    	}
    	catch(Exception ex)
    	{
    		System.out.println(ex);
    	}
    	try
    	{
    		
    		synchronized(PluginEngine.logOutQueue) 
    		{
    			
    			while ((!PluginEngine.logOutQueue.isEmpty())) 
    			{
    				
    				MsgEvent le = PluginEngine.logOutQueue.poll(); //get logevent
    			
    				//create rootXML for marshaler & create XML output
    				StringWriter LogEventXMLString = new StringWriter();
    				QName qName = new QName("com.researchworx.cresco.shared", "LogEvent");
    				JAXBElement<MsgEvent> root = new JAXBElement<MsgEvent>(qName, MsgEvent.class, le);
    				LogEventMarshaller.marshal(root, LogEventXMLString);
    	        
    				PluginEngine.log_channel.basicPublish(PluginEngine.LOG_CHANNEL_NAME, "", null, LogEventXMLString.toString().getBytes());
    				
    			}
    		}
    	}
    	catch(Exception ex)
    	{
    		System.out.println("AMPQChannel ERROR : AMPQProducer : " +  ex.toString());
    	}
         			
         			
    }
    
}