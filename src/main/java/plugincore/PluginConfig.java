package plugincore;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.SubnodeConfiguration;

public class PluginConfig {

	private SubnodeConfiguration configObj; 
	  
	
	public PluginConfig(SubnodeConfiguration configObj) throws ConfigurationException
	{
	    this.configObj = configObj;
	}
	public Boolean webDb()
	{
		if(configObj.getString("webdb").equals("1"))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	public String getPluginName()
	{
		return configObj.getString("pluginname");
	}
	public String getRegion()
	{
		return configObj.getString("region");
	}
	public boolean getLogConsumerEnabled()
	{
		if(configObj.getString("enablelogconsumer").equals("1"))
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	public int getWatchDogTimer()
	{
		return configObj.getInt("watchdogtimer");
	}
	
	public String getAMPQControlHost()
	{
		return configObj.getString("ampq_control_host");
	}
	
	public String getAMPQControlUser()
	{
		return configObj.getString("ampq_control_username");	    
	}
	
	public String getAMPQControlPassword()
	{
		return configObj.getString("ampq_control_password");	    
	}
	
	
}