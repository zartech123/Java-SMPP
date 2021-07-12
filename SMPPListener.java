import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.jsmpp.bean.AlertNotification;
import org.jsmpp.bean.BindType;
import org.jsmpp.bean.DataSm;
import org.jsmpp.bean.DeliverSm;
import org.jsmpp.bean.DeliveryReceipt;
import org.jsmpp.bean.MessageType;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.extra.ProcessRequestException;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.DataSmResult;
import org.jsmpp.session.MessageReceiverListener;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.Session;
import org.jsmpp.session.SessionStateListener;
import org.jsmpp.util.InvalidDeliveryReceiptException;

public class SMPPListener {

	private static Logger logger;
	private static Logger logger2;
	private Properties propXML = new Properties();
	private ConnectionImpl connDb = new ConnectionImpl();
	private SMPPSession session = new SMPPSession();
	private String userName = "";
	private String password = "";
	private String host = "";
	private int port = 0;
	private long reconnectInterval = 5000L; // 5 seconds
	private Object[][] results_inquiry2 = new Object[10][1];
	private Map <Integer, String> field_inquiry2 = new HashMap<Integer, String>();
	
	public static void main(String[] args) 
	{
    	System.setProperty("log4j.configurationFile",  System.getProperty("user.dir")+"/config/log4j.xml");
    	logger = LogManager.getLogger("log");
    	logger2 = LogManager.getLogger("cdr");
		SMPPListener smppListener = new SMPPListener();
		while (true) 
		{
		      try 
		      {
		        Thread.sleep(1000);
		      }
		      catch (InterruptedException e) 
		      {
		      }
	    }

	}

	public SMPPListener()
	{
		try
		{
			propXML.load(new FileInputStream(System.getProperty("user.dir")+"/conf/properties.prop"));
	
//			PropertyConfigurator.configure(System.getProperty("user.dir")+"/config/log4j3.properties");    			
			this.host=this.propXML.getProperty("smpp.host").trim();
			this.userName=this.propXML.getProperty("smpp.userName").trim();
			this.password=this.propXML.getProperty("smpp.password").trim();
			this.port = Integer.parseInt(this.propXML.getProperty("smpp.port").trim());
			logger.info("SMPP Session : "+session.getSessionState());
			field_inquiry2 = new TreeMap<Integer, String>();
			field_inquiry2.put(0, "email");
			session = newSession();
		}	
		catch (FileNotFoundException e) 
		{
			logger.error(this.getClass().getName()+" "+e.getMessage());
		}	
		catch (IOException e) 
		{
			logger.error(this.getClass().getName()+" "+e.getMessage());
		}	
	}

	
	  private SMPPSession newSession() throws IOException {
		    SMPPSession tmpSession = new SMPPSession(this.host, this.port, new BindParameter(BindType.BIND_RX, userName, password, "WIBPUSH", TypeOfNumber.UNKNOWN, NumberingPlanIndicator.UNKNOWN, null));
		    tmpSession.setEnquireLinkTimer(5000);
		    tmpSession.setTransactionTimer(10000);
		    tmpSession.addSessionStateListener(new SessionStateListenerImpl());
			logger.info("SMPP Session : "+tmpSession.getSessionState());

			if(tmpSession.getSessionState()==SessionState.BOUND_RX)
			{	
				tmpSession.setMessageReceiverListener(new MessageReceiverListener() 
				{
		            public void onAcceptDeliverSm(DeliverSm deliverSm)
		                    throws ProcessRequestException {
		                if (MessageType.SMSC_DEL_RECEIPT.containedIn(deliverSm.getEsmClass())) 
		                {
		                    try 
		                    {
		                        DeliveryReceipt delReceipt = deliverSm.getShortMessageAsDeliveryReceipt();
		                        long id = Long.parseLong(delReceipt.getId()) & 0xffffffff;
		                        String messageId = Long.toString(id, 16).toUpperCase();
		                        logger.info("Receiving Delivery Receipt for Message '"+messageId+"' : "+delReceipt);
		                    } 
		                    catch (InvalidDeliveryReceiptException e) 
		                    {
		            			logger.error(this.getClass().getName()+" "+e.getMessage());    		
		                    }
		                } 
		                else 
		                {
		                	int j = 0;
		        			while(connDb.isConnected()==false && j<10)
		        			{	
		        				connDb.setProperties(propXML);
		        				connDb.setUrl();
		        				connDb.setConnection();
		        				try
		        				{
		        					Thread.sleep(5000);
		        				}
		        				catch (InterruptedException e) 
		        				{
		        					logger.error(this.getClass().getName()+" "+e.getMessage());
		        				}	
		        				j = j +1;
		        			}
		        			String text = new String(deliverSm.getShortMessage());
		        			String text_split[] = text.split(",");
		        			String msisdn = deliverSm.getSourceAddr();
		                    logger.info("Receiving Message : ["+text+"] From : ["+msisdn+"] To : ["+deliverSm.getDestAddress()+"]");
		                    if(connDb.isConnected())
		                    {	
			        			if(text_split.length==3)
			        			{
			        			
			        			}
			        			else if(text_split.length==2)
			        			{	
				        			int type = 0;
				        			if(text_split[1].compareTo("Received")==0)
				        			{
				        				type=0;
				        			}
				        			else if(text_split[1].compareTo("Layer1")==0)
				        			{
				        				type=1;
				        			}
				        			else if(text_split[1].compareTo("Layer2")==0)
				        			{
				        				type=2;
				        			}
				        			else if(text_split[1].compareTo("Layer3")==0)
				        			{
				        				type=3;
				        			}
				        			int id_campaign = Integer.parseInt(text_split[0]);
				        			logger2.info(msisdn+"|"+deliverSm.getDestAddress()+"|"+id_campaign+"|"+type);	
				        			connDb.updateQuery("insert into smpp_mo (created_date,text,id_campaign,type,msisdn) values (now(),?,?,?,?)",new Object[]{text,id_campaign,type,msisdn});
				        			connDb.updateQuery("replace traffic (id_traffic) values (2)",new Object[]{});
			        			}    	
			        			connDb.Close();
		                    }	
		                }
		            }
		            
		            public void onAcceptAlertNotification(AlertNotification alertNotification) 
		            {
		            	logger.info("AlertNotification not implemented");
		            }
		            
		            public DataSmResult onAcceptDataSm(DataSm dataSm, Session source)
		                    throws ProcessRequestException 
		            {
		                // TODO Auto-generated method stub
		            	logger.info("DataSm not implemented");
		                return null;
		            }
		        });			
			}	
			return tmpSession;
		  }

		  /**
		   * Get the session. If the session still null or not in bound state, then IO exception will be thrown.
		   *
		   * @return the valid session.
		   * @throws IOException if there is no valid session or session creation is invalid.
		   */
		  private SMPPSession getSession() throws IOException {
		    if (session == null) {
		      logger.info("Initiate session for the first time to "+this.host+":"+this.port);
		      session = newSession();
		    }
		    else if (!session.getSessionState().isBound()) {
		      throw new IOException("We have no valid session yet");
		    }
		    return session;
		  }

		  /**
		   * Reconnect session after specified interval.
		   *
		   * @param timeInMillis is the interval.
		   */
		  private void reconnectAfter(final long timeInMillis) {
		    new Thread() {
		      @Override
		      public void run() {
		        logger.info("Schedule reconnect after "+timeInMillis+" millis");
		        try 
		        {
		          Thread.sleep(timeInMillis);
		        }
		        catch (InterruptedException e) {
		        }

		        int attempt = 0;
		        while (session == null || session.getSessionState().equals(SessionState.CLOSED)) {
		          try 
		          {
		            logger.info("Reconnecting attempt #"+attempt+" ...");
		            attempt = attempt + 1;
		            session = newSession();
		          }
		          catch (IOException e) 
		          {
          			logger.error(this.getClass().getName()+" "+e.getMessage());    		
		            // wait for a second
		            try 
		            {
		              Thread.sleep(1000);
		            }
		            catch (InterruptedException e2) 
		            {
            			logger.error(this.getClass().getName()+" "+e2.getMessage());    				            	
		            }
		          }
		        }
		      }
		    }.start();
		  }

		  /**
		   * This class will receive the notification from {@link SMPPSession} for the state changes. It will schedule to
		   * re-initialize session.
		   *
		   * @author uudashr
		   */
		  private class SessionStateListenerImpl implements SessionStateListener {
		    public void onStateChange(SessionState newState, SessionState oldState, Session source) {
		      logger.info("State changed from "+oldState+" to "+newState);
		      if (newState.equals(SessionState.CLOSED)) {
		        logger.info("Session "+source.getSessionId()+" CLOSED");
		        reconnectAfter(reconnectInterval);
		      }  
		    }
		  }	
	
}
