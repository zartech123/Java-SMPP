import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import org.jsmpp.util.TimeFormatter;
import org.jsmpp.util.AbsoluteTimeFormatter;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.BindType;
import org.jsmpp.bean.DataCodings;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.bean.SMSCDeliveryReceipt;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.AbstractSession;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.Session;
import org.jsmpp.session.SessionStateListener;


public class SMPPSend {

	private static Logger logger;
	private static TimeFormatter timeFormatter = new AbsoluteTimeFormatter();
	private Properties propXML = new Properties();
	private ConnectionImpl connDb = new ConnectionImpl();
	private SMPPSession session;
	private String userName = "";
	private Object[][] results_inquiry = new Object[1][2];
	private Map <Integer, String> field_inquiry = new HashMap<Integer, String>();
	private String password = "";
	private String host = "";
	private int port = 0;
	private String sdc = "";
    private int idSmpp = 0;
	private long reconnectInterval = 5000L; // 5 seconds
	
	public static void main(String[] args) 
	{
    	System.setProperty("log4j.configurationFile",  System.getProperty("user.dir")+"/config/log4j2.xml");
    	logger = LogManager.getLogger(SMPPSend.class);
		SMPPSend smppSend = new SMPPSend();
	}

	public SMPPSend()
	{
		try
		{
			propXML.load(new FileInputStream(System.getProperty("user.dir")+"/conf/properties.prop"));
	
//			PropertyConfigurator.configure(System.getProperty("user.dir")+"/config/log4j2.properties");    			
			int j = 0;
			while(connDb.isConnected()==false && j < 10)
			{	
				connDb.setProperties(propXML);
				connDb.setUrl();
				connDb.setConnection();
				Thread.sleep(5000);
				j = j + 1;
			}				
			if(connDb.isConnected())
			{	
				this.sdc=this.propXML.getProperty("smpp.sdc").trim();
				this.host=this.propXML.getProperty("smpp.host").trim();
				this.userName=this.propXML.getProperty("smpp.userName").trim();
				this.password=this.propXML.getProperty("smpp.password").trim();
				this.port = Integer.parseInt(this.propXML.getProperty("smpp.port").trim());
				field_inquiry = new TreeMap<Integer, String>();
				field_inquiry.put(0, "id_smpp");
				field_inquiry.put(1, "text");
				field_inquiry.put(2, "msisdn");
				session = newSession();
			}	
		}	
		catch (InterruptedException e) 
		{
			logger.error(this.getClass().getName()+" "+e.getMessage());
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

	
	  private SMPPSession newSession() throws IOException 
	  {
		  session = new SMPPSession();
		  session.setEnquireLinkTimer(5000);
		  session.setTransactionTimer(10000);
		    session.connectAndBind(this.host, this.port, new BindParameter(BindType.BIND_TX, userName, password, "WIBPUSH", TypeOfNumber.UNKNOWN, NumberingPlanIndicator.UNKNOWN, null),10000l);
		    session.addSessionStateListener(new SessionStateListenerImpl());
			logger.info("SMPP Session : "+session.getSessionState());

			while(session.getSessionState()==SessionState.BOUND_TX)
			{	
				try
				{
					results_inquiry=connDb.getQuery("SELECT id_smpp, text, msisdn FROM smpp_mt WHERE id_state=0 order by created_date limit 1 ", new Object[]{0,"",""}, field_inquiry, new Object[]{},0);
					if(connDb.getRowCount(0)>0)
					{    		
						Date newDate = new Date(new Date().getTime() + 2 * 3600 * 1000);
						this.idSmpp = Integer.parseInt(results_inquiry[0][0].toString());
						final RegisteredDelivery registeredDelivery = new RegisteredDelivery();
				        registeredDelivery.setSMSCDeliveryReceipt(SMSCDeliveryReceipt.SUCCESS_FAILURE);					logger.info("Get ID Recipient "+this.idSmpp);
						connDb.updateQuery("update smpp_mt set id_state='1' where id_smpp=?",new Object[]{this.idSmpp});
						try {
			                String messageId = session.submitShortMessage("",
			                    TypeOfNumber.UNKNOWN, NumberingPlanIndicator.ISDN, this.sdc,
			                    TypeOfNumber.INTERNATIONAL, NumberingPlanIndicator.ISDN, results_inquiry[0][2].toString(),
			                    new ESMClass(), (byte)0, (byte)1,  null, timeFormatter.format(newDate),
			                    registeredDelivery, (byte)0, DataCodings.ZERO, (byte)0,
			                    results_inquiry[0][1].toString().getBytes());
			                logger.info("Message "+results_inquiry[0][1].toString()+" submitted to "+results_inquiry[0][2].toString());
							connDb.updateQuery("update smpp_mt set id_state='2' where id_smpp=?",new Object[]{this.idSmpp});
		        			connDb.updateQuery("replace traffic (id_traffic) values (3)",new Object[]{});
			            } 
						catch (PDUException e) 
						{
			                // Invalid PDU parameter
							connDb.updateQuery("update smpp_mt set id_state='3' where id_smpp=?",new Object[]{this.idSmpp});
			            	logger.error(this.getClass().getName()+" "+e.getMessage());
			            }
						catch (ResponseTimeoutException e) 
						{
			                // Response timeout
							connDb.updateQuery("update smpp_mt set id_state='3' where id_smpp=?",new Object[]{this.idSmpp});
			            	logger.error(this.getClass().getName()+" "+e.getMessage());
			            } 
						catch (InvalidResponseException e) 
						{
			                // Invalid response
							connDb.updateQuery("update smpp_mt set id_state='3' where id_smpp=?",new Object[]{this.idSmpp});
			            	logger.error(this.getClass().getName()+" "+e.getMessage());
			            } 
						catch (NegativeResponseException e) 
						{
			                // Receiving negative response (non-zero command_status)
							connDb.updateQuery("update smpp_mt set id_state='3' where id_smpp=?",new Object[]{this.idSmpp});
			            	logger.error(this.getClass().getName()+" "+e.getMessage());
			            } 
						catch (IOException e) 
						{
							connDb.updateQuery("update smpp_mt set id_state='3' where id_smpp=?",new Object[]{this.idSmpp});
			            	logger.error(this.getClass().getName()+" "+e.getMessage());
			            }				
					}
			        Thread.sleep(50);
				}     
				catch (InterruptedException e) 
				{
		                // Response timeout
	            	logger.error(this.getClass().getName()+" "+e.getMessage());
	            } 
			}	
			return session;
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
		        try {
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
