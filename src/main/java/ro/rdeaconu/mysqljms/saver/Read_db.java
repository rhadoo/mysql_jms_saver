package ro.rdeaconu.mysqljms.saver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import com.sun.messaging.jmq.io.Packet;
import com.sun.messaging.jmq.io.PacketType;
import com.sun.messaging.jmq.jmsservice.JMSPacket;
/**                 
 *
 * @author raducu.deaconu
 */
public class Read_db
{
 
	
	               public static void ziper(String messageID,String JMSHeaders,String textbodysize, String TextMessageBody,String UserProperties)
	                {
	                    StringBuilder sb = new StringBuilder(JMSHeaders);
	                    //.append("a");
	                    
	                    final File f = new File("ID"+messageID+"_TextMessage.zip");
	                    ZipOutputStream out = null;
						try {
							out = new ZipOutputStream(new FileOutputStream(f));
						} catch (FileNotFoundException e1) {
							// TODO Auto-generated catch block
							System.out.println ("Unable to create zip.file");
							e1.printStackTrace();
						}
	                    ZipEntry e = new ZipEntry("JMSHeaders.txt");
	                    try {
							out.putNextEntry(e);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							System.out.println ("Unable to add file to archive");
							e1.printStackTrace();
						}

	                    byte[] data = sb.toString().getBytes();
	                    try {
							out.write(data, 0, data.length);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
	                    try {
							out.closeEntry();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

	                    
	                    
	                    
	                    sb = new StringBuilder(textbodysize);
	                    e = new ZipEntry("textbodysize");
	                    try {
							out.putNextEntry(e);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

	                     data = sb.toString().getBytes();
	                    try {
							out.write(data, 0, data.length);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
	                    try {
							out.closeEntry();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
	                    
	                    
	                    sb = new StringBuilder(TextMessageBody);
	                    e = new ZipEntry("TextMessageBody.txt");
	                    try {
							out.putNextEntry(e);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

	                     data = sb.toString().getBytes();
	                    try {
							out.write(data, 0, data.length);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
	                    try {
							out.closeEntry();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
	                    
	                    
	                    sb = new StringBuilder(UserProperties);
	                    e = new ZipEntry("UserProperties.txt");
	                    try {
							out.putNextEntry(e);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}

	                     data = sb.toString().getBytes();
	                    try {
							out.write(data, 0, data.length);
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
	                    try {
							out.closeEntry();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
	                                 
	                    try {
							out.close();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}


	                }	              
	
  public static void main(String[] args)
  {
    try
    {
    	if(args.length < 5)
    	   {
    	   System.out.println("Proper Usage is: java -jar mysqlsaver.jar ip dbname user pass table queuename(all if unspecified)");
    	   System.exit(0);
    	   }	
    	
    	String IP=args[0];
    	String Dbname=args[1];
    	String user=args[2];
    	String pass=args[3];
    	String table=args[4];
    	/*
    	String IP = "10.52.13.140";
    	String Dbname = "jms";
    	String table = "MQMSG41Cjms";
    	String user = "jms";
    	String pass = "jms";
    	*/		
    	 System.out.println(IP);
    	 
    	String query = "SELECT * FROM "+table;
    	if (args.length == 6)
    			query = "SELECT * FROM "+table+" where DESTINATION_ID=\"Q:"+args[5]+"\"";
    	
    	// create our mysql database connection
      String myDriver = "com.mysql.jdbc.jdbc2.optional.MysqlDataSource";
      String myUrl = "jdbc:mysql://"+IP+":3306/"+Dbname;
      Class.forName(myDriver);
      Connection conn = DriverManager.getConnection(myUrl, user, pass);
       
      // our SQL SELECT query.
      // 
     
 
      // create the java statement
      ///Statement st = conn.createStatement();
      Statement st = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
      st.setFetchSize(Integer.MIN_VALUE);
      // execute the query, and get a java resultset
      ResultSet rs = st.executeQuery(query);
       
      // iterate through the java resultset
      while (rs.next())
      {
        String id = rs.getString("id");
       
        JMSPacket a;
        java.sql.Blob message = rs.getBlob("MESSAGE");
             
       // print the results
        InputStream is = message.getBinaryStream();
        Packet msg = new Packet(false);
        msg.generateTimestamp(false);
        msg.generateSequenceNumber(false);
        msg.readPacket(is);
        javax.jms.Message aici=msg.getMessage();
        JMSPacket msg2=null;
		msg2=msg;
     System.out.println ("MESSAGE ID:");   
     System.out.println (msg.getPacket().getMessageID());
     System.out.println ("HEADERS:"); 
     System.out.println ("PROPERTIES:"); 
     System.out.println ("MESSAGE:"); 
     System.out.println ("SIZE:"+msg.getMessageBodySize());
     //build files
     System.out.println("JMSHeaders.txt");
     /*
     System.out.println("\"JMSMessageID\",String=<@begin-@string@>ID:"+msg.getPacket().getMessageID()+"<@end-@string@>");
     System.out.println("\"JMSDestination\",String=<@begin-@string@>"+msg.getPacket().getDestination()+" : Queue<@end-@string@>");
     System.out.println("\"JMSReplyTo\",String=<@begin-@string@>"+msg.getPacket().getReplyTo()+"<@end-@string@>");
     System.out.println("\"JMSCorrelationID\",String=<@begin-@string@>"+msg.getPacket().getCorrelationID()+"<@end-@string@>");
     System.out.println("\"JMSDeliverMode\",Int=\"2\"");
     System.out.println("\"JMSPriority\",Int=\""+msg.getPacket().getPriority()+"\"");
     System.out.println("\"JMSExpiration\",Long=\""+msg.getPacket().getExpiration()+"\"");
     System.out.println("\"JMSType\",String=<@begin-@string@>null<@end-@string@>");
     System.out.println("\"JMSRedelivered\",Boolean=\""+msg.getPacket().getRedelivered()+"\"");
     System.out.println("\"JMSTimestamp\",Long=\""+msg.getPacket().getTimestamp()+"\"");
      */
     StringBuilder head = new StringBuilder("");
     head.append("\"JMSMessageID\",String=<@begin-@string@>ID:"+msg.getPacket().getMessageID()+"<@end-@string@>\n");
     head.append("\"JMSDestination\",String=<@begin-@string@>"+msg.getPacket().getDestination()+" : Queue<@end-@string@>\n");
     head.append("\"JMSReplyTo\",String=<@begin-@string@><@end-@string@>\n");
     head.append("\"JMSCorrelationID\",String=<@begin-@string@>"+msg.getPacket().getCorrelationID()+"<@end-@string@>\n");
     head.append("\"JMSDeliverMode\",Int=\"2\"\n");
     head.append("\"JMSPriority\",Int=\""+msg.getPacket().getPriority()+"\"\n");
     head.append("\"JMSExpiration\",Long=\""+msg.getPacket().getExpiration()+"\"\n");
     head.append("\"JMSType\",String=<@begin-@string@>null<@end-@string@>\n");
     head.append("\"JMSRedelivered\",Boolean=\""+msg.getPacket().getRedelivered()+"\"\n");
     head.append("\"JMSTimestamp\",Long=\""+msg.getPacket().getTimestamp()+"\"\n");
    // System.out.println(head.toString());
     String size=Integer.toString(msg.getMessageBodySize()); 
         
     StringBuilder uproperties = new StringBuilder("");
     System.out.println("UserProperties.txt");
     Iterator<Map.Entry>  it;
     Map.Entry            entry;
     it = msg.getPacket().getProperties().entrySet().iterator();
     while (it.hasNext()) {
         entry = it.next();
         if (entry.getValue().getClass()==String.class)
        //	 =<@begin-@string@>
         {	 
         //System.out.println("\""+entry.getKey().toString() + "\",String=<@begin-@string@>" +entry.getValue().toString()+"<@end-@string@>");
         uproperties.append("\""+entry.getKey().toString() + "\",String=<@begin-@string@>" +entry.getValue().toString()+"<@end-@string@>\n");
         }
         else
         {
        // System.out.println("\""+entry.getKey().toString() + "\","+entry.getValue().getClass().getSimpleName()+"=\"" +entry.getValue()+"\"");
         uproperties.append("\""+entry.getKey().toString() + "\","+entry.getValue().getClass().getSimpleName()+"=\"" +entry.getValue()+"\"\n");
         //System.out.println(entry.getValue().getClass().getSimpleName());
         }
         }
     //System.out.println(uproperties.toString());
     System.out.println("textbodysize.txt");
     System.out.println (msg.getMessageBodySize());
     System.out.println("TextMessageBody.txt");
     String tmessage="";
     if (size.equalsIgnoreCase("0"))
     {
    	 tmessage="";
     }
     else
     { byte [] a1=msg.getPacket().getMessageBodyByteArray();
     tmessage= new String(a1,0);
    }
       	 if (msg.getPacketType() == PacketType.OBJECT_MESSAGE)
    		 System.out.println("OBJECT MESSAGE"); else
    			 if (msg.getPacketType() == PacketType.TEXT_MESSAGE)
    	    		 System.out.println("TEXT MESSAGE");
    			 else
    				 if (msg.getPacketType() == PacketType.BYTES_MESSAGE)
        	    		 System.out.println("BYTES MESSAGE");else
    	         if (msg.getPacketType() == PacketType.STREAM_MESSAGE)
    		     System.out.println("STREAM MESSAGE");else
        	         if (msg.getPacketType() == PacketType.MAP_MESSAGE)
        		     System.out.println("MAP MESSAGE");
     
    	 ziper(msg.getPacket().getMessageID(),head.toString(),size,tmessage,uproperties.toString());
    	   }
      st.close();
    }
    catch (Exception e)
    {
      System.err.println("Got an exception! ");
     System.err.println(e.getMessage());
    }
  }
}