package ch.ivx.tools;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import javax.activation.DataHandler;
import javax.activation.DataSource;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Multipart;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import com.github.sardine.Sardine;
import com.github.sardine.SardineFactory;
/**
 * 
 * Simple command line tool to query a database, save the result in csv format and either
 * publish it to a webdav directory or send it via email.
 * 
 * 
 * @author Jonas Tappolet jtappolet@gmail.com
 *
 */
public class DBMailDump {

	public static final String toolName = "DBRemoteDump";
	
	public static void main(String[] args){
		
		Options options = getOptions();
		
		CommandLineParser parser = new PosixParser();
		CommandLine cmd;
		try {
			cmd = parser.parse( options, args);
		} catch (ParseException e){
			printHelp();
			return;
		}
			if(cmd.hasOption("q") && ((cmd.hasOption("email") && cmd.hasOption("m")) || (cmd.hasOption("webdav") && cmd.hasOption("wh")))){

				System.out.println("Running query "+cmd.getOptionValue("q")+" ...");
				String fullPath = new File("").getAbsolutePath();
				String fileName = System.currentTimeMillis()+".csv";
				String fullFile = fullPath+System.getProperty("file.separator")+fileName;
				String destinationFileName = "db-export-"+new SimpleDateFormat("dd-mm-yyyy").format(new Date())+".csv";
				System.out.println("Temporary output file "+fullFile);
				boolean sqlOk = dbDump(cmd.hasOption("h")?cmd.getOptionValue("h"):"localhost",
						cmd.hasOption("d")?cmd.getOptionValue("d"):"imsma",
						cmd.hasOption("u")?cmd.getOptionValue("u"):"root",
						cmd.hasOption("p")?cmd.getOptionValue("p"):"password",
						cmd.getOptionValue("q"),
						fullFile,
						cmd.hasOption("s")?cmd.getOptionValue("s"):";");
				if(!sqlOk)
					return;
				
				if(cmd.hasOption("email")){
					
					sendMail(	cmd.hasOption("mh")?cmd.getOptionValue("mh"):"localhost",
								Integer.parseInt(cmd.hasOption("mp")?cmd.getOptionValue("mp"):"587"),
								cmd.hasOption("mu")?cmd.getOptionValue("mu"):"",
								cmd.hasOption("mpw")?cmd.getOptionValue("mpw"):"",
								cmd.getOptionValue("m"), 
								"db-dump@ivx.ch", "DB Dump", 
								"Find attached the result of the DB query as .csv file.", 
								fullFile,
								destinationFileName);
					
				}else if(cmd.hasOption("webdav")){
				
					sendWebdav(	cmd.getOptionValue("wh"),
								cmd.hasOption("wu")?cmd.getOptionValue("wu"):"",
								cmd.hasOption("wpw")?cmd.getOptionValue("wpw"):"",
								fullFile,
								destinationFileName);
				
				}
		
				new File(fileName).deleteOnExit();		
						
			}else{
				printHelp();
			}	

		
		
	}
	
	/**
	 * 
	 * Prints the argument description to stdout.
	 * 
	 */
	public static void printHelp(){
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "java -jar "+toolName+".jar", getOptions() );
	}
	
	/**
	 * 
	 * Dumps the result of a db query into a csv file.
	 * 
	 * @param host the MySQL server hostname / ip
	 * @param dbName the database name
	 * @param username the MySQL username
	 * @param password the MySQL password
	 * @param query the db query
	 * @param fileName the output filename
	 * @param separator the separator character to use in the csv file
	 * @return true if query execution was successful, false otherwise
	 */
	private static boolean dbDump(String host, String dbName, String username, String password, String query, String fileName, String separator){
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection conn = null;
			conn = DriverManager.getConnection("jdbc:mysql://"+host+":3306/"+dbName,username, password);
			//removed to support remote hosts
			//String q = (query.trim().endsWith(";")?query.trim().substring(0, query.trim().length()-1):query.trim())+" "+
			//"INTO OUTFILE '"+fileName+"' FIELDS ESCAPED BY '\"\"' TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\\r\\n';";
			PreparedStatement stmt = conn.prepareStatement(query);
			ResultSet rs = stmt.executeQuery();
			try {
				PrintStream out = new PrintStream(new File(fileName));
				for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
					out.print(rs.getMetaData().getColumnName(i)+(i!=rs.getMetaData().getColumnCount()?separator:System.getProperty("line.separator")));
				}
				while(rs.next()){
					for(int i=1;i<=rs.getMetaData().getColumnCount();i++){
						out.print(rs.getString(i)+(i!=rs.getMetaData().getColumnCount()?separator:System.getProperty("line.separator")));
					}
				}
				conn.close();
				out.close();
				return true;
			} catch (FileNotFoundException e) {
				System.out.println("Error writing file "+fileName);
				new File(fileName).delete();
				return false;
			}
		} catch (ClassNotFoundException e) {
			System.out.println("Error loading MySQL driver.");
			new File(fileName).delete();
			return false;
		} catch (SQLException e) {
			System.out.println("Error "+e.getMessage());
			new File(fileName).delete();
			return false;
		}
		
	}
	
	private static boolean sendWebdav(String webdavServer, String webdavUser, String webdavPassword, String filePath, String destinationFileName){
		
		Sardine sardine;
		if(webdavUser.isEmpty() && webdavPassword.isEmpty()){
			sardine = SardineFactory.begin();
		}else{
			sardine = SardineFactory.begin(webdavUser, webdavPassword);
		}
		
		try {
			InputStream fis = new FileInputStream(new File(filePath));
			String remoteUrl = webdavServer.endsWith("/")?webdavServer+destinationFileName:webdavServer+"/"+destinationFileName;
			System.out.println("Putting file "+remoteUrl);
			sardine.put(remoteUrl, fis);
			return true;
			
		} catch (FileNotFoundException e) {
			System.out.println("Error reading file "+destinationFileName);
			new File(filePath).delete();
			return false;
		} catch (IOException e) {
			System.out.println("Error putting file to webdav server. "+e.getMessage());
			new File(filePath).delete();
			return false;
		}
		
	}
	
	private static boolean sendMail(String smtpServer, int smtpPort, final String smtpUser, final String smtpPass, String to, String from, String subject, String body, String attPath, String attName){

		        // Get system properties
		        Properties props = System.getProperties();
		        props.put("mail.smtp.host", smtpServer);
		        props.put("mail.smtp.port", smtpPort);
		        props.put("mail.smtp.auth", "true");
		        //props.put("mail.smtp.starttls.enable", "true");
				props.put("mail.smtp.socketFactory.class","javax.net.ssl.SSLSocketFactory");
				Session session = Session.getDefaultInstance(props,
						new javax.mail.Authenticator() {
							protected PasswordAuthentication getPasswordAuthentication() {
								return new PasswordAuthentication(smtpUser,smtpPass);
							}
						});

		        
		        try {
		        	MimeMessage message = new MimeMessage(session);
					message.setFrom(new InternetAddress(from));
			        message.setRecipients(Message.RecipientType.TO, to);
			        message.setSubject(subject);
			        BodyPart messageBodyPart = new MimeBodyPart();
			        messageBodyPart.setText(body);
			        Multipart multipart = new MimeMultipart();
			        multipart.addBodyPart(messageBodyPart);
			        messageBodyPart = new MimeBodyPart();
			        DataSource source = new FileDataSource(attPath);
			        messageBodyPart.setDataHandler(new DataHandler(source));
			        messageBodyPart.setFileName(attName);
			        multipart.addBodyPart(messageBodyPart);
			        message.setContent(multipart);
		            Transport.send(message, message.getAllRecipients());
		            System.out.println("Mail Sent Successfully");
		            return true;
				} catch (AddressException e) {
					System.out.println("Invalid email address:"+to);
					return false;
				} catch (MessagingException e) {
					System.out.println("Error sending email. Is your SMTP server up? "+e.getMessage());
					//e.printStackTrace();
					return false;
				}

		    }
	
	
	private static Options getOptions(){

		Options options = new Options();
		options.addOption("email",false,"Send output via email.");
		options.addOption("webdav",false,"Send output via webdav.");
		options.addOption("h", true, "MySQL database server host (default: localhost)");
		options.addOption("n", true, "MySQL database name (default: imsma)");
		options.addOption("u", true, "MySQL database user name (default: root)");
		options.addOption("p", true, "MySQL dtabase password (default: password)");
		options.addOption("q", true, "MySQL query (required)");
		options.addOption("s", true, "CSV separator (default: ;)");	
		options.addOption("fp", true, "Filename prefix. Will be db-dump[fp]-dd-mm-yyyy.csv");
		options.addOption("m",true,"Receipient email address (required for email)");
		options.addOption("mh",true,"SMTP host name (default: localhost)");
		options.addOption("mp",true,"SMTP port (default: 587)");
		options.addOption("mpw",true,"SMTP password (default: empty)");
		options.addOption("mu",true,"SMTP user name (default: empty)");
		options.addOption("wh",true,"WebDAV host and path (required for webdav)");
		options.addOption("wu",true,"WebDAV user (default: empty)");
		options.addOption("wpw",true,"WebDAV user (default: empty)");
		options.addOption("wp",true,"WebDAV port (default: 80)");

		return options;
	}
	
	
	
}
