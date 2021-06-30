package edu.utah.hci.aws.util;
import java.util.Date;
import java.util.Properties;
import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/*
 * From https://www.journaldev.com/2532/javamail-example-send-mail-in-java-smtp
 * 
 * Turn on less secure apps in the gmail account you're using for logging, see https://myaccount.google.com/lesssecureapps
 * 
 * */
public class Emailer {

	//fields
	private String fromEmail = null; //"david.austin.nix@gmail.com";
	private Session session = null;

	public Emailer(String fromEmail, String password, String smtpHost) {
		
		this.fromEmail = fromEmail;

		Properties props = new Properties();
		props.put("mail.smtp.host", smtpHost);
		props.put("mail.smtp.port", "587");
		props.put("mail.smtp.auth", "true");
		props.put("mail.smtp.starttls.enable", "true");

		//create Authenticator object to pass in Session.getInstance argument
		Authenticator auth = new Authenticator() {
			protected PasswordAuthentication getPasswordAuthentication() {
				return new PasswordAuthentication(fromEmail, password);
			}
		};
		session = Session.getInstance(props, auth);
	}


	public boolean sendEmail(String toEmail, String subject, String body){
		try {
			MimeMessage msg = new MimeMessage(session);
			//set message headers
			msg.addHeader("Content-type", "text/HTML; charset=UTF-8");
			msg.addHeader("format", "flowed");
			msg.addHeader("Content-Transfer-Encoding", "8bit");
			msg.setFrom(new InternetAddress(fromEmail, fromEmail));
			msg.setReplyTo(InternetAddress.parse(fromEmail, false));
			msg.setSubject(subject, "UTF-8");
			msg.setText(body, "UTF-8");
			msg.setSentDate(new Date());
			msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(toEmail, false));
			
			//send it
			Transport.send(msg);  
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}
	
	public static void main(String[] args) {
		Emailer emailer = new Emailer("david.austin.nix@gmail.com", "xxxxxxxxx", "smtp.gmail.com");
		boolean mailed = emailer.sendEmail("david.nix@hci.utah.edu", "TestSubject", "Hello World");
		System.out.println(mailed);
	}

}

