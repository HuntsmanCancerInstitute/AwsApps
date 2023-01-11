package edu.utah.hci.aws.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.text.NumberFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferProgress;

/**Static utility methods.*/
public class Util {

	public static final Pattern COLON = Pattern.compile(":");
	public static final Pattern COMMA = Pattern.compile(",");
	public static final Pattern EQUALS = Pattern.compile("\\s*=\\s*");
	public static final Pattern FORWARD_SLASH = Pattern.compile("/");
	public static final Pattern WHITESPACE = Pattern.compile("\\s+");
	public static final Pattern QUESTION = Pattern.compile("\\?");
	
	public static double gigaBytes(File file) {
		double bytes = file.length();
		return gigaBytes(bytes);
	}
	
	public static double gigaBytes(double bytes) {
		return bytes / 1073741824.0;
	}
	
	public static int ageOfFileInDays (File f) throws IOException {
		FileTime t = Files.getLastModifiedTime(f.toPath());
		long ft = t.toMillis();
		long now = System.currentTimeMillis();
		long diff = now - ft;
		return (int) millisecToDays(diff);
	}
	
	public static double daysOld(Instant i) {
		long secLastMod = i.getEpochSecond();
		long secNow = Instant.now().getEpochSecond();
		long secOld = secNow - secLastMod;
		if (secOld < 0) return 0.0;
		return (double)secOld/86400.0;
	}
	
	/**Trims characters common to all from start of lines.*/
	public static String[] trimCommonStart(String[] lines){
		//trim front
		boolean go = true;
		int clip = 0;
		while (go){
			//check length
			if (lines[0].length() <= clip){
				clip--;
				break;
			}
			char test = lines[0].charAt(clip);
			//for each line
			for (int i=1; i< lines.length; i++){
				//check if long enough
				if (lines[i].length() <= clip) {
					clip--;
					go = false;
					break;
				}
				
				else if (test != lines[i].charAt(clip) ){
					go = false;
					break;
				}
			}
			if (go) clip++;
		}
		String[] clipped = new String[lines.length];
		for (int i=0; i< lines.length; i++){
			clipped[i] = lines[i].substring(clip);
		}
		return clipped;
	}

	
	/**Splits mybucket/path/to/file.txt to mybucket path/to/file.txt  MUST not start with s3://! */
	public static String[] splitBucketKey(String uri) {
		int i = uri.indexOf('/');
		if (i == -1) return new String[] {uri, ""};
		return new String[] {uri.substring(0, i), uri.substring(i+1)};
	}
	
	/**Parses a file containing key = value lines into a HashMap.
	 * Empty, #, [ lines are ignored. Subsequent duplicate keys are not extracted only the first occurrence.*/
	public static HashMap<String, String> parseKeyValues (File f) throws IOException{
		HashMap<String, String> attributes = new HashMap<String, String>();

		BufferedReader in = new BufferedReader( new FileReader(f));
		String line;
		while ((line = in.readLine()) != null) {
			line = line.trim();
			if (line.length() == 0 || line.startsWith("#") || line.startsWith("[")) continue;
			String[] tokens = Util.EQUALS.split(line);
			if (tokens.length != 2) {
				in.close();
				throw new IOException ("Failed to find a single key and value in -> "+line);
			}
			if (attributes.containsKey(tokens[0]) == false) attributes.put(tokens[0], tokens[1]);
		}
		in.close();
		return attributes;
	}
	
	/**Extracts the region from the ~/.aws/credentials file. Can only be one profile.*/
	public static String getRegionFromCredentials() throws IOException {
		String path = System.getProperty("user.home")+"/.aws/credentials";
		File cred = new File (path);
		if (cred.exists() == false) throw new IOException("Failed to find your ~/.aws/credentials file?");
		HashMap<String, String> a = Util.parseKeyValues(cred);
		String region = a.get("region");
		if (region == null) throw new IOException("Failed to find a 'region' key in ~/.aws/credentials file");
		return region;
	}
	
	/**Extracts the region from the ~/.aws/credentials file for a particular profile, returns null if nothing found.*/
	public static String getRegionFromCredentials(String profile) throws IOException {
		String path = System.getProperty("user.home")+"/.aws/credentials";
		File cred = new File (path);
		if (cred.exists() == false) throw new IOException("Failed to find your ~/.aws/credentials file?");
		String[] lines = Util.loadTxtFile(cred);
		String lookFor = "["+profile+"]";
		for (int i=0; i<lines.length; i++) {
			if (lines[i].equals(lookFor)) {
				for (int j=i+1; j< lines.length; j++) {
					if (lines[j].startsWith("[")) break;
					else if (lines[j].startsWith("region")) {
						String[] tokens = Util.EQUALS.split(lines[j]);
						if (tokens.length !=2) throw new IOException("Failed to split your region key in two, "+lines[j]); 
						return tokens[1];
					}
				}
			}
		}
		return null;
	}
	
	/**Converts milliseconds to days.*/
	public static double millisecToDays (long ms){
		double current = (double)ms;
		current = current/1000;
		current = current/60;
		current = current/60;
		current = current/24;
		return current;
	}
	
	/**Converts a double ddd.dddddddd to a user determined number of decimal places right of the .  */
	public static String formatNumber(double num, int numberOfDecimalPlaces){
		NumberFormat f = NumberFormat.getNumberInstance();
		f.setMaximumFractionDigits(numberOfDecimalPlaces);
		return f.format(num);
	}
	
	/**Returns a String separated by commas for each bin.*/
	public static String stringArrayToString(String[] s, String separator){
		if (s==null) return "";
		int len = s.length;
		if (len==1) return s[0];
		if (len==0) return "";
		StringBuffer sb = new StringBuffer(s[0]);
		for (int i=1; i<len; i++){
			sb.append(separator);
			sb.append(s[i]);
		}
		return sb.toString();
	}
	
	public static String getStackTrace (Throwable t) {
		StringWriter sw = new StringWriter();
		t.printStackTrace(new PrintWriter(sw));
		return t.getMessage()+"\n"+sw.toString();
	}
	
	/**Prints message to screen, then exits.*/
	public static void printErrAndExit (String message){
		System.err.println (message);
		System.exit(1);
	}
	/**Prints message to screen, then exits.*/
	public static void printExit (String message){
		System.out.println (message);
		System.exit(0);
	}
	
	/**
	 * Sends an email
	 * @param recipients, e.g. david.nix@hci.utah.edu
	 * @param subject
	 * @param message
	 * @param from, e.g. barack.o@hci.utah.edu
	 * @param smtpHost, e.g. hci-mail.hci.utah.edu
	 * @throws MessagingException
	 */
	public static void postMail(String recipients, String subject, String message, String from, String smtpHost) throws MessagingException {
		//set the host smtp address
		Properties props = new Properties();
		props.put("mail.smtp.host", smtpHost);

		//create some properties and get the default Session
		javax.mail.Session session = javax.mail.Session.getDefaultInstance(props, null);

		//create message
		Message msg = new MimeMessage(session);

		//set the from and to address
		InternetAddress addressFrom = new InternetAddress(from);
		msg.setFrom(addressFrom);
		msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(recipients, false));

		//setting the Subject and Content type
		msg.setSubject(subject);
		msg.setContent(message, "text/plain");
		Transport.send(msg);
	}
	
    /** Waits for the transfer to complete, catching any exceptions that occur.
     * @return true if successful upload, false if failed.
     */
    public static boolean waitForCompletion(Transfer xfer){
        try {
            xfer.waitForCompletion();
            if (xfer.getState().equals(Transfer.TransferState.Completed)) return true;
            return false;
        } catch (AmazonServiceException e) {
            System.err.println("Failed transfer - Amazon service error: " + e.getMessage());
            return false;
        } catch (AmazonClientException e) {
            System.err.println("Failed transfer - Amazon client error: " + e.getMessage());
            return false;
        } catch (InterruptedException e) {
            System.err.println("Failed transfer - Transfer interrupted: " + e.getMessage());
            return false;
        }
    }

    /**Uses ProcessBuilder to execute a cmd, combines standard error and standard out into one and returns their output.
     * @throws IOException */
    public static String[] executeViaProcessBuilder(String[] command, boolean printToStandardOut, Map<String,String> envVarToAdd) throws IOException{
    	ArrayList<String> al = new ArrayList<String>();

    	ProcessBuilder pb = new ProcessBuilder(command);
    	
    	//add enviro props?
    	if (envVarToAdd != null) pb.environment().putAll(envVarToAdd);
    	
    	pb.redirectErrorStream(true);
    	Process proc = pb.start();

    	BufferedReader data = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    	String line;
    	while ((line = data.readLine()) != null){
    		al.add(line);
    		if (printToStandardOut) System.out.println(line);
    	}
    	data.close();
    	String[] res = new String[al.size()];
    	al.toArray(res);
    	return res;
    }
    
	/**Uses ProcessBuilder to execute a cmd, combines standard error and standard out into one and printsToLogs if indicated.
	 * Returns exit code, 0=OK, >0 a problem
	 * @throws IOException */
	public static int executeReturnExitCode(String[] command, boolean printToLog, boolean printIfNonZero, Map<String,String> envVarToAdd) throws Exception{
		if (printToLog) pl ("Executing: "+Util.stringArrayToString(command, " "));
		ProcessBuilder pb = new ProcessBuilder(command);
		//add enviro props?
		if (envVarToAdd!=null) pb.environment().putAll(envVarToAdd);
		pb.redirectErrorStream(true);
		Process proc = pb.start();
		BufferedReader data = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = data.readLine()) != null) {
			if (printToLog) pl(line);
			sb.append(line);
			sb.append("\n");
		}
		int exitCode = proc.waitFor();
		if (exitCode !=0 && printIfNonZero) pl(sb.toString());
		return exitCode;
	}
	
	
	
	/**Executes a String of shell script commands via a temp file.  Only good for Unix.
	 * @throws IOException */
	public static String[] executeShellScript (String shellScript, File tempDirectory) throws IOException{
		//make shell file
		File shellFile = new File (tempDirectory, new Double(Math.random()).toString().substring(2)+"_TempFile.sh");
		shellFile.deleteOnExit();
		//write to shell file
		write(new String[] {shellScript}, shellFile);
		shellFile.setExecutable(true);
		//execute
		String[] cmd = new String[]{"bash", shellFile.getCanonicalPath()};
		String[] res = executeViaProcessBuilder(cmd, false, null);
		shellFile.delete();
		return res; 
	}
	
	/**Executes a String of shell script commands via a temp file.  Only good for Unix. Returns the exit code. Prints errors if encountered.
	 * @throws IOException */
	public static int executeShellScriptReturnExitCode (String shellScript, File tempDirectory) throws Exception{
		//make shell file
		File shellFile = new File (tempDirectory, new Double(Math.random()).toString().substring(2)+"_TempFile.sh");
		shellFile.deleteOnExit();
		//write to shell file
		write(new String[] {shellScript}, shellFile);
		shellFile.setExecutable(true);
		//execute
		String[] cmd = new String[]{"bash", shellFile.getCanonicalPath()};
		int res = executeReturnExitCode (cmd, false, true, null);
		shellFile.delete();
		return res; 
	}
	
	/**Fetches the full path from file paths that use . and ~ , may or may not actually exist.*/
	public static File fetchFullPath (String partialFilePath) throws IOException {
		File toReturn = null;
		
		// starts with .
		if (partialFilePath.startsWith(".")) {
			String workingDirWithSlash = System.getProperty("user.dir")+"/"; //  /Users/u0028003/Code/AwsApps/
			// ./ or .
			if (partialFilePath.equals(".") || partialFilePath.equals("./")) toReturn = new File(workingDirWithSlash);
			// ./Larry.txt
			else if (partialFilePath.startsWith("./")) toReturn = new File(workingDirWithSlash+ partialFilePath.substring(2));
			else throw new IOException("ERROR parsing full file path for -> "+partialFilePath);
		}
		
		// starts with ~
		else if (partialFilePath.startsWith("~")) {
			String homeDirWithSlash = System.getProperty("user.home")+"/"; //  /Users/u0028003/
			// ~/ or ~
			if (partialFilePath.equals("~") || partialFilePath.equals("~/")) toReturn = new File(homeDirWithSlash);
			// ~/Larry.txt
			else if (partialFilePath.startsWith("~/")) toReturn = new File(homeDirWithSlash+ partialFilePath.substring(2));
			else throw new IOException("ERROR parsing full file path for -> "+partialFilePath);
		}
		
		//either full path / or something else
		else toReturn = new File(partialFilePath);
		
		return toReturn.getCanonicalFile();
	}
	
	/**Loads a file's lines into a String[], won't save blank lines.*/
	public static String[] loadTxtFile(File file){
		ArrayList<String> a = new ArrayList<String>();
		try{
			BufferedReader in = new BufferedReader( new FileReader(file));
			String line;
			while ((line = in.readLine())!=null){
				line = line.trim();
				a.add(line);
			}
			in.close();
		}catch(Exception e){
			System.out.println("Prob loadFileInto String[]");
			e.printStackTrace();
		}
		String[] strings = new String[a.size()];
		a.toArray(strings);
		return strings;
	}

    // Prints progress while waiting for the transfer to finish.
    public static void showTransferProgress(Transfer xfer){
        // print an empty progress bar...
        printProgressBar(0.0);
        // update the progress bar while the xfer is ongoing.
        do {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                return;
            }
            TransferProgress progress = xfer.getProgress();
            double pct = progress.getPercentTransferred();
            eraseProgressBar();
            printProgressBar(pct);
        } while (xfer.isDone() == false);
        // print the final state of the transfer.
        TransferState xfer_state = xfer.getState();
        System.out.println(": " + xfer_state);
    }
    
    // prints a simple text progressbar: [#####     ]
    public static void printProgressBar(double pct){
        // if bar_size changes, then change erase_bar (in eraseProgressBar) to
        // match.
        final int bar_size = 40;
        final String empty_bar = "                                        ";
        final String filled_bar = "########################################";
        int amt_full = (int)(bar_size * (pct / 100.0));
        System.out.format("  [%s%s]", filled_bar.substring(0, amt_full),
              empty_bar.substring(0, bar_size - amt_full));
    }

    // erases the progress bar.
    public static void eraseProgressBar(){
        // erase_bar is bar_size (from printProgressBar) + 4 chars.
        final String erase_bar = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
        System.out.format(erase_bar);
    }

    /**Writes String[] as lines to the file.*/
	public static void write(String[] attributes, File p) throws IOException {
		PrintWriter out = new PrintWriter (new FileWriter (p));
		for (String s: attributes) out.println(s);
		out.close();
	}
	
    /**Writes String to the file.*/
	public static void write(String txt, File p) throws IOException {
		PrintWriter out = new PrintWriter (new FileWriter (p));
		out.println(txt);
		out.close();
	}
	
	public static void pl(Object ob) {
		System.out.println(ob.toString());
	}
	
	public static void el(Object ob) {
		System.err.println(ob.toString());
	}
	
	/** Fast & simple file copy. From GForman http://www.experts-exchange.com/M_500026.html
	 * Hit an odd bug with a "Size exceeds Integer.MAX_VALUE" error when copying a vcf file. -Nix.*/
	@SuppressWarnings("resource")
	public static boolean copyViaFileChannel(File source, File dest){
		FileChannel in = null, out = null;
		try {
			in = new FileInputStream(source).getChannel();
			out = new FileOutputStream(dest).getChannel();
			long size = in.size();
			MappedByteBuffer buf = in.map(FileChannel.MapMode.READ_ONLY, 0, size);
			out.write(buf);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		} finally {
			try {
				if (in != null) in.close();
				if (out != null) out.close();
			} catch (IOException e1) {}
		}
		return true;
	}
	
	public static final String[] months = {"Jan","Feb","Mar","Apr","May","June","July", "Aug","Sept","Oct","Nov","Dec"};
	
	/**Returns a nicely formated time, 15 May 2004 21:53 */
	public static String getDateTime(){
		GregorianCalendar c = new GregorianCalendar();
		int minutes = c.get(Calendar.MINUTE);
		String min;
		if (minutes < 10) min = "0"+minutes;
		else min = ""+minutes;
		return c.get(Calendar.DAY_OF_MONTH)+" "+months[c.get(Calendar.MONTH)]+" "+ c.get(Calendar.YEAR)+" "+c.get(Calendar.HOUR_OF_DAY)+":"+min;
	}
	
	/**Returns a nicely formated time, with given separator between the date and the time, 15May2004 21 53 */
	public static String getDateTime(String separator){
		GregorianCalendar c = new GregorianCalendar();
		int minutes = c.get(Calendar.MINUTE);
		String min;
		if (minutes < 10) min = "0"+minutes;
		else min = ""+minutes;
		return c.get(Calendar.DAY_OF_MONTH)+months[c.get(Calendar.MONTH)]+ c.get(Calendar.YEAR)+separator+c.get(Calendar.HOUR_OF_DAY)+separator+min;
	}
	
	public static String getMinutesSinceEpoch(){
		double ms = System.currentTimeMillis();
		double min = ms/60000.0;
		Long rounded = Math.round(min);
		return rounded.toString();
	}
	
	/**Fetch random string constisting of ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890*/
	private static String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	public static String getRandomString(int length) {
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) { // length of the random string.
            int index = (int) (rnd.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }
	
	/**Attempts to delete a directory and it's contents.
	 * Returns false if all the file cannot be deleted or the directory is null.
	 * Files contained within scheduled for deletion upon close will cause the return to be false.*/
	public static void deleteDirectory(File dir){
		if (dir == null || dir.exists() == false) return;
		if (dir.isDirectory()) {
			File[] children = dir.listFiles();
			for (int i=0; i<children.length; i++) {
				deleteDirectory(children[i]);
			}
			dir.delete();
		}
		dir.delete();
	}
	
	/**Extracts the full path file names of all the files in a given directory with a given extension (ie txt or .txt).
	 * If the dirFile is a file and ends with the extension then it returns a File[] with File[0] the
	 * given directory. Returns null if nothing found. Case insensitive.*/
	public static File[] extractFiles(File dirOrFile, String extension){
		if (dirOrFile == null || dirOrFile.exists() == false) return null;
		File[] files = null;
		Pattern p = Pattern.compile(".*"+extension+"$", Pattern.CASE_INSENSITIVE);
		Matcher m;
		if (dirOrFile.isDirectory()){
			files = dirOrFile.listFiles();
			int num = files.length;
			ArrayList<File> chromFiles = new ArrayList<File>();
			for (int i=0; i< num; i++)  {
				m= p.matcher(files[i].getName());
				if (m.matches()) chromFiles.add(files[i]);
			}
			files = new File[chromFiles.size()];
			chromFiles.toArray(files);
		}
		else{
			m= p.matcher(dirOrFile.getName());
			if (m.matches()) {
				files=new File[1];
				files[0]= dirOrFile;
			}
		}
		if (files != null) Arrays.sort(files);
		return files;
	}

	public static String arrayListToString(@SuppressWarnings("rawtypes") ArrayList al, String delimiter) {
		int size = al.size();
		if (size == 0) return "";
		else if (size == 1) return al.get(0).toString();
		StringBuilder sb = new StringBuilder(al.get(0).toString());
		for (int i=1; i< size; i++) {
			sb.append(delimiter);
			sb.append(al.get(i));
		}
		return sb.toString();
	}
	
	/*From https://stackoverflow.com/questions/3758606/how-can-i-convert-byte-size-into-a-human-readable-format-in-java*/
	public static String formatSize(long v) {
	    if (v < 1024) return v + " B";
	    int z = (63 - Long.numberOfLeadingZeros(v)) / 10;
	    return String.format("%.1f %sB", (double)v / (1L << (z*10)), " KMGTPE".charAt(z));
	}

}
