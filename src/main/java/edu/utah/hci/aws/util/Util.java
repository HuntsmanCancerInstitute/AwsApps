package edu.utah.hci.aws.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.attribute.FileTime;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.regex.Pattern;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;

import com.amazonaws.services.s3.transfer.TransferProgress;

/**Static utility methods.*/
public class Util {

	public static final Pattern COMMA = Pattern.compile(",");
	public static final Pattern EQUALS = Pattern.compile("\\s*=\\s*");
	
	public static double gigaBytes(File file) {
		double bytes = file.length();
		double kilobytes = (bytes / 1024);
		double megabytes = (kilobytes / 1024);
		double gigabytes = (megabytes / 1024);
		return gigabytes;
	}
	
	public static int ageOfFileInDays (File f) throws IOException {
		FileTime t = Files.getLastModifiedTime(f.toPath());
		long ft = t.toMillis();
		long now = System.currentTimeMillis();
		long diff = now - ft;
		return (int) millisecToDays(diff);
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
    public static String[] executeViaProcessBuilder(String[] command, boolean printToStandardOut) throws IOException{
    	ArrayList<String> al = new ArrayList<String>();

    	ProcessBuilder pb = new ProcessBuilder(command);
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
	
	/**Executes a String of shell script commands via a temp file.  Only good for Unix.
	 * @throws IOException */
	public static String[] executeShellScript (String shellScript, File tempDirectory) throws IOException{
		//make shell file
		File shellFile = new File (tempDirectory, new Double(Math.random()).toString().substring(2)+"_TempFile.sh");
		shellFile.deleteOnExit();
		//write to shell file
		write(new String[] {shellScript}, shellFile);
		//set permissions for execution
		String[] cmd = {"chmod", "777", shellFile.getCanonicalPath()};
		String[] res = executeViaProcessBuilder(cmd, true);
		if (res == null || res.length !=0 ) throw new IOException("Failed to execute "+shellScript);
		//execute
		cmd = new String[]{"bash", shellFile.getCanonicalPath()};
		res = executeViaProcessBuilder(cmd, true);
		return res; 
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

}
