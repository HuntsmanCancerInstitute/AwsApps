package edu.utah.hci.aws.apps.gsync;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import edu.utah.hci.aws.util.Util;

/**Container for placeholder attributes, these may evolve over time so keep generic. File format is key = value, no whitespace or = signs in key or value.
 * At minimum include:
 * 	bucket - aws bucket name 
 * 	key - aws key, should be the canonical path at the time of upload, this should be the same as the current file canonical path unless it has been moved
 * 	size - file size in bytes
 */
public class Placeholder {
	
	
	//fields
	private HashMap<String, String> attributes = new HashMap<String, String>();
	private File placeHolderFile = null;
	private File localFile = null;
	private boolean keyMatchesLocalPlaceholderPath = false;
	private boolean foundInS3 = false;
	private boolean s3SizeMatches = false;
	private boolean s3EtagMatches = false;
	public static final String[] requiredKeys = {"bucket", "key", "size", "etag"};
	public static final String PLACEHOLDER_EXTENSION = ".S3.txt"; //don't change this
	public static final String RESTORE_PLACEHOLDER_EXTENSION = ".S3.txt.restore"; //don't change this
	public static final String DELETE_PLACEHOLDER_EXTENSION = ".S3.txt.delete"; //don't change this
	public static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\.S3\\.txt.*");
	private ArrayList<String> errorMessages = null;
	private GSync gsync;
	
	/**Null for unset,  STANDARD, RESTORE, DELETE.*/
	private String type = null;
	public static final String TYPE_PLACEHOLDER = "PLACEHOLDER";
	public static final String TYPE_RESTORE = "RESTORE";
	public static final String TYPE_DELETE = "DELETE";
	
	/*STANDARD or null, DEEP_ARCHIVE, GLACIER
	 * Only the first can be downloaded directly.*/
	private String storageClass = "";
	
	
	public Placeholder() {};
	
	public Placeholder(File f, String keyDelete, GSync gsync) throws IOException {
		attributes = Util.parseKeyValues(f);
		placeHolderFile = f;
		this.gsync = gsync;
		//set type
		if (f.getName().endsWith(PLACEHOLDER_EXTENSION)) type = TYPE_PLACEHOLDER;
		else if (f.getName().endsWith(RESTORE_PLACEHOLDER_EXTENSION)) type = TYPE_RESTORE;
		else if (f.getName().endsWith(DELETE_PLACEHOLDER_EXTENSION)) type = TYPE_DELETE;
		checkKeyMatchesPath(keyDelete);
	}
	
	public void writePlaceholder(File f) {
		PrintWriter out = null;
		try {
			//check that bucket, key, and size are in attributes
			for (String k: requiredKeys) {
				if (attributes.containsKey(k) == false)throw new IOException("Missing required attribute "+k+" in "+attributes);
			}
			//write it
			out = new PrintWriter( new FileWriter(f));
			out.println("# DO NOT DELETE - Amazon S3 Archive Info");
			out.println("# See https://github.com/HuntsmanCancerInstitute/AwsApps GSync to restore or delete the archived S3 file to this location.");
			out.println("# "+Util.getDateTime());
			for (String key: attributes.keySet()) out.println(key+" = "+attributes.get(key));
			placeHolderFile = f;
		} catch (IOException e) {
			f.delete();
			gsync.el("\nFailed to write placeholder file for "+f);
			gsync.el(Util.getStackTrace(e));
			System.exit(0);
		}
		finally {
			if (out!= null) out.close();
		}
	}
	
	public String getMinimalInfo() throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("\tlocalPlaceholderFile = "+placeHolderFile+"\n");
		sb.append("\tstorageClass = "+storageClass+"\n");
		sb.append("\ttype = "+type);
		for (String key : attributes.keySet()) {
			sb.append("\n\t");
			sb.append(key);
			sb.append(" = ");
			sb.append(attributes.get(key));
		}
		for (String m: errorMessages) {
			sb.append("\n\t");
			sb.append("error = ");
			sb.append(m);
		}
		sb.append("\n");
		
		return sb.toString();
	}
	
	public void checkKeyMatchesPath(String keyDelete) throws IOException {
		String key = attributes.get("key");
		if (key == null) throw new IOException("Failed to find the 'key' attribute in "+placeHolderFile);
		String cp = placeHolderFile.getCanonicalPath().replaceFirst(keyDelete, "");
		String tp = PLACEHOLDER_PATTERN.matcher(cp).replaceFirst("");
		keyMatchesLocalPlaceholderPath = tp.equals(key);
	}

	public HashMap<String, String> getAttributes() {
		return attributes;
	}
	public void setAttributes(HashMap<String, String> attributes) {
		this.attributes = attributes;
	}
	public File getPlaceHolderFile() {
		return placeHolderFile;
	}
	public void setPlaceHolderFile(File placeHolderFile) {
		this.placeHolderFile = placeHolderFile;
	}
	public void putAttribute(String key, String value) {
		attributes.put(key, value);
	}
	public String getAttribute(String key) {
		return attributes.get(key);
	}

	public boolean isFoundInS3() {
		return foundInS3;
	}

	public void setFoundInS3(boolean foundInS3) {
		this.foundInS3 = foundInS3;
	}

	public boolean isS3SizeMatches() {
		return s3SizeMatches;
	}

	public void setS3SizeMatches(boolean s3SizeMatches) {
		this.s3SizeMatches = s3SizeMatches;
	}

	public boolean isKeyMatchesLocalPlaceholderPath() {
		return keyMatchesLocalPlaceholderPath;
	}

	public boolean isS3EtagMatches() {
		return s3EtagMatches;
	}

	public void setS3EtagMatches(boolean s3EtagMatches) {
		this.s3EtagMatches = s3EtagMatches;
	}

	public File getLocalFile() {
		return localFile;
	}

	public void setLocalFile(File localFile) {
		this.localFile = localFile;
	}

	public String getType() {
		return type;
	}

	public ArrayList<String> getErrorMessages() {
		return errorMessages;
	}

	public void addErrorMessage(String message) {
		if (errorMessages == null) errorMessages = new ArrayList<String>();
		errorMessages.add(message);
	}

	public void setStorageClass(String storageClass) {
		this.storageClass = storageClass;
		
	}

	public String getStorageClass() {
		return storageClass;
	}
}
