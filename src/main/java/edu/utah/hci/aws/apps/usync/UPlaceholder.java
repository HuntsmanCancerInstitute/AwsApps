package edu.utah.hci.aws.apps.usync;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

import edu.utah.hci.aws.util.Gzipper;
import edu.utah.hci.aws.util.Util;

/**Container for placeholder attributes, these may evolve over time so keep generic. File format is key = value, no whitespace or = signs in key or value.
 * At minimum include:
 *  region - aws region containing the bucket
 * 	bucket - aws bucket name 
 * 	key - path_aws random generated key
 * 	size - file size in bytes
 *  dateOfUpload 
 */
public class UPlaceholder {
	
	
	//fields
	private HashMap<String, String> attributes = new HashMap<String, String>();
	private File placeHolderFile = null;
	private File localFile = null;
	private boolean foundInS3 = false;
	private boolean s3SizeMatches = false;
	private boolean s3EtagMatches = false;
	private boolean restore = false;
	private String newPath = null;
	
	public static final String PLACEHOLDER_EXTENSION = ".S3"; 
	public static final String RESTORE_PLACEHOLDER_EXTENSION = ".S3.restore"; 
	public static final String PATH_NAME= "localPath"; 
	public static final String[] requiredKeys = {"region", "bucket", "key", "size", "etag", PATH_NAME};

	private ArrayList<String> errorMessages = null;
	private USync usync;
	
	/*STANDARD or null, DEEP_ARCHIVE, GLACIER
	 * Only the first can be downloaded directly.*/
	private String storageClass = "";
	
	
	public UPlaceholder() {};
	
	public UPlaceholder(File f, USync usync) throws IOException {
		attributes = Util.parseKeyValues(f);
		placeHolderFile = f;
		this.usync = usync;
		restore = f.getName().endsWith(RESTORE_PLACEHOLDER_EXTENSION);
	}
	
	public void writePlaceholder(File f) {
		Gzipper out = null;
		try {
			//check that the minimum requirements are in attributes
			for (String k: requiredKeys) {
				if (attributes.containsKey(k) == false)throw new IOException("Missing required attribute "+k+" in "+attributes);
			}
			//write it
			out = new Gzipper(f);
			out.println("# DO NOT DELETE - Amazon S3 Archive Info");
			out.println("# See https://github.com/HuntsmanCancerInstitute/AwsApps USync to restore archived S3 file to this location.");
			out.println("# "+Util.getDateTime());
			for (String key: attributes.keySet()) out.println(key+" = "+attributes.get(key));
			placeHolderFile = f;
			out.close();
		} catch (IOException e) {
			f.delete();
			usync.el("\nFailed to write placeholder file for "+f);
			usync.el(Util.getStackTrace(e));
			out.closeNoException();
			System.exit(0);
		}
		
	}
	
	public String getMinimalInfo() throws IOException {
		StringBuilder sb = new StringBuilder();
		sb.append("\tlocalPlaceholderFile = "+placeHolderFile+"\n");
		sb.append("\tstorageClass = "+storageClass+"\n");
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
	
	public boolean checkPaths(int pathDelete) throws IOException {
		String path = attributes.get("path");
		if (path == null) throw new IOException("Failed to find the 'path' attribute in "+placeHolderFile);
		String cp = placeHolderFile.getCanonicalPath();
		String tp = null;
		if (restore) tp = cp.substring(pathDelete, cp.length()- RESTORE_PLACEHOLDER_EXTENSION.length());
		else tp = cp.substring(pathDelete, cp.length()- PLACEHOLDER_EXTENSION.length());
		
System.out.println("PathCheck "+path+" vs "+tp);

		boolean matches = tp.equals(path);
		//save a newPath to indicate that this needs to be updated in the S3 Object and in this local placeholder
		if (matches == false ) newPath = tp;
		return matches;
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

	public boolean isRestore() {
		return restore;
	}

	public String getNewPath() {
		return newPath;
	}
}
