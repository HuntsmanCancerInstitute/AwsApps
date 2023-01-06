package edu.utah.hci.aws.apps.copy;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import edu.utah.hci.aws.util.Util;

/** This holds information related to the copy request the user has made.  Each copy request can actually contain one or more individual copy jobs*/
public class CopyRequest {
	
	private String inputCopyJob = null;
	private S3Copy s3Copy = null;
	
	//must be a s3:// uri
	private String sourceUri  = null;
	private String[] sourceBucketKey = null;
	
	//could be a s3:// uri or a local path
	private String destinationUri = null;
	private String[] destinationBucketKey = null;
	private File destinationFile = null;

	private ArrayList<String> errorMessage = new ArrayList<String>();
	private static final Pattern splitter = Pattern.compile("\\s*>\\s*");
	private boolean complete = false;
	
	private ArrayList<CopyJob> copyJobs = new ArrayList<CopyJob>();
	
	
	public CopyRequest (String line, S3Copy s3Copy) {
		this.s3Copy = s3Copy;
		inputCopyJob = line.trim();
		
		//split into two parts, the source and the destination, keep in mind that they might be doing a recursive copy of the entire bucket or a folder within
		String[] t = splitter.split(inputCopyJob);
		if (t.length == 2) {
			sourceUri = t[0];
			destinationUri = t[1];
			
			//check source
			if (sourceUri.startsWith("s3://")) sourceBucketKey = Util.splitBucketKey(sourceUri.substring(5));
			else errorMessage.add("ERROR: missing s3:// from source uri, "+inputCopyJob);
			
			//check destination
			if (destinationUri.startsWith("s3://")) destinationBucketKey = Util.splitBucketKey(destinationUri.substring(5));
			else {
				//must be a file
				destinationFile = new File(destinationUri);
				//directory? if needed make it.
				if (destinationUri.endsWith("/")) {
					if (destinationFile.exists()== false) {
						destinationFile.mkdirs();
						if (destinationFile.exists()== false) errorMessage.add("ERROR: failed to make the destination directory, "+inputCopyJob);
					}
					else if (destinationFile.isDirectory() == false)  errorMessage.add("ERROR: the destination uri ends with '/' but the local path isn't a directory, "+inputCopyJob+" "+destinationFile);
				}
				//get full path
				try {
					destinationFile = destinationFile.getCanonicalFile();
				} catch (IOException e) {
					errorMessage.add("ERROR: failed to find the canonical destination file or directory, "+inputCopyJob+" "+destinationFile);
				}
			}
		}
		else errorMessage.add("ERROR: copy job doesn't split on '>' into source and destination, "+inputCopyJob);
	}

	public String toString() {
		return sourceUri+"\t->\t"+destinationUri;
	}
	
	public boolean isPassing() {
		return errorMessage.size()==0;
	}

	public String[] getSourceBucketKey() {
		return sourceBucketKey;
	}

	public String[] getDestinationBucketKey() {
		return destinationBucketKey;
	}

	public ArrayList<String> getErrorMessage() {
		return errorMessage;
	}

	public boolean isComplete() {
		return complete;
	}

	public void setComplete(boolean complete) {
		this.complete = complete;
	}
	
	public void makeSimpleS3CopyJob(S3ObjectSummary sourceObject) throws Exception {
		copyJobs.add(new CopyJob(sourceObject, destinationBucketKey[0], destinationBucketKey[1], null, s3Copy));
	}

	private void makeS3CopyJob(S3ObjectSummary sourceObject,String trimmedOriginKey) throws Exception {
		String destinationKey = null;
		if (destinationBucketKey[1].length()==0) destinationKey = trimmedOriginKey;
		else destinationKey = destinationBucketKey[1] + trimmedOriginKey;
		copyJobs.add(new CopyJob(sourceObject, destinationBucketKey[0], destinationKey, null, s3Copy));
	}
	

	/**Returns numComplete, numToCopy, sizeComplete, sizeToCopy */
	public long[] fetchCopyJobNumbers() {
		long numComplete = 0;
		long numToCopy = 0;
		long sizeToCopy = 0;
		long sizeComplete = 0;
		for (CopyJob cj: this.copyJobs) {
			if (cj.isComplete()) {
				numComplete++;
				sizeComplete+= cj.getSource().getSize();
			}
			else {
				numToCopy++;
				sizeToCopy+= cj.getSource().getSize();
			}
		}
		if (numToCopy == 0) complete = true;
		return new long[] {numComplete, numToCopy, sizeComplete, sizeToCopy};
	}
	
	public void addIncompleteCopyJobs(ArrayList<CopyJob> cjs) {
		if (complete) return;
		boolean allComplete = true;
		for (CopyJob cj: copyJobs) {
			if (cj.isComplete() == false) {
				cjs.add(cj);
				allComplete = false;
			}
		}
		if (allComplete) complete = true;
	}
	
	public void makeCopyJobs() throws Exception {
		
		//fetch all source objects
		ArrayList<S3ObjectSummary> sourceObjects = s3Copy.fetchS3Objects(sourceBucketKey[0], sourceBucketKey[1], false);

		//any objects found?
		if (sourceObjects.size() == 0) {
			errorMessage.add("\tERROR, no source s3 objects? Aborting.");
		}
		
		//s3 to s3 copy?
		else if (destinationBucketKey != null) {
			
			boolean destEndsWithDir = false;
			if (destinationBucketKey[1].endsWith("/")|| destinationBucketKey[1].length()==0) destEndsWithDir = true;
			
			//is it a simple object to object copy?
			if (destEndsWithDir == false) {
				if (sourceObjects.size()==1) makeSimpleS3CopyJob(sourceObjects.get(0));
				else {
					errorMessage.add("\tERROR, more than one s3 objects found but the destination does not end with '/'");
					for (S3ObjectSummary o: sourceObjects) errorMessage.add("\t\t"+o.getKey());
				}
			}

			//OK it's a recursive copy to either the bucket root or a subfolder, trim the source prefixes
			else {
				String[] trimmedSourceKeys = new String[sourceObjects.size()];
				int lastSlashIndex = sourceBucketKey[1].lastIndexOf("/");
				if (lastSlashIndex == -1) lastSlashIndex = 0;
				else lastSlashIndex++;
				for (int i=0; i< trimmedSourceKeys.length; i++) trimmedSourceKeys[i] = sourceObjects.get(i).getKey().substring(lastSlashIndex);
				for (int i=0; i< trimmedSourceKeys.length; i++) {
					makeS3CopyJob(sourceObjects.get(i), trimmedSourceKeys[i]);
				}
			}
		}
		else {
			errorMessage.add("\tERROR, s3 copy to local not implemented.");
		}
	}
}
