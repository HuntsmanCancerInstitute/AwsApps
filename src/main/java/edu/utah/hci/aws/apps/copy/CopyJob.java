package edu.utah.hci.aws.apps.copy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.utah.hci.aws.util.Util;

public class CopyJob {

	//fields
	private S3ObjectSummary source;
	private String destinationBucket;
	private String destinationKey;
	private File destinationFile;
	private S3Copy s3Copy;
	private boolean complete = false;
	private boolean error = false;
	
	public CopyJob (S3ObjectSummary source, String destinationBucket, String destinationKey, File destinationFile, S3Copy s3Copy) throws Exception {
		this.source = source;
		this.destinationBucket = destinationBucket;
		this.destinationKey = destinationKey;
		this.destinationFile = destinationFile;
		this.s3Copy = s3Copy;
		
		s3Copy.p("\t"+toString());
		String size = "\t"+Util.formatSize(source.getSize());
		
		//does the destination already exist and matches size?
		//destination file
		if (destinationFile != null) {
			if (destinationFile.exists()) {
				if (destinationFile.length() == source.getSize()) complete = true;
				else {
					s3Copy.pl("\tDestination exists but different size, will overwrite"+size);
					return;
				}
			}
			else {
				s3Copy.pl("\tDownload to local"+size);
				return;
			}
		}
		//destination s3 object
		else {
			ArrayList<S3ObjectSummary> destObj = s3Copy.fetchS3Objects(destinationBucket, destinationKey, true);
			if (destObj.size()==1) {
				if (destObj.get(0).getSize() == source.getSize()) complete = true;
				else {
					s3Copy.pl("\tDestination exists but different size, will overwrite"+size);
					return;
				}
			}
		}
		
		if (complete) s3Copy.pl("\tExists, skipping"+size);
		else s3Copy.pl("\tTo copy"+size);
	}
	
	public String toString() {
		String dest = null;
		if (destinationFile != null)
			try {
				dest = destinationFile.getCanonicalPath();
			} catch (IOException e) {
				s3Copy.el("ERROR: fetching canonical path for "+destinationFile);
				e.printStackTrace();
				error = true;
			}
		else dest = "s3://"+destinationBucket+"/"+destinationKey;
		return "s3://"+source.getBucketName()+"/"+source.getKey()+" -> "+dest;
	}
	
	public String restoreCopy(CopyJobWorker cjw) throws Exception {
		if (complete) return "Complete";

		//does it need to be restored? archived?
		if (source.getStorageClass().equals("STANDARD") == false) {
			//pull meta data
			ObjectMetadata metaData = s3Copy.tryGetObjectMetadata(source.getBucketName(),  source.getKey());
			
			//restore placed?
			Boolean or = metaData.getOngoingRestore();
			if (or != null) {
				//yes, is it ready for copying
				if (or==false) return copyIt(cjw);
				//nope, must wait
				return "Restoring"; 
			}
			//null so place a restore request
			else {
				cjw.restore(source.getBucketName(), source.getKey());
				return "Restore request placed";
			}
		}
		//not archived so copy it
		else return copyIt(cjw);
		
	}

	private String copyIt(CopyJobWorker cjw) throws Exception {
		//destination is s3?
		if (destinationFile == null) {
			cjw.s3Copy(source.getBucketName(), source.getKey(), destinationBucket, destinationKey);
			complete = true;
			return "Copied";
		}
		//destination is local
		cjw.tryDownload(source.getBucketName(), source.getKey(), destinationFile);
		complete = true;
		return "Downloaded";
		
		
	}

	public boolean isError() {
		return error;
	}

	public boolean isComplete() {
		return complete;
	}

	public S3ObjectSummary getSource() {
		return source;
	}
	
	
	
	
}
