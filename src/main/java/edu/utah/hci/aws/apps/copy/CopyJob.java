package edu.utah.hci.aws.apps.copy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import com.amazonaws.services.s3.AmazonS3;
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
		if (destinationFile != null && destinationFile.exists()) {
			if (destinationFile.length() == source.getSize()) complete = true;
			
		}
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

		//does it need to be restored?
		if (source.getStorageClass().equals("STANDARD") == false) {
			//pull meta data
			ObjectMetadata metaData = s3Copy.tryGetObjectMetadata(source.getBucketName(),  source.getKey());
			
			//ready to be copied?
			if (metaData.getArchiveStatus()==null && metaData.getOngoingRestore()==false) return copyIt(cjw);
			
			//needs a restore?
			if (metaData.getOngoingRestore()==false) {
				cjw.restore(source.getBucketName(), source.getKey());
				return "Restore request placed";
			}
			return "Restoring";
			
		}
		//copy it
		return copyIt(cjw);
		
	}

	private String copyIt(CopyJobWorker cjw) throws Exception {
		if (destinationFile == null) {
			cjw.s3Copy(source.getBucketName(), source.getKey(), destinationBucket, destinationKey);
			complete = true;
			return "Copied";
		}
		
		s3Copy.el("Copy from s3 to local not implemented just yet!");
		return "Local copy not supported";
		
		
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
