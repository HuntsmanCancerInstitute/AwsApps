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
	private CopyRequest copyRequest;
	private boolean complete = false;
	private boolean error = false;
	
	public CopyJob (S3ObjectSummary source, String destinationBucket, String destinationKey, File destinationFile, S3Copy s3Copy, CopyRequest copyRequest) throws Exception {
		this.source = source;
		this.destinationBucket = destinationBucket;
		this.destinationKey = destinationKey;
		this.destinationFile = destinationFile;
		this.s3Copy = s3Copy;
		this.copyRequest = copyRequest;
		
		s3Copy.p("\t"+source.getStorageClass()+ "\t"+ toString());
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
		//destination s3 object, must use appropriate region
		else {
			ArrayList<S3ObjectSummary> destObj = s3Copy.fetchS3Objects(destinationBucket, destinationKey, copyRequest.getDestinationRegion(),  true);
			
			//returned null?
			if (destObj == null) {
				s3Copy.pl("\tERROR, access denied. Aborting.");
				error = true;
				return;
			}
			
			else if (destObj.size()==1) {
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
			ObjectMetadata metaData = cjw.tryGetObjectMetadata(source.getBucketName(),  source.getKey(), copyRequest.getSourceRegion());
			
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
				cjw.restore(source.getBucketName(), source.getKey(), copyRequest.getSourceRegion(), s3Copy.getRestoreTier());
				return "Restore request placed";
			}
		}
		//not archived so copy it
		else return copyIt(cjw);
		
	}

	private String copyIt(CopyJobWorker cjw) throws Exception {
		long startTime = System.currentTimeMillis();
		String result = null;
		//destination is s3?
		if (destinationFile == null) {
			cjw.s3S3Copy(source.getBucketName(), source.getKey(), copyRequest.getSourceRegion(), destinationBucket, destinationKey, copyRequest.getDestinationRegion());
			complete = true;
			result = "Copied ";
		}
		//destination is local
		else {
			cjw.tryDownload(source.getBucketName(), source.getKey(), copyRequest.getSourceRegion(), destinationFile);
			complete = true;
			result = "Downloaded ";
		}
		double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
		String time = Util.formatNumber(diffTime, 1);
		return result+time+" Min";
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
