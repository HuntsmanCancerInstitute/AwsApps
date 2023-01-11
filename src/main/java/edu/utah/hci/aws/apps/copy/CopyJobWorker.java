package edu.utah.hci.aws.apps.copy;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import edu.utah.hci.aws.util.Util;


public class CopyJobWorker implements Runnable {
	
	//fields
	private boolean failed = false;
	private S3Copy s3Copy = null;
	private String name = null;
	private AmazonS3 s3 = null;
	private int maxTries;
	private int numberDaysToRestore;
	private int minToWait;
	private TransferManager tm;
	
	//constructor
	public CopyJobWorker (S3Copy s3Copy, String name) {
		this.s3Copy = s3Copy;
		this.name = name;
		this.maxTries = s3Copy.getMaxTries();
		this.numberDaysToRestore = s3Copy.getNumberDaysToRestore();
		this.minToWait = s3Copy.getMinToWait();
		s3 = AmazonS3ClientBuilder.standard().withCredentials(s3Copy.getCredentials()).withRegion(s3Copy.getRegion()).build();
		tm = TransferManagerBuilder.standard().withS3Client(s3).withMultipartCopyThreshold((long) (256 * 1024 * 1024)).build();
	}
	
	
	
	//the thread runner
	public void run() {	
		try {
			CopyJob cj;
			while ((cj = s3Copy.fetchNextCopyJob())!= null) {
				Util.pl(cj+"\t"+ cj.restoreCopy(this));
			}
			s3.shutdown();
			
		} catch (Exception e) {
			failed = true;
			s3.shutdown();
			e.printStackTrace();
		}
	}
	
	/**Attempts 'ts3.restoreObjectV2()' maxTries before throwing error message*/
	public void restore(String bucketName, String key) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				RestoreObjectRequest requestRestore = new RestoreObjectRequest(bucketName, key, numberDaysToRestore);
				s3.restoreObjectV2(requestRestore);
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed restoreRequest trying again, "+attempt);
			}
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed send a s3.restoreObjectV2 ("+bucketName+", "+key+") S3 error message:\n"+error);
	}
	
	public void s3Copy(String sourceBucket, String sourceKey, String destBucket, String destKey) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				Copy archiveCopy = tm.copy(sourceBucket, sourceKey, destBucket, destKey);
				archiveCopy.waitForCompletion();
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 'tm.copy(sourceBucket, sourceObjectKey, destBucket, destObjectKey)' trying again, "+attempt);
			}
			catch ( InterruptedException ie) {
				error = Util.getStackTrace(ie);
				sleep("\tWARNING: failed 'tm.copy(sourceBucket, sourceObjectKey, destBucket, destObjectKey)' trying again, "+attempt);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 'tm.copy(sourceBucket, sourceObjectKey, destBucket, destObjectKey)' trying again, "+attempt);
			};
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed tm.copy("+sourceBucket+", "+sourceKey+", "+destBucket+", "+destKey+") S3 error message:\n"+error);
	}
	
	/**Attempts 'tm.download(bucketName, key)' maxTries before throwing error message*/
	public void tryDownload(String bucketName, String key, File destination) throws Exception {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				Download xfer = tm.download(bucketName, key, destination);
				xfer.waitForCompletion();
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt);
			}
			catch ( InterruptedException ie) {
				error = Util.getStackTrace(ie);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt);
			};
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed tm.download("+bucketName+", "+key+") S3 error message:\n"+error);
	}


	private void sleep(String message) {
		try {
			s3Copy.pl(message+", sleeping "+minToWait+" minutes");
			TimeUnit.MINUTES.sleep(minToWait);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}


	public boolean isFailed() {
		return failed;
	}



	public String getName() {
		return name;
	}

}
