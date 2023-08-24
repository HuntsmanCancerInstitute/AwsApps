package edu.utah.hci.aws.apps.copy;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.GlacierJobParameters;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.Tier;
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
	private int maxTries;
	private int numberDaysToRestore;
	private int minToWait;
	private HashMap<String, AmazonS3> s3 = new HashMap<String, AmazonS3>();
	private HashMap<String, TransferManager> tm = new HashMap<String, TransferManager>();
	
	//constructor
	public CopyJobWorker (S3Copy s3Copy, String name) {
		this.s3Copy = s3Copy;
		this.name = name;
		this.maxTries = s3Copy.getMaxTries();
		this.numberDaysToRestore = s3Copy.getNumberDaysToRestore();
		this.minToWait = s3Copy.getMinToWait();
	}
	
	
	//the thread runner
	public void run() {	
		try {
			CopyJob cj;
			while ((cj = s3Copy.fetchNextCopyJob())!= null) Util.pl(cj+"\t"+ cj.restoreCopy(this));
		} catch (Exception e) {
			failed = true;
			s3Copy.el(Util.getStackTrace(e));
		} finally {
			shutdown();
		}
	}
	
	/**Close down the AmazonS3 clients and any associated TransferManagers.*/
	private void shutdown() {
		for (AmazonS3 s : s3.values()) s.shutdown();
	}

	/**Attempts 's3.getObjectMetadata(bucketName, key)' maxTries before throwing error message*/
	ObjectMetadata tryGetObjectMetadata(String bucketName, String key, String region) throws IOException {		
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				return fetchS3Client(region).getObjectMetadata(bucketName, key);
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 's3.getObjectMetadata(bucketName, key)' trying again, "+attempt);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 's3.getObjectMetadata(bucketName, key)' trying again, "+attempt);
			};
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed to fetch ObjectMedaData for "+key+" in "+bucketName+" S3 error message:\n"+error);
	}
	
	private AmazonS3 fetchS3Client(String region) {
		AmazonS3 s3Client = s3.get(region);
		if (s3Client == null) {
			s3Client = AmazonS3ClientBuilder.standard().withCredentials(s3Copy.getCredentials()).withRegion(region).build();
			s3.put(region, s3Client);
		}
		return s3Client;
	}
	
	private TransferManager fetchTransferManager(String destinationRegion) {
		TransferManager t = tm.get(destinationRegion);
		if (t == null) {
			AmazonS3 s3Dest = fetchS3Client(destinationRegion);
			t = TransferManagerBuilder.standard().withS3Client(s3Dest).withMultipartCopyThreshold((long) (256 * 1024 * 1024)).build();
			tm.put(destinationRegion, t);
		}
		return t;
	}


	/**Attempts 'ts3.restoreObjectV2()' maxTries before throwing error message*/
	public void restore(String bucketName, String key, String region, Tier tier) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				//already placed?
				GlacierJobParameters gjp = new GlacierJobParameters().withTier(tier);
				RestoreObjectRequest requestRestore = new RestoreObjectRequest(bucketName, key, numberDaysToRestore).withGlacierJobParameters(gjp);
				fetchS3Client(region).restoreObjectV2(requestRestore);
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				//already restore request placed?
				if (error.contains("RestoreAlreadyInProgress")) return;
				sleep("\tWARNING: failed restoreRequest trying again, "+attempt+"\n"+error);
			}
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed send a s3.restoreObjectV2 ("+bucketName+", "+key+") S3 error message:\n"+error);
	}
	
	public void s3S3Copy(String sourceBucket, String sourceKey, String sourceRegion, String destBucket, String destKey, String destRegion) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				Copy archiveCopy = this.fetchTransferManager(destRegion).copy(new CopyObjectRequest(sourceBucket, sourceKey, destBucket, destKey), fetchS3Client(sourceRegion), null);
				archiveCopy.waitForCompletion();
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 'tm.copy(sourceBucket, sourceObjectKey, destBucket, destObjectKey)' trying again, "+attempt+"\n"+error);
			}
			catch ( InterruptedException ie) {
				error = Util.getStackTrace(ie);
				sleep("\tWARNING: failed 'tm.copy(sourceBucket, sourceObjectKey, destBucket, destObjectKey)' trying again, "+attempt+"\n"+error);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 'tm.copy(sourceBucket, sourceObjectKey, destBucket, destObjectKey)' trying again, "+attempt+"\n"+error);
			};
		}
		//only hits this if all the attempts failed
			
		throw new IOException("ERROR failed tm.copy("+sourceBucket+", "+sourceKey+", "+destBucket+", "+destKey+", "+destRegion+") S3 error message:\n"+error);
	}
	
	/**Attempts 'tm.download(bucketName, key)' maxTries before throwing error message*/
	public void tryDownload(String bucketName, String key, String region, File destination) throws Exception {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				Download xfer = fetchTransferManager(region).download(bucketName, key, destination);
				xfer.waitForCompletion();
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt+"\n"+error);
			}
			catch ( InterruptedException ie) {
				error = Util.getStackTrace(ie);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt+"\n"+error);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt+"\n"+error);
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
