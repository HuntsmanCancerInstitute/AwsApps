package edu.utah.hci.aws.apps.copy;

import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import edu.utah.hci.aws.util.Util;
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**JUnit tests for the S3Copy application. Adjust the paths to match your resources.
 * 1) Create two buckets on S3 for just testing. They will be cleared.
 * 2) Create a local credentials file ~/.aws/credentials with your access, secret, and region info, chmod 600 the file and keep it private.
		[default]
		aws_access_key_id = AKxxxxxxxxxxxxxxxxxxxxxxJK6A  
		aws_secret_access_key = BgDV2UHxxxxxxxxxxxx5HZMpUQ
		region = us-west-2	
 */
public class TestS3Copy {

	/*Adjust these fields to match your testing environment	 */
	
	/**Be sure these bucket exists and doesn't contain anything you care about. WARNING, they will be emptied!*/
	private static final String sourceBucketName = "hcibioinfo-nix-test";
	private static final String destinationBucketName = "hcibioinfo-gsync-test"; 

	/**Full path to a small test file.*/
	private static final String pathToSmallTestFile = "/Users/u0028003/Code/AwsApps/TestData/GSync/Bam/testShortIndex.bam.S3.txt";
	
	/**Full path to a temporary dir.*/
	private static final String pathToTmp = "/Users/u0028003/Code/AwsApps/TestData/Tmp/";
	
	/**Email address to send log info*/
	private static final String email = "david.nix@hci.utah.edu";

	/* No need to modify anything below */
	

	@Test
	public void bucketToBucketNoRecursive() {
		AmazonS3 s3 = null;
		try {
			//make a client
			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
			
			//empty the buckets
			emptyS3Bucket(s3, sourceBucketName);
			emptyS3Bucket(s3, destinationBucketName);
			
			//upload some files the test file into the source
			upload(s3, sourceBucketName, "S3Copy/test.txt", new File(pathToSmallTestFile));
			upload(s3, sourceBucketName, "S3Copy/test.log", new File(pathToSmallTestFile));
			
			String sourceS3Uri = "s3://"+sourceBucketName+ "/S3Copy/test.txt";
			
			//dry run
			String[] args = new String[] {
					"-j", sourceS3Uri+ " > s3://"+destinationBucketName+"/S3CopyDest/test.txt",
					"-d", "-e", email, "-s"
			};
			S3Copy s3Copy = new S3Copy(args);
			String[] lines = Util.loadTxtFile(s3Copy.getTmpEmailLog());
			boolean found = false;
			for (String l: lines) if (l.contains("Dry run, exiting.")) found = true;
			assertTrue("Checking dry run", found);
			
			//copy file to file, not recursive
			args = new String[] {
					"-j", sourceS3Uri+ " > s3://"+destinationBucketName+"/S3CopyDest/test.txt", "-s"
			};
			s3Copy = new S3Copy(args);
			HashMap<String, S3ObjectSummary> keySum = fetchS3Objects(s3, destinationBucketName);
			//should be just one
			assertTrue("Not seeing just one file in destination? "+keySum.size(), keySum.size()==1);
			//looking for
			assertTrue("Not finding 'S3CopyDest/test.txt' in destination? ", keySum.containsKey("S3CopyDest/test.txt"));
			
			//attempt a re copy and show it doesn't do it
			//copy file to file, not recursive
			args = new String[] {
					"-j", sourceS3Uri+ " > s3://"+destinationBucketName+"/S3CopyDest/test.txt",
					"-e", email, "-s"
			};
			s3Copy = new S3Copy(args);
			lines = Util.loadTxtFile(s3Copy.getTmpEmailLog());
			found = false;
			for (String l: lines) if (l.contains("Exists, skipping")) found = true;
			assertTrue("Checking for 'Exists, skipping'", found);
			
			//copy over two named files
			emptyS3Bucket(s3, destinationBucketName);
			String sourceS3UriLog = "s3://"+sourceBucketName+ "/S3Copy/test.log";
			args = new String[] {
					"-j", sourceS3Uri+   " > s3://"+destinationBucketName+"/S3CopyDest/test.txt, "+
						  sourceS3UriLog+" > s3://"+destinationBucketName+"/S3CopyDest/test.log", "-s"
			};
			s3Copy = new S3Copy(args);
			keySum = fetchS3Objects(s3, destinationBucketName);
			//should be two
			assertTrue("Not seeing just two file in destination? "+keySum.size(), keySum.size()==2);
			//looking for
			assertTrue("Not finding 'S3CopyDest/test.txt' in destination? ", keySum.containsKey("S3CopyDest/test.txt"));
			assertTrue("Not finding 'S3CopyDest/test.log' in destination? ", keySum.containsKey("S3CopyDest/test.log"));
			//Util.pl(keySum.keySet().toString());
			
			//copy one file just into the bucket
			emptyS3Bucket(s3, destinationBucketName);
			//named file copy to bucket
			args = new String[] {
					"-j", sourceS3Uri+ " > s3://"+destinationBucketName+"/",
			};
			
			s3Copy = new S3Copy(args);
			keySum = fetchS3Objects(s3, destinationBucketName);
			//should be just one
			assertTrue("Not seeing just one file in destination? "+keySum.size(), keySum.size()==1);
			//looking for
			assertTrue("Not finding 'test.txt' in destination? ", keySum.containsKey("test.txt"));
	

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		} finally {
			if (s3!=null) s3.shutdown();
		}
	}
	
	@Test
	public void bucketToBucketRecursive() {
		AmazonS3 s3 = null;
		try {
			//make a client
			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
			
			//empty the buckets
			emptyS3Bucket(s3, sourceBucketName);
			emptyS3Bucket(s3, destinationBucketName);
			
			//upload some files into the source
			upload(s3, sourceBucketName, "S3Copy/test.txt", new File(pathToSmallTestFile));
			upload(s3, sourceBucketName, "S3Copy/test.log", new File(pathToSmallTestFile));
			upload(s3, sourceBucketName, "S3Copy/noCopy.log", new File(pathToSmallTestFile));
			
			//recursive copy
			String[] args = new String[] {
					"-j", "s3://"+sourceBucketName+ "/S3Copy/test > s3://"+destinationBucketName+"/Test/",
					"-r", "-s"
			};
			S3Copy s3Copy = new S3Copy(args);
			HashMap<String, S3ObjectSummary> keySum = fetchS3Objects(s3, destinationBucketName);
			//should be two copied over
			assertTrue("Not seeing just two files in destination? "+keySum.size(), keySum.size()==2);
			//looking for
			assertTrue("Not finding 'Test/test.txt' in destination? ", keySum.containsKey("Test/test.txt"));
			assertTrue("Not finding 'Test/test.log' in destination? ", keySum.containsKey("Test/test.log"));
			Util.pl(keySum.keySet().toString());
			
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		} finally {
			if (s3!=null) s3.shutdown();
		}
	}
	
	@Test
	public void bucketToLocal() {
		AmazonS3 s3 = null;
		try {
			//make a client
			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
			
			//empty the buckets
			emptyS3Bucket(s3, sourceBucketName);
			emptyS3Bucket(s3, destinationBucketName);
			
			//upload some files into the source
			upload(s3, sourceBucketName, "S3Copy/test.txt", new File(pathToSmallTestFile));
			upload(s3, sourceBucketName, "S3Copy/test.log", new File(pathToSmallTestFile));
			upload(s3, sourceBucketName, "S3Copy/noCopy.log", new File(pathToSmallTestFile));
			
			//make the tmp dir
			File tmp = new File(pathToTmp);
			tmp.mkdirs();
			File workingTmp = new File(tmp,"TestS3CopyDeleteMe");
			if (workingTmp.exists()) Util.deleteDirectory(workingTmp);
			workingTmp.mkdir();
			
			//recursive copy
			String[] args = new String[] {
					"-j", "s3://"+sourceBucketName+ "/S3Copy/test > "+workingTmp,
					"-r", "-s"
			};
			S3Copy s3Copy = new S3Copy(args);
			String[] fileNames = workingTmp.list();
			//should be two copied over
			assertTrue("Not seeing just two files in local? "+fileNames.length, fileNames.length==2);
			//looking for
			assertTrue("Not finding 'test.txt' in local? ", new File(workingTmp,"test.txt").exists());
			assertTrue("Not finding 'test.log' in local? ", new File(workingTmp,"test.log").exists());
			
			Util.deleteDirectory(workingTmp);
			
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		} finally {
			if (s3!=null) s3.shutdown();
		}
	}
	
	@Test
	public void archiveRetrieval() {
		AmazonS3 s3 = null;
		try {
			//make a client
			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
			
			//empty the buckets
			emptyS3Bucket(s3, sourceBucketName);
			emptyS3Bucket(s3, destinationBucketName);
			
			//upload a file into the source in 
			upload(s3, sourceBucketName, "S3Copy/test.txt", new File(pathToSmallTestFile), StorageClass.Glacier);

			//recursive copy, expedited retrieval
			String[] args = new String[] {
					"-j", "s3://"+sourceBucketName+ "/S3Copy/test > s3://"+destinationBucketName+"/Test/",
					"-r", "-x", "-s"

			};
			S3Copy s3Copy = new S3Copy(args);
			HashMap<String, S3ObjectSummary> keySum = fetchS3Objects(s3, destinationBucketName);
			//Util.pl(keySum.keySet().toString());
			//should be one copied over
			assertTrue("Not seeing just one file in destination? "+keySum.size(), keySum.size()==1);
			//looking for
			assertTrue("Not finding 'Test/test.txt' in destination? ", keySum.containsKey("Test/test.txt"));
			
			
		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		} finally {
			if (s3!=null) s3.shutdown();
		}
	}


	private boolean upload(AmazonS3 s3, String bucketName, String key, File file) {	
		try {
			TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).withMultipartCopyThreshold((long) (256 * 1024 * 1024)).build();
			Upload u = tm.upload(bucketName, key, file);
			u.waitForCompletion();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	private boolean upload(AmazonS3 s3, String bucketName, String key, File file, StorageClass storageClass) {	
		try {
	        PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, key, file).withStorageClass(storageClass);
			TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).withMultipartCopyThreshold((long) (256 * 1024 * 1024)).build();
			Upload u = tm.upload(putObjectRequest);
			u.waitForCompletion();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}

	/**Returns key:objectSummary */
	private static HashMap<String, S3ObjectSummary> fetchS3Objects(AmazonS3 s3, String bucketName) throws Exception{
		HashMap<String, S3ObjectSummary> kos = new HashMap<String, S3ObjectSummary>();
		ListObjectsV2Result result;
		//must watch for truncated results
		do {
			result = s3.listObjectsV2(bucketName);
			for (S3ObjectSummary os : result.getObjectSummaries()) kos.put(os.getKey(), os);
			result.setContinuationToken(result.getNextContinuationToken());
		} while (result.isTruncated());
		return kos;
	}


	private static void emptyS3Bucket(AmazonS3 s3, String bucketName) throws Exception {
		HashMap<String, S3ObjectSummary> keySum = fetchS3Objects(s3, bucketName);
		for (S3ObjectSummary os: keySum.values()) s3.deleteObject(bucketName, os.getKey());
	}



	public static void p(String s) {
		System.out.println(s);
	}
}
