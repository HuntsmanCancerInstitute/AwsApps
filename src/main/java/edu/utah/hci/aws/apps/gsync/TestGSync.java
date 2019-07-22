package edu.utah.hci.aws.apps.gsync;

import org.junit.Test;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import edu.utah.hci.aws.util.Util;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

/**JUnit tests for the GSync application. Adjust the paths to match your resources.
 * 1) Create a bucket on S3 for just testing. It will be cleared.
 * 2) Create this file ~/.aws/credentials with your access, secret, and region info, chmod 600 the file and keep it private.
		[default]
		aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A
		aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ
		region = us-west-2	
 */
public class TestGSync {

	//Adjust these fields to match your testing environment
	/**Be sure this bucket exists and doesn't contain anything you care about. WARNING, it will be emptied!*/
	private static final String testS3BucketName = "hcibioinfo-gsync-test";

	/**Directory in the AwsApps project containing the GSync.zip file. MUST end with a / */
	private static final String pathToTestData = "/Users/u0028003/Code/AwsApps/TestData/";
	
	
	
	//No need to modify anything below
	
	/**Relative paths of data files that should be uploaded*/
	private static String[] filesForUpload = {
			"GSync/Gz/testNoIndex.bed.gz",
			"GSync/Cram/testShortIndex.cram",
			"GSync/Cram/testShortIndex.crai",
			"GSync/Cram/testWithIndex.cram",
			"GSync/Cram/testWithIndex.cram.crai",
			"GSync/Gz/testWithIndex.bed.gz",
			"GSync/Gz/testWithIndex.bed.gz.tbi",
			"GSync/Bam/testWithIndex.bam",
			"GSync/Bam/testWithIndex.bam.bai",
			"GSync/Bam/testShortIndex.bam",
			"GSync/Bam/testShortIndex.bai"
	};
	
	private static String[] filesNotForUpload = {
			"GSync/NoUpload/testTooRecent.bed.gz",
			"GSync/NoUpload/testTooSmall.bed.gz",
			"GSync/NoUpload/testTooSmall.bed.gz.tbi",
			"GSync/NoUpload/testWrongExtension.bed"
	};


	@Test
	public void testDirectoryScan() {
		try {
			setupLocalDir();
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			
			gs.scanLocalDir();
			HashMap <String,File> pathFile = gs.getCandidatesForUpload();
			for (String s: pathFile.keySet()) System.out.println("Can\t"+s);
			
			//check size
			assertTrue(pathFile.size() == filesForUpload.length);
			
			//check keys
			String path = pathToTestData.substring(1);
			for (String s: filesForUpload) {
				String testKey = path + s;
				assertTrue(pathFile.containsKey(testKey));
			}

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testInitialSync() {
		try {
			setupLocalDir();
			emptyS3Bucket();

			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			
			//this fires the entire scan and upload but not delete local
			gs.doWork();
			
			//check final
			assertTrue(gs.isResultsCheckOK());
			
			//check these are all zero
			assertTrue(gs.getPlaceholders().size() == 0);
			assertTrue(gs.getPlaceholderFiles().size() == 0);
			assertTrue(gs.getLocalFileAlreadyUploaded().size() == 0);
			assertTrue(gs.getLocalFileAlreadyUploadedButDiffSize().size() == 0);
			assertTrue(gs.getLocalFileAlreadyUploadedNoPlaceholder().size() == 0);
			assertTrue(gs.getS3KeyWithNoLocal().size() == 0);
			
			//check that the original files are still there
			for (String s: filesForUpload) assertTrue(new File (pathToTestData + s).exists());
			
			//check that placeholders have been created
			for (String s: filesForUpload) assertTrue(new File (pathToTestData + s + GSync.PLACEHOLDER_EXTENSION).exists());
			
			//check that the S3 objects exist
			HashMap<String, S3ObjectSummary> kos = fetchS3Objects();
			String path = pathToTestData.substring(1);
			for (String s: filesForUpload) assertTrue(kos.containsKey(path + s));

		} catch (Exception e) {
			System.err.println(e.getMessage());
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testSecondSync() {
		try {
			setupLocalDir();
			emptyS3Bucket();
			
			//get sizes of the upload files
			HashMap<String, Long> uploadKeySize = new HashMap<String, Long>();
			for (String s: filesForUpload) {
				String n = pathToTestData + s;
				Long size = new File (n).length();
				uploadKeySize.put(n.substring(1), size);
			}

			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
		
			//run a second time and delete local
			GSync gs2 = new GSync();
			gs2.setLocalDir(new File(pathToTestData+"/GSync"));
			gs2.setMinGigaBytes(0.0005);
			gs2.setBucketName(testS3BucketName);
			gs2.setDryRun(false);
			gs2.setDeleteUploaded(true);
			gs2.doWork();
			assertTrue(gs2.isResultsCheckOK());
			
			//check these are all correct
			assertTrue(gs2.getPlaceholders().size() == filesForUpload.length);
			assertTrue(gs2.getPlaceholderFiles().size() == filesForUpload.length);
			assertTrue(gs2.getLocalFileAlreadyUploaded().size() == 0);
			assertTrue(gs2.getLocalFileAlreadyUploadedButDiffSize().size() == 0);
			assertTrue(gs2.getLocalFileAlreadyUploadedNoPlaceholder().size() == 0);
			assertTrue(gs2.getS3KeyWithNoLocal().size() == 0);
			
			//check that the original files are now deleted
			for (String s: filesForUpload) assertFalse(new File (pathToTestData + s).exists());
			
			//check that placeholders have been created
			for (String s: filesForUpload) assertTrue(new File (pathToTestData + s + GSync.PLACEHOLDER_EXTENSION).exists());
			
			//check that the non uploaded files are present
			for (String s: filesNotForUpload) assertTrue(new File (pathToTestData + s).exists());
			
			//check that the S3 objects exist and have the correct size
			HashMap<String, S3ObjectSummary> kos = fetchS3Objects();
			String path = pathToTestData.substring(1);
			
			for (String s: filesForUpload) {
				assertTrue(kos.containsKey(path+s));
				assertTrue(kos.get(path+s).getSize() == uploadKeySize.get(path+s));
			}
			
			//check the local Placeholders have the correct size
			HashMap<String, Placeholder> placeholders = gs2.getPlaceholders();
			for (String s: uploadKeySize.keySet()) {
				Placeholder p = placeholders.get(s);
				long pSize = Long.parseLong(p.getAttribute("size"));
				assertTrue (pSize == uploadKeySize.get(s));
			}
			

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testBrokenSync() {
		try {
			setupLocalDir();
			emptyS3Bucket();
			
			//get sizes of the upload files
			HashMap<String, Long> uploadKeySize = new HashMap<String, Long>();
			for (String s: filesForUpload) {
				String n = pathToTestData + s;
				Long size = new File (n).length();
				uploadKeySize.put(n.substring(1), size);
			}

			//run uploads and delete local
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
			
			//delete an S3 object
			deleteS3Object(pathToTestData.substring(1)+filesForUpload[0]);
			
			//delete a local placeholder file
			File p = new File(pathToTestData+filesForUpload[2]+GSync.PLACEHOLDER_EXTENSION);
			p.delete();
			
			//change the size in a placeholder
			File pFile = new File(pathToTestData+filesForUpload[1]+GSync.PLACEHOLDER_EXTENSION);
			Placeholder ph = new Placeholder(pFile);
			ph.getAttributes().put("size", "111");
			ph.writePlaceholder(pFile);
			
			//change the etag in a placeholder
			File pFile2 = new File(pathToTestData+filesForUpload[3]+GSync.PLACEHOLDER_EXTENSION);
			Placeholder ph2 = new Placeholder(pFile2);
			ph2.getAttributes().put("etag", "badEtag");
			ph2.writePlaceholder(pFile2);
			
			//move a placeholder
			File pFile3 = new File(pathToTestData+filesForUpload[4]+GSync.PLACEHOLDER_EXTENSION);
			File destination = new File(pathToTestData+"GSync/Zip/"+pFile3.getName());
			pFile3.renameTo(destination);
			
		
			//run a second time, lots of issues should occur
			GSync gs2 = new GSync();
			gs2.setLocalDir(new File(pathToTestData+"/GSync"));
			gs2.setMinGigaBytes(0.0005);
			gs2.setBucketName(testS3BucketName);
			gs2.setDryRun(false);
			gs2.setDeleteUploaded(true);
			gs2.doWork();
			
			//check that there are problems
			assertFalse(gs2.isResultsCheckOK());
			
			//check that the missing S3 object is recorded
			Placeholder notInS3 = gs2.getNotFoundInS3().get(0);
			assertTrue(notInS3.getAttribute("key").equals(pathToTestData.substring(1)+filesForUpload[0]));
			
			//check for an S3 object with no local placeholder or file
			assertTrue(gs2.getS3KeyWithNoLocal().contains(pathToTestData.substring(1)+filesForUpload[2]));
		
			//check for incorrect size in placeholder
			Placeholder size = gs2.getS3SizeNotMatchPlaceholder().get(0);
			assertTrue(size.getAttribute("key").equals(pathToTestData.substring(1)+filesForUpload[1]));
			
			//check for incorrect etag in placeholder
			Placeholder etag = gs2.getS3EtagNotMatchPlaceholder().get(0);
			assertTrue(etag.getAttribute("key").equals(pathToTestData.substring(1)+filesForUpload[3]));
			
			//check for moved placeholder
			Placeholder moved = gs2.getKeyDoesNotMatchLocalPlaceholderPath().get(0);
			assertTrue(moved.getAttribute("key").equals(pathToTestData.substring(1)+filesForUpload[4]));


		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}

	
	
	
	
	/**Returns key:objectSummary */
	private HashMap<String, S3ObjectSummary> fetchS3Objects() {
		AmazonS3 s3 = null;
		try {
			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

			ListObjectsV2Result result = s3.listObjectsV2(testS3BucketName);
	        List<S3ObjectSummary> objects = result.getObjectSummaries();
	        HashMap<String, S3ObjectSummary> kos = new HashMap<String, S3ObjectSummary>();
	        for (S3ObjectSummary os: objects) kos.put(os.getKey(), os);
	        return kos;
	        
		} catch (Exception e) {
		    System.err.println(e.getMessage());
		    fail("Problem fetching S3 Object Summary");
		} finally {
			if (s3 != null) s3.shutdown();
		}
		return null;
		
	}

	private void deleteS3Object(String key) {
		AmazonS3 s3 = null;
		try {
			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
			s3.deleteObject(testS3BucketName, key);

		} catch (Exception e) {
		    System.err.println(e.getMessage());
		    fail("Problem deleting object "+key);
		} finally {
			if (s3 != null) s3.shutdown();
		}

		
	}
	
	
	private void emptyS3Bucket() {
		AmazonS3 s3 = null;
		try {
			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

			ListObjectsV2Result result = s3.listObjectsV2(testS3BucketName);
	        List<S3ObjectSummary> objects = result.getObjectSummaries();
	        for (S3ObjectSummary os: objects) s3.deleteObject(testS3BucketName, os.getKey());

		} catch (Exception e) {
		    System.err.println(e.getMessage());
		    fail("Problem emptying bucket.");
		} finally {
			if (s3 != null) s3.shutdown();
		}
	}

	/**This sets up the local test dir.*/
	private void setupLocalDir() throws IOException, InterruptedException {
		String[] cmds = {
				"cd "+pathToTestData,
				"rm -rf GSync __MACOSX",
				"unzip -q GSync.zip",
				"find GSync -print | while read filename; do touch -t 201801010101.01 $filename; done",
				"touch GSync/NoUpload/testTooRecent.bed.gz",
				"ln -s GSync/Bam/testWithIndex.bam GSync/NoUpload/testSymLink.bam"
		};
		String c = Util.stringArrayToString(cmds, "\n");
		Util.executeShellScript(c, new File(pathToTestData));
	}

	public static void p(String s) {
		System.out.println(s);
	}
}
