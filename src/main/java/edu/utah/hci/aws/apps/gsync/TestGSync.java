package edu.utah.hci.aws.apps.gsync;

import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import edu.utah.hci.aws.util.Util;
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
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

	/*Adjust these fields to match your testing environment	 */
	
	/**Be sure this bucket exists and doesn't contain anything you care about. WARNING, it will be emptied!*/
	private static final String testS3BucketName = "hcibioinfo-gsync-test";

	/**Directory in the AwsApps project containing the GSync.zip file. MUST end with a / */
	private static final String pathToTestData = "/Users/u0028003/Code/AwsApps/TestData/";
	
	/**Email address to send log info, must be an hci address given the default smpt server. */
	private static final String email = "david.nix@hci.utah.edu";

	/* No need to modify anything below */
	
	/*Relative paths of data files that should be uploaded*/
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

	/*Relative paths of data files that should not be uploaded*/
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
			for (String s: filesForUpload) assertTrue(pathFile.containsKey(s));

			//check zero placeholders
			assertTrue(gs.getPlaceholderFiles().size() == 0);

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
			gs.setEmail(email);

			//this fires the entire scan and upload but not delete local
			gs.doWork();
			gs.sendEmail();
			System.err.println("\nCheck "+email+" for OK message ");

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
			for (String s: filesForUpload) assertTrue(new File (pathToTestData + s + Placeholder.PLACEHOLDER_EXTENSION).exists());

			//check that the S3 objects exist
			HashMap<String, S3ObjectSummary> kos = fetchS3Objects();
			for (String s: filesForUpload) assertTrue(kos.containsKey(s));

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
				uploadKeySize.put(s, size);
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
			for (String s: filesForUpload) assertTrue(new File (pathToTestData + s + Placeholder.PLACEHOLDER_EXTENSION).exists());

			//check that the non uploaded files are present
			for (String s: filesNotForUpload) assertTrue(new File (pathToTestData + s).exists());

			//check that the S3 objects exist and have the correct size
			HashMap<String, S3ObjectSummary> kos = fetchS3Objects();

			for (String s: filesForUpload) {
				assertTrue(kos.containsKey(s));
				assertTrue(kos.get(s).getSize() == uploadKeySize.get(s));
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

			//run uploads and delete local files
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());

			//move a placeholder
			File pFile3 = new File(pathToTestData+filesForUpload[4]+Placeholder.PLACEHOLDER_EXTENSION);
			File destination = new File(pathToTestData+"GSync/Zip/"+pFile3.getName());
			pFile3.renameTo(destination);

			//run a second time, this will stop work after finding the bad placeholder
			GSync gsp = new GSync();
			gsp.setLocalDir(new File(pathToTestData+"/GSync"));
			gsp.setMinGigaBytes(0.0005);
			gsp.setBucketName(testS3BucketName);
			gsp.setDryRun(false);
			gsp.setVerbose(true);
			gsp.setDeleteUploaded(true);
			gsp.setEmail(email);
			//trap exception
			try {
				gsp.doWork();
			} catch (Exception e) {}
			gsp.sendEmail();
			System.err.println("\nCheck "+email+" for ERROR message");

			//check that there are problems
			assertFalse(gsp.isResultsCheckOK());

			//check for moved placeholder
			Placeholder badP = gsp.getFailingPlaceholders().get(0);
			assertTrue(badP.getPlaceHolderFile().toString().equals(destination.toString()));

			//fix the misplaced placeholder
			destination.renameTo(pFile3);

			//delete an S3 object
			deleteS3Object(filesForUpload[0]);

			//delete a local placeholder file
			File p = new File(pathToTestData+filesForUpload[2]+Placeholder.PLACEHOLDER_EXTENSION);
			p.delete();

			//change the size in a placeholder
			File pFile = new File(pathToTestData+filesForUpload[1]+ Placeholder.PLACEHOLDER_EXTENSION);
			Placeholder ph = new Placeholder(pFile, pathToTestData,gsp);
			ph.getAttributes().put("size", "111");
			ph.writePlaceholder(pFile);

			//change the etag in a placeholder
			File pFile2 = new File(pathToTestData+filesForUpload[3]+ Placeholder.PLACEHOLDER_EXTENSION);
			Placeholder ph2 = new Placeholder(pFile2,pathToTestData,gsp);
			ph2.getAttributes().put("etag", "badEtag");
			ph2.writePlaceholder(pFile2);

			//run a third time, this will throw lots of errors
			GSync gs2 = new GSync();
			gs2.setLocalDir(new File(pathToTestData+"/GSync"));
			gs2.setMinGigaBytes(0.0005);
			gs2.setBucketName(testS3BucketName);
			gs2.setDryRun(false);
			gs2.setVerbose(true);
			gs2.setDeleteUploaded(true);
			//trap exception
			try {
				gs2.doWork();
			} catch (Exception e) {}

			//check that there are problems
			assertFalse(gsp.isResultsCheckOK());

			//check that the missing S3 object is recorded
			Placeholder notInS3 = gs2.getFailingPlaceholders().get(1);  
			assertTrue(notInS3.getAttribute("key").equals(filesForUpload[0]));

			//check for an S3 object with no local placeholder or file
			assertTrue(gs2.getS3KeyWithNoLocal().keySet().contains(filesForUpload[2]));

			//check for incorrect size in placeholder
			Placeholder size = gs2.getFailingPlaceholders().get(0);
			assertTrue(size.getAttribute("key").equals(filesForUpload[1]));

			//check for incorrect etag in placeholder
			Placeholder etag = gs2.getFailingPlaceholders().get(2);
			assertTrue(etag.getAttribute("key").equals(filesForUpload[3]));

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testPlaceholderFileRecreation() {
		try {
			setupLocalDir();
			emptyS3Bucket();

			//run uploads and delete local files
			System.out.println("\nRun 1st time to upload datasets....");
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());

			//delete a placeholder
			File pFile = new File(pathToTestData+filesForUpload[4]+Placeholder.PLACEHOLDER_EXTENSION);
			pFile.delete();
			assertFalse(pFile.exists());

			//run a second time, this will stop work after finding the missing placeholder
			System.out.println("\nRunning 2rd time with missing placeholder....");
			GSync gsp = new GSync();
			gsp.setLocalDir(new File(pathToTestData+"/GSync"));
			gsp.setMinGigaBytes(0.0005);
			gsp.setBucketName(testS3BucketName);
			gsp.setDryRun(false);
			gsp.setVerbose(true);
			gsp.setDeleteUploaded(true);
			gsp.setEmail(email);
			//trap exception
			try {
				gsp.doWork();
			} catch (Exception e) {}
			gsp.sendEmail();
			System.err.println("\nCheck "+email+" for ERROR message");

			//check that there are problems due to the missing file
			assertFalse(gsp.isResultsCheckOK());
			assertTrue(gsp.getS3KeyWithNoLocal().size() == 1);

			//run a third time and indicate a placeholder file restore
			System.out.println("\nRunning 3rd time....");
			GSync gs2 = new GSync();
			gs2.setLocalDir(new File(pathToTestData+"/GSync"));
			gs2.setMinGigaBytes(0.0005);
			gs2.setBucketName(testS3BucketName);
			gs2.setDryRun(false);
			gs2.setVerbose(true);
			gs2.setDeleteUploaded(true);
			gs2.setRestorePlaceholderFiles(true);
			gs2.doWork();
			
			//check that there are problems
			assertTrue(pFile.exists());
			assertTrue(gs2.isResultsCheckOK());
			

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}


	@Test
	public void testRestore() {
		try {
			setupLocalDir();
			emptyS3Bucket();

			//run an upload with deletion of local files
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());

			//rename a placeholder to restore
			File a = new File(pathToTestData+filesForUpload[0]+Placeholder.PLACEHOLDER_EXTENSION);
			File aRestore = new File(pathToTestData+filesForUpload[0]+Placeholder.RESTORE_PLACEHOLDER_EXTENSION);
			a.renameTo(aRestore);
			assertFalse(new File(pathToTestData+filesForUpload[0]).exists());

			//run gsync
			GSync gs2 = new GSync();
			gs2.setLocalDir(new File(pathToTestData+"/GSync"));
			gs2.setMinGigaBytes(0.0005);
			gs2.setBucketName(testS3BucketName);
			gs2.setDryRun(false);
			gs2.setDeleteUploaded(true);
			gs2.setVerbose(true);
			gs2.doWork();
			assertTrue(gs2.isResultsCheckOK());

			//check that it was restored and the placeholder restored
			File localFile = new File(pathToTestData+filesForUpload[0]);
			assertTrue(localFile.exists());
			assertTrue(a.exists());
			assertFalse(aRestore.exists());

			//copy the placeholder to restore, not good! 
			Util.copyViaFileChannel(a, aRestore);

			//run gsync
			GSync gs3 = new GSync();
			gs3.setLocalDir(new File(pathToTestData+"/GSync"));
			gs3.setMinGigaBytes(0.0005);
			gs3.setBucketName(testS3BucketName);
			gs3.setDryRun(false);
			gs3.setDeleteUploaded(true);
			gs3.setVerbose(true);
			gs3.doWork();

			//check that there were problems
			assertFalse(gs3.isResultsCheckOK());

			//check that the failing placeholder is the one you copied
			String fp = gs3.getFailingPlaceholders().get(0).getPlaceHolderFile().toString();
			assertTrue(fp.equals(aRestore.toString()));

			//rename the placeholder to the local file, this effectively deletes it and alters the size of the  local
			a.renameTo(localFile);

			//run gsync
			GSync gs4 = new GSync();
			gs4.setLocalDir(new File(pathToTestData+"/GSync"));
			gs4.setMinGigaBytes(0.0005);
			gs4.setBucketName(testS3BucketName);
			gs4.setDryRun(false);
			gs4.setDeleteUploaded(true);
			gs3.setVerbose(true);
			//trap exception
			try {
				gs4.doWork();
			} catch (Exception e) {}

			//check that there were problems
			assertFalse(gs4.isResultsCheckOK());

			//check that the failing placeholder is the one pointing to the partial sized local
			Placeholder failingP = gs4.getFailingPlaceholders().get(0);
			assertTrue(failingP.getPlaceHolderFile().toString().equals(aRestore.toString()));
			assertTrue(failingP.getErrorMessages().get(0).contains("size"));

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}

	@Test
	public void testDelete() {
		try {
			setupLocalDir();
			emptyS3Bucket();

			//run an upload with no deletion of local files
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(false);
			gs.setVerbose(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());

			//check S3 object is present
			String s3Key = filesForUpload[0];
			HashMap<String, S3ObjectSummary> s3Sum = fetchS3Objects();
			assertTrue(s3Sum.containsKey(s3Key));

			//rename a placeholder to delete
			File localFile = new File(pathToTestData+filesForUpload[0]);
			File a = new File(pathToTestData+filesForUpload[0]+Placeholder.PLACEHOLDER_EXTENSION);
			File aDelete = new File(pathToTestData+filesForUpload[0]+Placeholder.DELETE_PLACEHOLDER_EXTENSION);
			a.renameTo(aDelete);
			assertTrue(localFile.exists());
			assertTrue(aDelete.exists());

			//copy placeholder for failed restore
			File aTemp = new File (pathToTestData+filesForUpload[0]+".temp");
			Util.copyViaFileChannel(aDelete, aTemp);

			//run gsync
			GSync gs2 = new GSync();
			gs2.setLocalDir(new File(pathToTestData+"/GSync"));
			gs2.setMinGigaBytes(0.0005);
			gs2.setBucketName(testS3BucketName);
			gs2.setDryRun(false);
			gs2.setDeleteUploaded(false);
			gs2.setVerbose(true);
			gs2.doWork();
			assertTrue(gs2.isResultsCheckOK());

			//check that the local and placeholder were deleted
			assertFalse(localFile.exists());
			assertFalse(aDelete.exists());

			//check S3 doesn't exist
			s3Sum = fetchS3Objects();
			assertFalse(s3Sum.containsKey(s3Key));

			//mv the tmp to do a restore
			File aRestore = new File(pathToTestData+filesForUpload[0]+Placeholder.RESTORE_PLACEHOLDER_EXTENSION);
			aTemp.renameTo(aRestore);

			//run gsync to attempt a restore on a non existent S3 object
			GSync gs3 = new GSync();
			gs3.setLocalDir(new File(pathToTestData+"/GSync"));
			gs3.setMinGigaBytes(0.0005);
			gs3.setBucketName(testS3BucketName);
			gs3.setDryRun(false);
			gs3.setVerbose(true);
			gs3.setDeleteUploaded(false);
			//trap exception
			try {
				gs3.doWork();
			} catch (Exception e) {}
			assertFalse(gs3.isResultsCheckOK());

			Placeholder failingP = gs3.getFailingPlaceholders().get(0);
			assertTrue(failingP.getErrorMessages().get(0).contains("S3 Object"));

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testMove() {
		try {
			setupLocalDir();
			emptyS3Bucket();

			//run an upload with no deletion of local files
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());

			//check S3 object is present
			String s3Key = filesForUpload[0];
			HashMap<String, S3ObjectSummary> s3Sum = fetchS3Objects();
			assertTrue(s3Sum.containsKey(s3Key));

			//move a placeholder to a diff folder
			File oriPlaceholder = new File(pathToTestData+filesForUpload[0]+Placeholder.PLACEHOLDER_EXTENSION);
			File dest = new File(pathToTestData+"GSync/Cram/"+oriPlaceholder.getName());
			oriPlaceholder.renameTo(dest);

			//run a second time and indicate a update
			gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setVerbose(true);
			gs.setUpdateS3Keys(true);
			//trap exception
			try {
				gs.doWork();
			} catch (Exception e) {}
			
			assertFalse(gs.isResultsCheckOK());
			
			//show the ori is gone, the dest is pres at file system and S3 system levels
			HashMap<String, S3ObjectSummary> os = fetchS3Objects();
			assertFalse(oriPlaceholder.exists());
			assertTrue(dest.exists());
			String oriKey = filesForUpload[0];
			String destKey = "GSync/Cram/"+oriPlaceholder.getName().replaceFirst(Placeholder.PLACEHOLDER_EXTENSION, "");

			assertFalse(os.containsKey(oriKey));
			assertTrue(os.containsKey(destKey));
			
			//show the internal key in the moved placeholder has been updated
			Placeholder updatedP = new Placeholder(dest, pathToTestData+"/GSync/",gs);
			assertTrue(updatedP.getAttribute("key").equals(destKey));
			
			//delete s3 object then attempt a move
			deleteS3Object(destKey);
			File newDest = new File(pathToTestData+"GSync/Bam/"+oriPlaceholder.getName());
			dest.renameTo(newDest);
			
			//run a third time, should fail due to missing s3 object
			gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setVerbose(true);
			gs.setUpdateS3Keys(true);
			//trap exception
			try {
				gs.doWork();
			} catch (Exception e) { 
				assertTrue(e.getMessage().contains("Original key doesn't exist"));
			}
			
			assertFalse(gs.isResultsCheckOK());

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}

	@Test
	public void testBrokenMove() {
		try {
			//setup
			setupLocalDir();
			emptyS3Bucket();
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
			
			//move a placeholder to a diff folder
			File oriPlaceholder = new File(pathToTestData+filesForUpload[0]+Placeholder.PLACEHOLDER_EXTENSION);
			File destPlaceholder = new File(pathToTestData+"GSync/Cram/"+oriPlaceholder.getName());
			oriPlaceholder.renameTo(destPlaceholder);
			
			
			//upload a new file to S3 with a key that is the destination of a future update
			File uploadFile = new File(pathToTestData+filesNotForUpload[0]);
			String key = pathToTestData+"GSync/Cram/testNoIndex.bed.gz";

			AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion(Util.getRegionFromCredentials()).build();
			TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).withMultipartUploadThreshold((long) (256 * 1024 * 1024)).build();
			Upload u = tm.upload(testS3BucketName, key.substring(1), uploadFile);
			if (Util.waitForCompletion(u) == false) throw new IOException("Failed S3 upload "+uploadFile);
			
			//attempt an update of paths, this will fail
			gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setUpdateS3Keys(true);
			//trap exception
			try {
				gs.doWork();
			} catch (Exception e) { 
				assertTrue(e.getMessage().contains("Problems found with current or destination keys"));
			}
			assertFalse(gs.isResultsCheckOK());
			

		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	/*
	 * Causing major issues so skipping symlinks for now
	 * 
	@Test
	public void testSymlinkUpdate() {
		try {
			//setup
			setupLocalDir();
			emptyS3Bucket();

			//create a symlink in the Zip directory to a bam that's going to get uploaded and deleted
			Path realBam = Paths.get(pathToTestData+filesForUpload[7]);
			Path symlink = Paths.get(pathToTestData+"GSync/Zip/"+realBam.getFileName());
			Files.createSymbolicLink(symlink, realBam);
			assertTrue(Files.exists(symlink));

			//run a gsync and delete uploaded files
			GSync gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setUpdateS3Keys(true);
			gs.setUpdateSymlinks(true);
			gs.setVerbose(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
			assertFalse(Files.exists(realBam));

			//rerun to update the symlink
			gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setUpdateS3Keys(true);
			gs.setUpdateSymlinks(true);
			gs.setVerbose(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
			
			//test that old symlink is replaced with new
			File newSymLink = new File(pathToTestData+"GSync/Zip/"+realBam.getFileName()+Placeholder.PLACEHOLDER_EXTENSION);
			assertTrue(newSymLink.exists());
			//check that it correctly points to the source
			assertTrue(Files.exists(newSymLink.toPath()));
			//old is deleted
			assertFalse(symlink.toFile().exists());
			
			//restore the file by renaming the placeholder to the restore name
			File placeholder = new File(realBam.toString()+Placeholder.PLACEHOLDER_EXTENSION);
			assertTrue(placeholder.exists());
			File restorePlaceholder = new File(realBam.toString()+Placeholder.RESTORE_PLACEHOLDER_EXTENSION);
			placeholder.renameTo(restorePlaceholder);
			
			//rerun gsync to restore file, this will break the symlink
			gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setUpdateS3Keys(true);
			gs.setUpdateSymlinks(true);
			gs.setVerbose(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
			assertTrue(Files.exists(realBam));
			
			//run again to fix symlink to point to real bam
			gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setUpdateS3Keys(true);
			gs.setUpdateSymlinks(true);
			gs.setVerbose(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
			assertTrue(Files.exists(realBam));
			
			//check that symlink to real bam has been created and the old symlink deleted
			assertTrue(symlink.toFile().exists());
			assertFalse(newSymLink.exists());
			
			//delete the file
			placeholder = new File(realBam.toString()+Placeholder.PLACEHOLDER_EXTENSION);
			assertTrue(placeholder.exists());
			File deletePlaceholder = new File(realBam.toString()+Placeholder.DELETE_PLACEHOLDER_EXTENSION);
			placeholder.renameTo(deletePlaceholder);
			
			//rerun gsync to delete the file and placeholder, this will break the symlink
			gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setUpdateS3Keys(true);
			gs.setUpdateSymlinks(true);
			gs.setVerbose(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
			assertFalse(Files.exists(realBam));
			
			//rerun gsync to delete the symlink
			gs = new GSync();
			gs.setLocalDir(new File(pathToTestData+"/GSync"));
			gs.setMinGigaBytes(0.0005);
			gs.setBucketName(testS3BucketName);
			gs.setDryRun(false);
			gs.setDeleteUploaded(true);
			gs.setUpdateS3Keys(true);
			gs.setUpdateSymlinks(true);
			gs.setVerbose(true);
			gs.doWork();
			assertTrue(gs.isResultsCheckOK());
			assertFalse(symlink.toFile().exists());
		} catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Exception caught.");
		}
	}*/


	/**Returns key:objectSummary */
	private HashMap<String, S3ObjectSummary> fetchS3Objects() {
		AmazonS3 s3 = null;
		try {
			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

			HashMap<String, S3ObjectSummary> kos = new HashMap<String, S3ObjectSummary>();
			ListObjectsV2Result result;
			
			//must watch for truncated results
			do {
				result = s3.listObjectsV2(testS3BucketName);
				for (S3ObjectSummary os : result.getObjectSummaries()) kos.put(os.getKey(), os);
				result.setContinuationToken(result.getNextContinuationToken());
			} while (result.isTruncated());

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
				"ln -s "+pathToTestData+"GSync/Bam/testWithIndex.bam "+pathToTestData+"GSync/NoUpload/testSymLink.bam"
		};
		String c = Util.stringArrayToString(cmds, "\n");
		Util.executeShellScript(c, new File(pathToTestData));
	}

	public static void p(String s) {
		System.out.println(s);
	}
}
