package edu.utah.hci.aws.apps.versions;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import edu.utah.hci.aws.util.Util;
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;


/**JUnit tests for the AWS S3 Object Version Manager application. Adjust the paths to match your resources.
   1) Create a bucket on S3 with object versioning enabled for just testing. It will be emptied so don't put anything in it. 
   2) Install and configure the aws cli https://aws.amazon.com/cli/
   3) Create this file ~/.aws/credentials with your access, secret, and region info, chmod 600 the file and keep it private.
		[default]
		aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A  
		aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ
		region = us-west-2	
	  Alternatively, run the 'aws configure' cmd.

 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestVersionManager {

	/*Adjust these fields to match your testing environment	 */

	/**Be sure this bucket exists, is private, versioning enabled, and doesn't contain anything you care about. WARNING, it will be emptied!*/
	private static final String bucketName = "hcibioinfo-nix-test";

	/**Directory in the AwsApps project containing the JobRunner test files. */
	private static final File testDataDir = new File("/Users/u0028003/Code/AwsApps/TestData/VersionManager");

	/**Path to the installed aws cli app*/
	private static final String awsPath="/usr/local/bin/aws";


	/* No need to modify anything below */
	private static final HashMap<String, String> envPropToAdd = null;
	
	@BeforeClass
	public static void setupBucket() throws Exception {
		Util.pl("\nSetting up bucket...");
		//set a variety of paths and dirs
		//setPaths();
		//empty the bucket
		if (emptyBucket() != true) throw new IOException("FAILED to empty the s3://"+bucketName);
		//upload the test files, delete some, overwrite others
		if (uploadTestFiles()!= true) throw new IOException("FAILED to upload, overwrite, or delete test objects");
		//wait 30 sec
		Util.pl("Waiting 5 sec...");
		Thread.sleep(5000);
		Util.pl("Set up complete!");
	}
	
	@Test
	public void testDefaultParams() {
		try {
			//this is actually age >= 7 days
			VersionManager vm = new VersionManager(new String[] {"-b", bucketName});
			assertTrue( vm.getExitCode() == 0);
			assertTrue(vm.getNumberKeysScanned() == 5);
			assertTrue(vm.getNumberObjectsScanned() == 6);
			assertTrue(vm.getNumberObjectsDeleted() == 0);
			assertTrue(vm.getNumberMarksScanned() == 3);
			assertTrue(vm.getNumberMarksDeleted() == 0);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testAgeZero() {
		try {
			
			VersionManager vm = new VersionManager(new String[] {"-b", bucketName, "-a", "0"});
			assertTrue( vm.getExitCode() == 0);
			assertTrue(vm.getNumberKeysScanned() == 5);
			assertTrue(vm.getNumberObjectsScanned() == 6);
			assertTrue(vm.getNumberObjectsDeleted() == 4);
			assertTrue(vm.getNumberMarksScanned() == 3);
			assertTrue(vm.getNumberMarksDeleted() == 3);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testPrefix() {
		try {
			
			VersionManager vm = new VersionManager(new String[] {"-b", bucketName, "-a", "0", "-p", "Larry,Delme,Susan"});
			assertTrue( vm.getExitCode() == 0);
			assertTrue(vm.getNumberKeysScanned() == 5);
			assertTrue(vm.getNumberObjectsScanned() == 6);
			assertTrue(vm.getNumberObjectsDeleted() == 1);
			assertTrue(vm.getNumberMarksScanned() == 3);
			assertTrue(vm.getNumberMarksDeleted() == 1);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testSuffix() {
		try {
			
			VersionManager vm = new VersionManager(new String[] {"-b", bucketName, "-a", "0", "-s", "Larry,.delme,Susan"});
			assertTrue( vm.getExitCode() == 0);
			assertTrue(vm.getNumberKeysScanned() == 5);
			assertTrue(vm.getNumberObjectsScanned() == 6);
			assertTrue(vm.getNumberObjectsDeleted() == 1);
			assertTrue(vm.getNumberMarksScanned() == 3);
			assertTrue(vm.getNumberMarksDeleted() == 1);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void testZDelete() {
		try {
			
			VersionManager vm = new VersionManager(new String[] {"-b", bucketName, "-a", "0", "-r"});
			assertTrue( vm.getExitCode() == 0);
			assertTrue(vm.getNumberKeysScanned() == 5);
			assertTrue(vm.getNumberObjectsScanned() == 6);
			assertTrue(vm.getNumberObjectsDeleted() == 4);
			assertTrue(vm.getNumberMarksScanned() == 3);
			assertTrue(vm.getNumberMarksDeleted() == 3);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	

	public static boolean emptyBucket() throws Exception {
		Util.pl("Emptying test bucket...");

		//mark all the files for deletion
		String[] cmd = {awsPath, "s3", "rm", "--recursive", "s3://"+bucketName};
		int exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode !=0) return false;
		
		//use the VersionManager to delete everything
		VersionManager vm = new VersionManager(new String[] {"-b", bucketName, "-r", "-a", "0", "-q"});
		exitCode = vm.getExitCode();
		
		if (exitCode == 0) return true;
		return false;
	}
	
	public static boolean uploadTestFiles() throws Exception {
		Util.pl("Uploading test files...");
		
		String testA = new File(testDataDir, "testA.txt").getAbsolutePath();
		String testB = new File(testDataDir, "testB.txt").getAbsolutePath();
		
		// upload the first
	    // overwrite the first with testB.txt, this will be the latest and shouldn't be deleted, the overwritten obj is still there with isLatest=false
		String[] cmd = new String[]{awsPath, "s3", "cp", testA , "s3://"+bucketName+"/test.txt"};
		int exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
		cmd = new String[]{awsPath, "s3", "cp", testB, "s3://"+bucketName+"/test.txt"};
		exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
		
		// upload a second 
		cmd = new String[]{awsPath, "s3", "cp", testA, "s3://"+bucketName+"/ToDelete/testA.txt"};
		exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
		// delete/hide it to create a deletion marker and another isLatest=false object
		cmd = new String[]{awsPath, "s3", "rm", "s3://"+bucketName+"/ToDelete/testA.txt"};
		exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
		
	    // upload a third, it should never be deleted since it is the latest
		cmd = new String[]{awsPath, "s3", "cp", testA, "s3://"+bucketName+"/Untouched/testA.txt"};
		exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
		
		// upload a forth with a uniq extension, and delete/hide it
		cmd = new String[]{awsPath, "s3", "cp", testA, "s3://"+bucketName+"/testA.delme"};
		exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
		cmd = new String[]{awsPath, "s3", "rm", "s3://"+bucketName+"/testA.delme"};
		exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
			
		// upload a fifth with a uniq prefix, and delete/hide it
		cmd = new String[]{awsPath, "s3", "cp", testA, "s3://"+bucketName+"/Delme/testA.txt"};
		exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
		cmd = new String[]{awsPath, "s3", "rm", "s3://"+bucketName+"/Delme/testA.txt"};
		exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode != 0) return false;
		
		return true;
	}

	/*
	public void setPaths() throws Exception {
		if (envPropToAdd == null) {
			envPropToAdd = new HashMap <String, String>();
			envPropToAdd.put("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/X11/bin");
		}
	}*/

	public static void p(String s) {
		System.out.println(s);
	}
}
