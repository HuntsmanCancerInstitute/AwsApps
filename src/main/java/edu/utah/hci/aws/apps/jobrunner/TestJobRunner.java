package edu.utah.hci.aws.apps.jobrunner;

import org.junit.Test;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

import edu.utah.hci.aws.apps.gsync.GSync;
import edu.utah.hci.aws.apps.gsync.Placeholder;
import edu.utah.hci.aws.util.Util;
import static org.junit.Assert.*;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**JUnit tests for the JobRunner application. Adjust the paths to match your resources.
   1) Create a bucket on S3 for just testing. It will be cleared.
   2) Create this file ~/.aws/credentials with your access, secret, and region info, chmod 600 the file and keep it private.
		[default]
		aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A  
		aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ
		region = us-west-2	
   3) Install and configure the aws cli https://aws.amazon.com/cli/
 */
public class TestJobRunner {

	/*Adjust these fields to match your testing environment	 */

	/**Be sure this bucket exists, is private, and doesn't contain anything you care about. WARNING, it will be emptied!*/
	private static final String s3BucketUri = "s3://hcibioinfo-jobrunner-test/";
	private static final String s3BucketName = "hcibioinfo-jobrunner-test";

	/**Directory in the AwsApps project containing the JobRunner test files. */
	private static final File testDataDir = new File("/Users/u0028003/Code/AwsApps/TestData/JobRunner");

	/**Don't put this in the AwsApps dir. It will be deleted.*/
	private static final File awsTmpDir = new File("/Users/u0028003/TmpDelmeAWS");

	private static final File awsCredentialsFile = new File("/Users/u0028003/.aws/credentials");
	private static final String profile = "default";

	private static final String awsPath="/usr/local/bin/aws";


	/* No need to modify anything below */
	private HashMap<String, String> envPropToAdd = null;
	private String region = null;
	private String signedCredUrl = null;


	@Test
	public void testJob() {
		try {
			//set a variety of paths and dirs
			setPaths();

			//empty the bucket
			assertTrue(emptyBucket());

			//upload the zip and test job files
			assertTrue(uploadFile(testDataDir, true));

			//upload the credentials file
			assertTrue(uploadFile(awsCredentialsFile, false));

			//fetch a timed cred url
			fetchSignedUrl();

			//launch JobRunner
			String[] args = new String[]{
					"-d", awsTmpDir.getCanonicalPath(),
					"-a", awsTmpDir.getCanonicalPath(),
					"-c", signedCredUrl.toString(),
					"-r", s3BucketUri+"JRTestDataBundle.zip",
					"-j", s3BucketUri,
					"-l", s3BucketUri+"NodeLogs/",
					"-x", "-t", "-i"
			};

			//should exit with a zero, OK
			assertTrue(new JobRunner(args).getExitCode() == 0);

			//pull list of files and check
			String[] allFiles = listObjects(s3BucketUri);
			assertTrue( allFiles.length == 19);
			String[] instComp = testJobObjects(allFiles);
			assertTrue (instComp != null);

			//download and check the NodeComplete log
			assertTrue( checkNodeCompleteLog(instComp[1], "s3://hcibioinfo-jobrunner-test/TestJobs/OKJob1/mainJob.sh", "OK"));


		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}

	@Test
	public void badCredAndBundle() {
		try {
			//set a variety of paths and dirs
			setPaths();

			//empty the bucket
			assertTrue(emptyBucket());

			//upload the zip and test job files
			assertTrue(uploadFile(testDataDir, true));

			//upload the credentials file
			assertTrue(uploadFile(awsCredentialsFile, false));

			//fetch a timed cred url
			fetchSignedUrl();

			//launch JobRunner
			String[] args = new String[]{
					"-d", awsTmpDir.getCanonicalPath(),
					"-a", awsTmpDir.getCanonicalPath(),
					"-c", signedCredUrl.toString(),
					"-r", s3BucketUri+"JRTestDataBundleXXXX.zip",
					"-j", s3BucketUri,
					"-l", s3BucketUri+"NodeLogs/",
					"-x", "-t", "-i"
			};

			//should exit with a non zero, fail
			assertFalse(new JobRunner(args).getExitCode() == 0);
			
			//try again but with broken credentials url
			Util.deleteDirectory(awsTmpDir);
			awsTmpDir.mkdirs();
			args = new String[]{
					"-d", awsTmpDir.getCanonicalPath(),
					"-a", awsTmpDir.getCanonicalPath(),
					"-c", signedCredUrl + "xxx",
					"-r", s3BucketUri+"JRTestDataBundle.zip",
					"-j", s3BucketUri,
					"-l", s3BucketUri+"NodeLogs/",
					"-x", "-t", "-i"
			};

			//should exit with a non zero, fail
			assertFalse(new JobRunner(args).getExitCode() == 0);

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}
	
	@Test
	public void badTestJob() {
		try {
			//set a variety of paths and dirs
			setPaths();

			//empty the bucket
			assertTrue(emptyBucket());

			//upload the zip and test job files
			assertTrue(uploadFile(testDataDir, true));

			//upload the credentials file
			assertTrue(uploadFile(awsCredentialsFile, false));

			//fetch a timed cred url
			fetchSignedUrl();

			//add a start script to the bad job
			Util.deleteDirectory(awsTmpDir);
			awsTmpDir.mkdirs();
			s3CopyFile("s3://hcibioinfo-jobrunner-test/TestJobs/BadJob/mainJob.sh", "s3://hcibioinfo-jobrunner-test/TestJobs/BadJob/mainJob.sh_JR_START");
			String[] args = new String[]{
					"-d", awsTmpDir.getCanonicalPath(),
					"-a", awsTmpDir.getCanonicalPath(),
					"-c", signedCredUrl,
					"-r", s3BucketUri+"JRTestDataBundle.zip",
					"-j", s3BucketUri,
					"-l", s3BucketUri+"NodeLogs/",
					"-x", "-t", "-i"
			};

			//should exit with a zero, OK
			assertTrue(new JobRunner(args).getExitCode() == 0);
			
			//fetch the NodeComplete and check for the failed job
			assertTrue(checkNodeLogForJobError());

		} catch (Exception e) {
			e.printStackTrace();
			fail("Exception caught.");
		}
	}


	private boolean checkNodeLogForJobError() throws Exception {
		//fetch the log files and find the NodeComplete
		String[] allObjects = listObjects(s3BucketUri+"NodeLogs");
		String comp = null;
		for (String o: allObjects) {
			if (o.contains("NodeComplete")) {
				comp = Util.WHITESPACE.split(o)[3];
				break;
			}
		}
		if (comp == null) {
			Util.el("FAILED to find the NodeComplete log.");
			return false;
		}
		return checkNodeCompleteLog(comp, "s3://hcibioinfo-jobrunner-test/TestJobs/BadJob/mainJob.sh", "ERROR");
	}

	private boolean s3CopyFile(String source, String destination) throws Exception {
		Util.pl("Copying file...");
		String[] cmd = {awsPath, "s3", "cp", source, destination};
		int exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);
		if (exitCode == 0) return true;
		return false;
	}

	private boolean checkNodeCompleteLog(String log, String startsWith, String endsWith) throws IOException {
		Util.pl("Checking nodeComplete log....");
		File logFile = new File(awsTmpDir, "nodeCompleteLog.txt");
		String[] cmd = new String[]{awsPath, "s3", "cp", s3BucketUri+log, logFile.getCanonicalPath()};
		Util.executeViaProcessBuilder(cmd, false, envPropToAdd);
		if (logFile.exists()== false) {
			Util.el("\tFAILED to download the nodeCompleteLog?!");
			return false;
		}

		String[] lines = Util.loadTxtFile(logFile);
		for (String l: lines) {
			l = l.trim();
			if (l.startsWith(startsWith) && l.endsWith(endsWith)) return true;
		}
		return false;
	}

	private String[] testJobObjects(String[] allFiles) {
		Util.pl("Testing bucket contents after job completes....");
		/*
2021-12-15 09:13:37       4099 NodeLogs/u00280003.local_NodeComplete_15Dec2021-9-12.txt
2021-12-15 09:13:12       1628 NodeLogs/u00280003.local_NodeInstantiated_15Dec2021-9-12.txt
2021-12-15 09:12:56        896 JRTestDataBundle.zip
2021-12-15 09:12:56        102 TestJobs/BadJob/mainJob.sh
2021-12-15 09:12:56        165 TestJobs/BadJob/mainJob_JR_INIT.sh
2021-12-15 09:12:56        107 TestJobs/BadJob/mainJob_JR_TERM.sh
2021-12-15 09:13:33        896 TestJobs/OKJob1/TestDownload.zip
2021-12-15 09:13:33         85 TestJobs/OKJob1/mainJob.sh
2021-12-15 09:13:35       1315 TestJobs/OKJob1/mainJob.sh_JR_LOG.txt
2021-12-15 09:13:33        165 TestJobs/OKJob1/mainJob_JR_INIT.sh
2021-12-15 09:13:33        107 TestJobs/OKJob1/mainJob_JR_TERM.sh
2021-12-15 09:13:33          0 TestJobs/OKJob1/termCompleted.txt
2021-12-15 09:13:18        165 TestJobs/OKJob2/JR_INIT.sh
2021-12-15 09:13:18        114 TestJobs/OKJob2/JR_TERM.sh
2021-12-15 09:13:18        896 TestJobs/OKJob2/RenamedArchive.zip
2021-12-15 09:13:18        102 TestJobs/OKJob2/mainJob.sh
2021-12-15 09:13:19       1040 TestJobs/OKJob2/mainJob.sh_JR_LOG.txt
2021-12-15 09:13:18          0 TestJobs/OKJob2/termCompletedForJob2.txt
2021-12-15 09:12:57        136 credentials
		 */
		String[] objectNames = {"JRTestDataBundle.zip", "TestJobs/BadJob/mainJob.sh", "TestJobs/BadJob/mainJob_JR_INIT.sh", "TestJobs/BadJob/mainJob_JR_TERM.sh", "TestJobs/OKJob1/TestDownload.zip", "TestJobs/OKJob1/mainJob.sh", "TestJobs/OKJob1/mainJob.sh_JR_LOG.txt", "TestJobs/OKJob1/mainJob_JR_INIT.sh", "TestJobs/OKJob1/mainJob_JR_TERM.sh", "TestJobs/OKJob1/termCompleted.txt", "TestJobs/OKJob2/JR_INIT.sh", "TestJobs/OKJob2/JR_TERM.sh", "TestJobs/OKJob2/RenamedArchive.zip", "TestJobs/OKJob2/mainJob.sh", "TestJobs/OKJob2/mainJob.sh_JR_LOG.txt", "TestJobs/OKJob2/termCompletedForJob2.txt", "credentials"};
		HashSet<String> expectedContents = new HashSet<String>();
		for (String on: objectNames) expectedContents.add(on);
		String nodeComplete = null;
		String nodeInstantiated = null;
		boolean failed = false;
		for (String fileLine: allFiles) {
			String[] fields = Util.WHITESPACE.split(fileLine.trim());
			String o= fields[3];
			if (expectedContents.contains(o) == false) {
				if (o.contains("NodeComplete")) nodeComplete = o;
				else if (o.contains("NodeInstantiated")) nodeInstantiated = o;
				else {
					failed = true;
					Util.el("\tUnrecognized s3 object : "+o);
				}
			}
		}
		if (failed || nodeComplete == null || nodeInstantiated == null) return null;

		return new String[] {nodeInstantiated, nodeComplete};
	}

	public String[] listObjects(String s3Uri) throws Exception {
		Util.pl("Listing contents in  "+s3Uri);
		String[] cmd = new String[]{awsPath, "s3", "ls", "--recursive", s3Uri};
		String[] out = Util.executeViaProcessBuilder(cmd, false, envPropToAdd);
		return out;
	}

	public void fetchSignedUrl() throws Exception {
		Util.pl("Fetching signed url...");
		String[] cmd = {awsPath, "--region", region, "s3", "presign", s3BucketUri+ awsCredentialsFile.getName(), "--expires-in", "25920"};
		String[] out = Util.executeViaProcessBuilder(cmd, false, envPropToAdd);
		if (out.length !=1 || out[0].startsWith("https") == false) throw new Exception("ERROR: failed to pull a signed credentials url");
		signedCredUrl = out[0];
	}

	public boolean emptyBucket() throws Exception {
		Util.pl("Emptying test bucket...");

		String[] cmd = {awsPath, "s3", "rm", "--recursive", s3BucketUri};
		int exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);

		if (exitCode == 0) return true;
		return false;
	}

	public boolean uploadFile(File f, boolean recursive) throws Exception {
		Util.pl("Uploading "+f);

		String[] cmd = null;
		if (recursive) cmd = new String[]{awsPath, "s3", "cp", "--recursive", f.getCanonicalPath(), s3BucketUri};
		else cmd = new String[]{awsPath, "s3", "cp", f.getCanonicalPath(), s3BucketUri};
		int exitCode = Util.executeReturnExitCode(cmd, false, true, envPropToAdd);

		if (exitCode == 0) return true;
		return false;
	}

	public void setPaths() throws Exception {
		//check bucket name
		if (s3BucketUri.endsWith("/") == false) throw new Exception ("ERROR: your s3 bucket name doesn't end with /");

		//remove then make
		Util.deleteDirectory(awsTmpDir);
		awsTmpDir.mkdirs();

		if (envPropToAdd == null) {
			envPropToAdd = new HashMap <String, String>();
			envPropToAdd.put("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/X11/bin");
		}

		region = Util.fetchBucketRegion(profile, s3BucketName);
	}

	public static void p(String s) {
		System.out.println(s);
	}
}
