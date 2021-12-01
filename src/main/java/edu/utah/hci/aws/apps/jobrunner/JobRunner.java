package edu.utah.hci.aws.apps.jobrunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.utah.hci.aws.util.Util;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;


/**Looks for bash scripts in a particular s3 bucket, reanames each, downloads, and runs, and transfers back jobs results 
 * using AWS S3 and the AWS CLI. Before running a job, it copies over a 
 * zip archive and uncompresses it in a resource directory
 * 
 * 24hr url
 * aws --region us-west-2 s3 presign s3://hcibioinfo-jobrunner/Credentials/aws.cred.txt  --expires-in 259200
 * 
cd /Users/u0028003/Downloads/JobRunner
aws s3 rm s3://hcibioinfo-jobrunner/NodeLogs/ --recursive
aws s3 rm s3://hcibioinfo-jobrunner/Jobs/ --recursive
aws s3 cp testJob.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/testJob.sh
aws s3 cp JR_INIT.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/
aws s3 cp JR_TERM.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/
aws s3 cp testJob.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/testJob.sh_JR_START
aws s3 cp testJob.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/mockDoc.txt
aws s3 cp testJob.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobB/testJob.sh
aws s3 cp testJob.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobB/testJob.sh_JR_START
aws s3 ls s3://hcibioinfo-jobrunner/ --recursive

* using a docker container

docker run -it -v /home/u0028003:/home/u0028003 hcibioinformatics/public:JR_AWS_SM_1 \
java -jar -Xmx2G /BioApps/AwsApps/JobRunner.jar \
or
java -jar -Xmx2G ~/Code/AwsApps/target/JobRunner.jar \
-c 'https://hcibioinfo-jobrunner.s3.us-west-2.amazonaws.com/Credentials/aws.cred.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARHBYAZBR33RCJK6A%2F20210714%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20210714T223034Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=416f5f3a77652edd37ca193c5daa3c461b6a76a88d30623f117beec87232d70d' \
-r s3://hcibioinfo-jobrunner/ResourceBundles/TNRunnerResTest.zip \
-j s3://hcibioinfo-jobrunner/Jobs \
-t /Users/u0028003/Downloads/JobRunner/TempDelme \
-l s3://hcibioinfo-jobrunner/NodeLogs

directly on ec2 node
scp -i ~/HCI/Amazon/NixEc2.pem ~/Code/AwsApps/target/JobRunner.jar ec2-user@ec2-52-26-13-150.us-west-2.compute.amazonaws.com:/home/ec2-user
sudo yum install java
sudo pip3 install ec2metadata
sudo ln -s /usr/local/bin/ec2metadata /usr/bin/ec2metadata

sudo java -jar -Xmx2G JobRunner.jar \
-c "https://hcibioinfo-jobrunner.s3.us-west-2.amazonaws.com/Credentials/aws.cred.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARHBYAZBR33RCJK6A%2F20210804%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20210804T163123Z&X-Amz-Expires=259200&X-Amz-SignedHeaders=host&X-Amz-Signature=b0b5d8dd2f17e1e6d487b42408c235c755b007b506750b3c6c78ac220138bd55" \
-r s3://hcibioinfo-jobrunner/ResourceBundles/TNRunnerResTest.zip \
-j s3://hcibioinfo-jobrunner/Jobs \
-l s3://hcibioinfo-jobrunner/NodeLogs

 * 
 * Daemon used to run jobs on AWS EC2 nodes.

 */
public class JobRunner {
	
	//user defined fields
	private File workDirectory = new File ("/JRDir");
	private File tmpDirectory = null;
	private String resourceS3Uri = null;
	private String jobsS3Uri = null;
	private String logsS3Uri = null;
	private String credentialsUrl = null;
	private boolean verbose = false;
	private boolean syncDirs = true;
	private boolean terminateInstance = false;

	//internal fields
	private String jobStartSuffix = "_JR_START";
	private String preJobSuffix = "JR_INIT.sh";
	private String postJobSuffix = "JR_TERM.sh";
	private String numberProcessors = "NA";
	private String ram = "NA";
	private String availableDisk = "NA";
	private String availabilityZone = null;
	private String instanceId = null;
	private String instanceType = null;
	private String awsPath = "aws";
	private StringBuilder hostLog = new StringBuilder();
	private File awsCredentialsDir = null;
	private HashMap<String, String> envPropToAdd = new HashMap <String, String>();
	private String hostName = null;

	private boolean resourceBundleDownloaded = false;
	private boolean wroteInstantiationLog = false;
	private File credentialsFile = null;
	private StringBuilder jobSummaries = new StringBuilder();
	private String[] environment = null;
	private long totalRunTime = 0;
	private String date = null;

	private static final String nodeInstantiated = "_NodeInstantiated_";
	private static final String nodeError = "_NodeError_";
	private static final String nodeComplete = "_NodeComplete_";
	private static final String jobRequested = "_JR_REQUESTED_";
	private static final String jobRunning = "_JR_RUNNING_";
	private static final String jobLog = "_JR_LOG.txt";
	private static final String jobLogError = "_JR_LOG_ERROR.txt";
	
	private JRJob workingJob = null;
	
	public JobRunner (String[] args){
		
		try {
			long startTimeTotal = System.currentTimeMillis();
			
			processArgs(args);

			loadCredentials();

			checkAwsCli();

			checkResourceBundle();
			
			runJobs();

			//shutdown node
			pl("\nNo more jobs...");
			totalRunTime = Math.round(((double)(System.currentTimeMillis() -startTimeTotal))/60000);
			writeSummaryStats();
			writeNodeComplete();
			
			pl("\nShutting down node...");
			shutDown(0);
			
		} catch (Exception e) {
			el("ERROR: "+e.getMessage());
			if (verbose) el(Util.getStackTrace(e));
			el("\nShutting down node...");
			try {
				writeNodeError(Util.getStackTrace(e));
			} catch (IOException e1) {e1.printStackTrace();}
			try {
				shutDown(1);
			} catch (Exception e1) {e1.printStackTrace();}
		}
	}

	private void runJobs() throws Exception {

		while (true) {

			//fetch next job
			for (int i=0; i< 60; i++) {
				workingJob = fetchJob();
				//not null, ready to go so break out of loop
				if (workingJob != null) break;
				// null and user want's to terminate immediately
				else if (terminateInstance) {
					writeInstantiationLog();
					return;
				}
				//just wait 1 min, then look again
				else {
					pl("\tNo jobs, waiting "+(59-i)+"m...");
					Thread.currentThread().sleep(1000*60);
				}
			}
			
			//did loop end without finding a job?
			if (workingJob == null) {
				writeInstantiationLog();
				return;
			}

			//check and if not present, download and unzip the data bundle, only do this if there is a job to run, will exit if a problem is found
			downloadResourceBundle();
			writeInstantiationLog();
			
			long startTime = System.currentTimeMillis();

			pl("\n----------------------- Launching a new job "+workingJob.getS3UrlRunningJobPath()+workingJob.getScriptFileName()+" -----------------------\n");

			//delete, then make the working job dir
			makeJobDirectory();

			//cp S3Job dir with local Job directory
			cpS3JobDirWithLocal();
			
			//execute the job
			String message = executeJobScripts();

			//sync local job dir with S3JobDir
			if (syncDirs) syncLocalJobDirWithS3JobDir();
			else deleteAndCopyLocalJobDirWithS3JobDir();
			
			long diffTime = Math.round(((double)(System.currentTimeMillis() -startTime))/60000);
			jobSummaries.append(workingJob.getS3UrlRunningJobPath()+workingJob.getScriptFileName()+"\t"+diffTime+"\t"+message+"\n");
			
			pl("\n----------------------- Job "+workingJob.getS3UrlRunningJobPath()+" finished in "+diffTime+" min, status: "+message+"   -----------------------");
			
			cleanUpWorkingJob();
		}
		
	}

	private void cleanUpWorkingJob() throws Exception {
		//close the print writer
		workingJob.getJobLogOut().close();
		//upload the log file to the S3 job
		String jl = jobLog;
		if (workingJob.isCompletedOK() == false) jl = jobLogError;
		writeS3File(workingJob.getS3UrlRunningJobPath()+ workingJob.getScriptFileName()+ jl, workingJob.getJobLogFile());
		//delete the job dir
		Util.deleteDirectory(workingJob.getLocalJobDir());
	}


	private void writeSummaryStats() {
		pl("\nJob Summary Statistics:\n\nJob Name\tRun Time (min)\tExit Status");
		pl(jobSummaries.toString());
		
		pl("Total Run Time (min):\t"+totalRunTime);
		
	}
	
	private void makeS3JobRequest(JRJob jrJob) throws Exception {
		String requestFileName = jrJob.getScriptFileName()+jobRequested+hostName;
		
		File request = new File (tmpDirectory, requestFileName);
		request.createNewFile();
		if (verbose) pl("\tUploading "+requestFileName+" to "+jrJob.getS3UrlRunningJobPath()+" ...");
		String[] cmd = {awsPath, "s3", "cp", request.getCanonicalPath(), jrJob.getS3UrlRunningJobPath()};
		int exitCode = executeReturnExitCode(cmd, false, true, null);
		if (exitCode == 0) {
			jrJob.setJobRequestFileName(requestFileName);
			return;
		}
		
		//this should not fail unless a major problem occurs
		throw new IOException("\tFailed to upload job request "+requestFileName+" to "+jrJob.getS3UrlRunningJobPath());
	}
	
	private void writeS3JobRunning(JRJob jrJob) throws Exception {
		String jobRunningFileName = jrJob.getScriptFileName()+jobRunning+hostName;
		
		File request = new File (tmpDirectory, jobRunningFileName);
		request.createNewFile();
		if (verbose) pl("\tUploading "+jobRunningFileName+" to "+jrJob.getS3UrlRunningJobPath()+" ...");
		String[] cmd = {awsPath, "s3", "cp", request.getCanonicalPath(), jrJob.getS3UrlRunningJobPath()};
		int exitCode = executeReturnExitCode(cmd, false, true, null);
		if (exitCode == 0) {
			jrJob.setJobRunningFileName(jobRunningFileName);
			return;
		}
		
		//this should not fail unless a major problem occurs
		throw new IOException("\tFailed to upload job running "+jobRunningFileName+" to "+jrJob.getS3UrlRunningJobPath());
	}
	
	private void writeS3File(String fullS3FilePathUrl, File file) throws Exception {

		if (verbose) pl("\tUploading "+file+" to "+fullS3FilePathUrl+" ...");
		String[] cmd = {awsPath, "s3", "cp", file.getCanonicalPath(), fullS3FilePathUrl};
		int exitCode = executeReturnExitCode(cmd, false, true, null);
		if (exitCode == 0) return;
		
		//this should not fail unless a major problem occurs
		throw new IOException("\tFailed to upload "+file+" to "+fullS3FilePathUrl);
	}
	
	private void deleteS3Objects(ArrayList<String> s3UrlsToDelete) throws Exception {
		for (String d: s3UrlsToDelete) {
			String[] cmd = {awsPath, "s3", "rm", d};
			int exitCode = executeReturnExitCode(cmd, false, true, null);
			if (exitCode != 0) throw new IOException("\tFailed to delete "+d+", aborting ");
		}
	}

	private void syncLocalJobDirWithS3JobDir() throws Exception {
		if (verbose) pl("\tSyncing "+workingJob.getLocalJobDir()+"/ with "+workingJob.getS3UrlRunningJobPath()+" ...");
		String[] cmd = {awsPath, "s3", "sync", workingJob.getLocalJobDir().getCanonicalPath(), workingJob.getS3UrlRunningJobPath()};
		int exitCode = executeReturnExitCode(cmd, false, true, null);
		if (exitCode == 0) return;
		//this should not fail unless a major problem occurs
		throw new IOException("\tFailed to sync "+workingJob.getLocalJobDir()+" with "+workingJob.getS3UrlRunningJobPath());
	}
	
	private void deleteAndCopyLocalJobDirWithS3JobDir() throws Exception {
		if (verbose) pl("\tDeleting "+workingJob.getS3UrlRunningJobPath()+" ...");
		String[] cmd = {awsPath, "s3", "rm", workingJob.getS3UrlRunningJobPath(), "--recursive"};
		int exitCode = executeReturnExitCode(cmd, false, true, null);
		if (exitCode == 0) {
			if (verbose) pl("\tCopying "+workingJob.getLocalJobDir()+" to "+workingJob.getS3UrlRunningJobPath()+" ...");
			cmd = new String[]{awsPath, "s3", "cp", workingJob.getLocalJobDir().getCanonicalPath(), workingJob.getS3UrlRunningJobPath() , "--recursive"};
			exitCode = executeReturnExitCode(cmd, false, true, null);
		}
		//this should not fail unless a major problem occurs
		if (exitCode !=0) throw new IOException("\tFailed to delete "+workingJob.getS3UrlRunningJobPath()+" and or then copy "+workingJob.getLocalJobDir()+" to "+workingJob.getS3UrlRunningJobPath());
	}
	

	/*writeNodeLogCode 0=noWrite, 1=error, 2=complete*/
	private void shutDown(int exitCode) throws Exception {

		//delete the working directory? not needed this is done by AWS, the individual job dirs are deleted
		//Util.deleteDirectory(workDirectory);
		
		// this will system exit
		if (availabilityZone != null && terminateInstance) {
			String[] cmd = new String[]{awsPath, "ec2", "terminate-instances", "--instance-ids", instanceId};
			executeReturnExitCode(cmd, false, true, null);
		}
		
		//remove the credentials dir
		if (awsCredentialsDir != null) Util.deleteDirectory(awsCredentialsDir);
		
		System.exit(exitCode);
	}

	private void writeInstantiationLog() throws Exception {
		if (wroteInstantiationLog) return;
		wroteInstantiationLog = true;
		
		pl("\n"+hostName+" successfully instantiated...");
		
		File hostLogFile = new File (tmpDirectory, hostName+ nodeInstantiated +date+ ".txt");
		Util.write(hostLog.toString(), hostLogFile);
		String[] cmd = {awsPath, "s3", "cp", hostLogFile.getCanonicalPath(), logsS3Uri};
		if (executeReturnExitCode(cmd, false, true, null) == 0) return;
		//this should not fail unless a major problem is found
		throw new IOException("\tFailed to upload the node instantiation log "+hostLogFile.getCanonicalPath()+" to "+ logsS3Uri );
	}

	private void writeNodeError(String string) throws IOException {
		el(string);

		File hostLogFile = new File (tmpDirectory, hostName+ nodeError+ date+ ".txt");
		Util.write(hostLog.toString(), hostLogFile);

		String[] cmd = {awsPath, "s3", "cp", hostLogFile.getCanonicalPath(), logsS3Uri};
		String[] out = executeViaProcessBuilder(cmd, false, null);
		for (String s: out) {
			if (s.startsWith("upload:")) return;
		}
		//this should not fail unless a major problem is found
		throw new IOException("\tFailed to upload the node error log "+hostLogFile.getCanonicalPath()+" to "+ logsS3Uri);
	}

	private void writeNodeComplete() throws IOException {
		
		File hostLogFile = new File (tmpDirectory, hostName+ nodeComplete + date+ ".txt");
		Util.write(hostLog.toString(), hostLogFile);

		String[] cmd = {awsPath, "s3", "cp", hostLogFile.getCanonicalPath(), logsS3Uri};
		String[] out = executeViaProcessBuilder(cmd, false, null);
		for (String s: out) {
			if (s.startsWith("upload:")) return;
		}
		//this should not fail unless a major problem is found
		throw new IOException("\tFailed to upload the node complete log "+hostLogFile.getCanonicalPath()+" to "+ logsS3Uri );
	}

	private String executeJobScripts() throws Exception {
		int exitCode = 0;
		
		//execute a pre run init shell script?
		File[] preRunScripts = Util.extractFiles(workingJob.getLocalJobDir(), preJobSuffix);
		if (preRunScripts != null && preRunScripts.length ==1) {
			if (verbose) pl("\tExecuting pre run script "+preRunScripts[0].getCanonicalPath());
			preRunScripts[0].setExecutable(true);
			String[] cmd = {preRunScripts[0].getCanonicalPath()};
			exitCode = executeReturnExitCode(cmd, true, true, workingJob.getLocalJobDir());
		}
		
		//execute the main script
		if (exitCode == 0) {
			File jobScript = new File (workingJob.getLocalJobDir(), workingJob.getScriptFileName());
			if (verbose) pl("\tExecuting main job script "+jobScript.getCanonicalPath());
			jobScript.setExecutable(true);
			String[] cmd = {jobScript.getCanonicalPath()};
			exitCode = executeReturnExitCode(cmd, true, true, workingJob.getLocalJobDir());
		}
		
		//execute a post run cleanup shell script?
		if (exitCode == 0) {
			File[] postRunScripts = Util.extractFiles(workingJob.getLocalJobDir(), postJobSuffix);
			if (postRunScripts != null && postRunScripts.length ==1) {
				if (verbose) pl("\tExecuting post run script "+postRunScripts[0].getCanonicalPath());
				postRunScripts[0].setExecutable(true);
				String[] cmd = {postRunScripts[0].getCanonicalPath()};
				exitCode = executeReturnExitCode(cmd, true, true, workingJob.getLocalJobDir());
			}
		}
		
		//Success? 
		String message = null;
		if (exitCode == 0) {
			workingJob.setCompletedOK(true);
			if (verbose) pl("\n\tOK job exit code");
			message = "OK";
		}
		else {
			workingJob.setCompletedOK(false);
			if (verbose) pl("\n\tERROR job exit code, check "+workingJob.getJobLogFile().getName() +" in "+workingJob.getS3UrlRunningJobPath());
			message = "ERROR";
		}
		return message;
	}

	private void makeJobDirectory() throws IOException {
		String relPath = workingJob.getS3UrlRunningJobPath().replace(jobsS3Uri, "RunningJob/");
		File localJobDir = new File(workDirectory, relPath);
		if (localJobDir.exists()) Util.deleteDirectory(localJobDir);
		localJobDir.mkdirs();
		if (localJobDir.exists() == false) throw new IOException("ERROR: Failed to create new job directory "+localJobDir);
		workingJob.setLocalJobDir(localJobDir);
		envPropToAdd.put("JR_JOB_DIR", localJobDir.getCanonicalPath());
	}

	private void downloadResourceBundle() throws Exception {
		//already downloaded?
		if (resourceBundleDownloaded == true) return;

		pl("\nDownloading and uncompressing "+resourceS3Uri+ " into "+workDirectory+" ...");
		String[] cmd = {awsPath, "s3", "cp", resourceS3Uri, workDirectory.getCanonicalPath()};
		String[] out = executeViaProcessBuilder(cmd, false, null);
		//download: s3://hcibioinfo-jobrunner/TNRunnerResTest.zip to Downloads/MockAWSJobRunnerWorkDir/TNRunnerResTest.zip
		for (String s: out) {
			if (s.startsWith("download:")) {
				String[] fields = Util.FORWARD_SLASH.split(resourceS3Uri);
				String fileName = fields[fields.length-1];
				File zipBundle = new File (workDirectory, fileName);
				if (zipBundle.exists() == false) break;

				String workingDir = workDirectory.getCanonicalPath()+"/";
				cmd = new String[]{"unzip", "-oqd", workingDir, zipBundle.getCanonicalPath()};
				int exitCode = executeReturnExitCode(cmd, true, true, null);
				if (exitCode!=0) break;

				//remove the bundle to save space, these can be quite large
				zipBundle.delete();
				resourceBundleDownloaded = true;
				return;
			}
		}
		//failed! major issue
		throw new IOException("\n\tFailed to download or uncompress the resource zip archive "+resourceS3Uri+" "+Util.stringArrayToString(out, " "));
	}

	private void cpS3JobDirWithLocal() throws Exception {
		if (verbose) pl("\tCopying "+workingJob.getS3UrlRunningJobPath()+" with "+ workingJob.getLocalJobDir()+"/ ...");
		String[] cmd = {awsPath, "s3", "cp", workingJob.getS3UrlRunningJobPath(), workingJob.getLocalJobDir().getCanonicalPath(), "--recursive"};
		int exitCode = executeReturnExitCode(cmd, false, true, null);
		if (exitCode == 0) {
			//delete JR_RUNNING from local
			File jrr = new File(workingJob.getLocalJobDir(), workingJob.getRunningFileName());
			jrr.delete();
			return;
		}
		//this should not fail unless a major problem occurs
		throw new IOException("\tFailed to cp "+workingJob.getS3UrlRunningJobPath()+" to "+ workingJob.getLocalJobDir().getCanonicalPath());
		

	}

	/**Returns the s3://hcibioinfo-jobrunner/JobsToRun/running_host_xxx Uri or null if no new jobs. */
	private JRJob fetchJob() throws Exception {
		while (true) {

			//pull all of the files in Job dir on S3
			HashMap<String, ArrayList<String>> allObjectHash = listFilesInS3DirRecursive(jobsS3Uri);
			
			//attempt to find an unclaimed job script
			JRJob jrJob = fetchNextJobScript(allObjectHash);
			if (jrJob == null) return null;

			//make a job request
			makeS3JobRequest(jrJob);
			
			//pull the files from the working job dir again, there could be another node that has requested it
			ArrayList<String> jobDirFileNames = listFilesInS3Dir(jrJob.getS3UrlRunningJobPath());

			//collect all of the requests and look for any in process files
			boolean otherFilesFound = false;
			ArrayList<String> requestFileNames = new ArrayList<String>();
			for (String fileName : jobDirFileNames) {
				if (fileName.contains(jobRequested)) requestFileNames.add(fileName);
				else if (fileName.contains(jobRunning) || fileName.contains(jobLogError) || fileName.contains(jobLog)) {
					otherFilesFound = true;
					break;
				}
			}
			//already being/ been processed?
			if (otherFilesFound) continue;
			
			//look to see if this request is the first request
			String s3Request;
			if (requestFileNames.size() == 1) s3Request = requestFileNames.get(0);
			else {
				String[] rfns = new String[requestFileNames.size()];
				requestFileNames.toArray(rfns);
				Arrays.sort(rfns);
				s3Request = rfns[0];
			}
			
			if (jrJob.getRequestFileName().equals(s3Request) == false) continue;
			
			//write a jobRunning file to S3
			writeS3JobRunning(jrJob);
			
			//look for and delete any job status files in the working job dir
			jobDirFileNames = listFilesInS3Dir(jrJob.getS3UrlRunningJobPath());
			ArrayList<String> toDelete = new ArrayList<String>();
			for (String fileName: jobDirFileNames) {
				if (fileName.contains(jobLogError) || fileName.contains(jobLog) || fileName.contains(jobStartSuffix) || fileName.contains(jobRequested)) toDelete.add(jrJob.getS3UrlRunningJobPath()+fileName); 
				else if (fileName.contains(jobRunning) && fileName.equals(jrJob.getRunningFileName()) == false) toDelete.add(jrJob.getS3UrlRunningJobPath()+fileName);
			}
			deleteS3Objects(toDelete);
			
			//start log
			File jobLogFile = new File (tmpDirectory, Util.getRandomString(8)+"_"+jrJob.getScriptFileName()+jobLog);
			jrJob.setJobLogFile(jobLogFile);

			return jrJob;
		}
	}

	private JRJob fetchNextJobScript(HashMap<String, ArrayList<String>> allObjectHash) throws Exception {
		//for each directory
		for (String dir : allObjectHash.keySet()) {
			ArrayList<String> files = allObjectHash.get(dir);
			//look for a file ending in the job script name
			String jobStartScript = null;
			for (String fileName: files) {
				if (fileName.endsWith(jobStartSuffix)) {
					jobStartScript = fileName;
					break;
				}
			}
			
			//was a script found?
			if (jobStartScript != null) {
				boolean othersFound = false;
				for (String fileName: files) {
					if (fileName.contains(jobRequested) || fileName.contains(jobRunning) || fileName.contains(jobLogError) || fileName.contains(jobLog)) {
						othersFound = true;
						break;
					}
				}
				if (othersFound == false) {
					String jobScript = jobStartScript.replace(jobStartSuffix, "");
					//look for the actual job script
					boolean jsFound = false;
					for (String fileName: files) {
						if (fileName.equals(jobScript)) {
							jsFound = true;
							break;
						}
					}
					if (jsFound) return new JRJob (jobsS3Uri+dir, jobScript, jobStartScript);
					else {
						String error = "\nJOB SCRIPT ERROR: Found "+jobStartScript+" in "+jobsS3Uri+dir+ " but no associated script "+jobScript+" , skipping.";
						el(error);
						File fileError = new File (tmpDirectory, jobScript+ jobLogError);
						Util.write(error, fileError);
						writeS3File(jobsS3Uri+dir+fileError.getName(), fileError);
					}
				}
			}
		}
		return null;
	}
	
	private HashMap<String, ArrayList<String>> listFilesInS3DirRecursive (String s3DirUri) throws IOException{
		String[] cmd = {awsPath, "s3", "ls", s3DirUri, "--recursive"};
		String[] out = executeViaProcessBuilder(cmd, false, null);
		
		HashMap<String, ArrayList<String>> fileDirFiles = new HashMap<String, ArrayList<String>>();
		
		for (String s: out) {
			s= s.trim();
			//empty
			if (s.length() == 0) continue;
			//folder?
			if (s.startsWith("PRE ")) continue;
			String[] fields = Util.WHITESPACE.split(s);
			String pathName = fields[fields.length-1];
			int firstSlash = pathName.indexOf("/") +1;
			int lastSlash = pathName.lastIndexOf("/") +1;
			String dirPath = pathName.substring(firstSlash, lastSlash);
			String fileName = pathName.substring(lastSlash);
			ArrayList<String> names = fileDirFiles.get(dirPath);
			if (names == null) {
				names = new ArrayList<String>();
				fileDirFiles.put(dirPath, names);
			}
			names.add(fileName);
		}
		return fileDirFiles;
	}
	
	private ArrayList<String> listFilesInS3Dir (String s3DirUri) throws IOException{
		String[] cmd = {awsPath, "s3", "ls", s3DirUri};
		String[] out = executeViaProcessBuilder(cmd, false, null);
		ArrayList<String> names = new ArrayList<String>();
		for (String s: out) {
			s= s.trim();
			//empty
			if (s.length() == 0) continue;
			//folder?
			if (s.startsWith("PRE ")) continue;
			String[] fields = Util.WHITESPACE.split(s);
			String pathName = fields[fields.length-1];
			String fileName = "";
			if (pathName.contains("/")) {
				int lastSlash = pathName.lastIndexOf("/") +1;
				fileName = pathName.substring(lastSlash);
			}
			else fileName = pathName;
			names.add(fileName);
		}
		return names;
	}
	
	


	/**Uses ProcessBuilder to execute a cmd, combines standard error and standard out into one and returns their output.
	 * @throws IOException */
	public String[] executeViaProcessBuilder(String[] command, boolean printToLog, File workingDirectory) throws IOException{
		if (verbose) pl ("Executing: '"+Util.stringArrayToString(command, " ")+"'");
		ArrayList<String> al = new ArrayList<String>();
		ProcessBuilder pb = new ProcessBuilder(command);
		//add enviro props?
		pb.environment().putAll(envPropToAdd);
		if (workingDirectory !=null) pb.directory(workingDirectory);
		
		pb.redirectErrorStream(true);
		Process proc = pb.start();
		BufferedReader data = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String line;
		while ((line = data.readLine()) != null){
			al.add(line);
			if (printToLog || verbose) pl(line);
		}
		data.close();
		String[] res = new String[al.size()];
		al.toArray(res);
		return res;
	}

	/**Uses ProcessBuilder to execute a cmd, combines standard error and standard out into one and printsToLogs if indicated.
	 * Returns exit code, 0=OK, >0 a problem
	 * @throws IOException */
	public int executeReturnExitCode(String[] command, boolean printToLog, boolean printIfNonZero, File workingDirectory) throws Exception{
		if (verbose) pl ("Executing: "+Util.stringArrayToString(command, " "));
		ProcessBuilder pb = new ProcessBuilder(command);
		//add enviro props?
		pb.environment().putAll(envPropToAdd);
		if (workingDirectory !=null) pb.directory(workingDirectory);
		pb.redirectErrorStream(true);
		Process proc = pb.start();
		BufferedReader data = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		StringBuilder sb = new StringBuilder();
		String line;
		while ((line = data.readLine()) != null) {
			if (printToLog || verbose) pl(line);
			sb.append(line);
			sb.append("\n");
		}
		int exitCode = proc.waitFor();
		if (exitCode !=0 && printIfNonZero) pl(sb.toString());
		return exitCode;
	}

	void pl(String s) {
		System.out.println(s);
		hostLog.append(s);
		hostLog.append("\n");
		System.out.flush();
		if (workingJob!=null && workingJob.getJobLogOut()!= null) {
			PrintWriter jobLogOut = workingJob.getJobLogOut();
			jobLogOut.println(s);
			jobLogOut.flush();
		}
	}
	void p(String s) {
		System.out.print(s);
		hostLog.append(s);
		System.out.flush();
		if (workingJob!=null && workingJob.getJobLogOut()!= null) {
			PrintWriter jobLogOut = workingJob.getJobLogOut();
			jobLogOut.print(s);
			jobLogOut.flush();
		}
	}
	void el(String s) {
		System.err.println(s);
		hostLog.append(s);
		hostLog.append("\n");
		System.err.flush();
		if (workingJob!=null && workingJob.getJobLogOut()!= null) {
			PrintWriter jobLogOut = workingJob.getJobLogOut();
			jobLogOut.println(s);
			jobLogOut.flush();
		}
	}

	public static void main(String[] args) {
		if (args.length ==0){
			printDocs();
			System.exit(0);
		}
		new JobRunner(args);
	}	

	private void checkAwsCli() throws IOException {
		pl("\nChecking the aws cli...");
		
		String[] cmd = {awsPath, "--version"};
		String[] out = executeViaProcessBuilder(cmd, false, null);

		if (out[0].contains("aws-cli") == false) {
			for (String s: out) el(s);
			throw new IOException("Error: 'aws --version' failed to return an appropriate response.");
		}
		else pl("  "+out[0]);
	}

	private void checkResourceBundle() throws IOException {
		pl("\nChecking resource bundle...");

		String[] cmd = {awsPath, "s3", "ls", resourceS3Uri};
		String[] out = executeViaProcessBuilder(cmd, false, null);

		//check it
		String[] fields = resourceS3Uri.split("/");
		if (out.length == 0 ||out[0].contains(fields[fields.length-1]) == false) {
			for (String s: out) el(s);
			throw new IOException("Error: failed to find the resource bundle "+resourceS3Uri);
		}
		else pl("  "+out[0]);
	}

	private void loadCredentials() throws IOException {
		pl("\nDownloading the aws credentials...");

		//it's important to not place this in the work directory since this can be a persistent mount
		awsCredentialsDir = new File (System.getProperty("user.home")+"/.aws");
		awsCredentialsDir.mkdirs();
		if (awsCredentialsDir.exists() == false) throw new IOException("Error: failed to make "+awsCredentialsDir+" for saving the aws credentials file");
		credentialsFile = new File (awsCredentialsDir, "credentials").getCanonicalFile();
		credentialsFile.deleteOnExit();

		//fetch the file with curl
		String[] cmd = {"curl", credentialsUrl, "-o", credentialsFile.toString(), "-s", "-S"};
		String[] out = executeViaProcessBuilder(cmd, false, null);
		if (credentialsFile.exists() == false) {
			for (String s: out) el(s);
			throw new IOException("Failed to download the aws credentials from -> "+credentialsUrl);
		}

		//check it, the downloaded file might be a error message from AWS about expired 
		String[] lines = Util.loadTxtFile(credentialsFile);
		int keyCount = 0;
		for (String l: lines) if (l.contains("aws_access_key_id")) keyCount++;
		String merged = Util.stringArrayToString(lines, "\n\t");
		if (keyCount !=1 || merged.contains("region") == false || merged.contains("aws_access_key_id") == false || merged.contains("aws_secret_access_key") == false) {
			throw new IOException("\tError: the credential file is malformed -> "+credentialsUrl+ "\n\t"+merged+"\n\tSee the JobRunner help menu.");
		}

		//set env var for the aws cli 
		envPropToAdd.put("AWS_SHARED_CREDENTIALS_FILE", credentialsFile.getCanonicalPath());


		//TODO: Only needed in Eclipse
		//envPropToAdd.put("PATH", "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/X11/bin");
		//awsPath="/usr/local/bin/aws";


	}
	

	/**This method will process each argument and assign new variables
	 * @throws Exception */
	public void processArgs(String[] args) throws Exception {

		Pattern pat = Pattern.compile("-[a-z]");
		pl("JobRunner Arguments: "+Util.stringArrayToString(args, " ")+"\n");
		for (int i = 0; i<args.length; i++){
			String lcArg = args[i].toLowerCase();
			Matcher mat = pat.matcher(lcArg);
			if (mat.matches()){
				char test = args[i].charAt(1);
				try{
					switch (test){
					case 'd': workDirectory = new File(args[++i]).getCanonicalFile(); break;
					case 'r': resourceS3Uri = args[++i]; break;
					case 'c': credentialsUrl = args[++i]; break;
					case 'j': jobsS3Uri = args[++i]; break;
					case 'l': logsS3Uri = args[++i]; break;
					case 't': terminateInstance = true; break;
					//case 'e': ebsSize = Integer.parseInt(args[++i]); break;
					case 'v': verbose = true; break;
					case 'x': syncDirs = false; break;
					case 'h': printDocs(); System.exit(0);
					default: Util.printExit("\nProblem, unknown option! " + mat.group());
					}
				}
				catch (Exception e){
					Util.printExit("\nSorry, something doesn't look right with this parameter: -"+test+"\n");
				}
			}
		}
		
		//work directory, need this to find RAM on machine
		if (workDirectory == null) {
			throw new IOException("Error: failed to find your -t temporary local work directory.");
		}
		
		workDirectory.mkdirs();
		if (workDirectory.exists() == false || workDirectory.canWrite() == false) {
			throw new IOException("Error: failed to find a writable work directory -> "+ workDirectory);
		}
		tmpDirectory = new File (workDirectory, "TmpDir");
		tmpDirectory.mkdir();
		
		//set node info
		loadHostInfo();
		
		//add workingDir to env
		envPropToAdd.put("JR_WORKING_DIR", workDirectory.getCanonicalPath());

		printParams();

		// required args?
		if (resourceS3Uri == null || credentialsUrl == null || jobsS3Uri == null || logsS3Uri == null) {
			throw new IOException("Error: failed to find one or more of the required arguments, see Parameters above.");
		}

		//fix jobsS3Uri and logsS3Uri
		if (jobsS3Uri.endsWith("/") == false) jobsS3Uri = jobsS3Uri+"/";
		if (logsS3Uri.endsWith("/") == false) logsS3Uri = logsS3Uri+"/";

		//check S3Uri
		if (resourceS3Uri.startsWith("s3://") == false || jobsS3Uri.startsWith("s3://") == false) {
			throw new IOException("Error: S3URIs must start with s3:// see Parameters above.");
		}
		if (resourceS3Uri.endsWith(".zip") == false) throw new IOException("Error: the zip resource S3Uri must end with xxx.zip, see "+resourceS3Uri);	

	}	

	/*Attempt to get machine info, doesn't work on a mac*/
	private void loadHostInfo() throws UnknownHostException {
		
		String[] out = null;
		try {
			//date
			date = Util.getDateTime("-");
			
			//env		
			environment = executeViaProcessBuilder(new String[]{"env"}, false, null);
			
			//num proc
			try {
				out = executeViaProcessBuilder(new String[]{"nproc"}, false, null);
			} catch (Exception e) {}
			
			if (out!= null && out.length == 1) numberProcessors = out[0];
			
			//Available memory
			try {
				out = Util.executeShellScript("expr `free -g | grep -oP '\\d+' | head -n 1` - 2", workDirectory);
			} catch (Exception e) {}
			if (out!= null && out.length != 0 && out[0].contains("free") == false) {
				ram = out[0];
				if (ram.startsWith("-")) ram = "<1";
			}
			//available disk
			// Filesystem                        Size  Used Avail Use% Mounted on
			// /dev/mapper/cl_hci--clingen-root  275G  147G  128G  54% /
			//   0                                1     2     3    4
			out = executeViaProcessBuilder(new String[]{"df", "-h", workDirectory.getCanonicalPath()}, false, null);
			if (out!= null && out.length == 2) {
				String[] fields = Util.WHITESPACE.split(out[1]);
				if (fields.length == 6) availableDisk = fields[3];
			}
			
			//will only work if this is in an Amazon Linux 2, ec2 instance
			try {
				out = executeViaProcessBuilder(new String[]{"ec2-metadata", "-z", "-i", "-t"}, false, null);
			} catch (Exception e) {}
			if (out!= null && out.length == 3) {
				availabilityZone = Util.WHITESPACE.split(out[0])[1];
				instanceId = Util.WHITESPACE.split(out[1])[1];
				hostName = instanceId;
				instanceType = Util.WHITESPACE.split(out[2])[1];
			}
			else hostName = InetAddress.getLocalHost().getHostName();
			
			/*
			out = executeViaProcessBuilder(new String[]{"ec2metadata", "--availability-zone", "--instance-id", "--instance-type"}, false);
			if (out!= null && out.length == 3) {
				availabilityZone = out[0];
				instanceId = out[1];
				hostName = instanceId;
				instanceType = out[2];
			}
			else hostName = InetAddress.getLocalHost().getHostName();
			*/

			
		} catch (IOException e) {
			pl("ERROR fetching host information\n");
			if (verbose) pl(Util.getStackTrace(e));
		}

	}

	private void printParams() {
		pl("Parameters:");
		String[] cred = Util.QUESTION.split(credentialsUrl);
		pl("  -c Credentials URL     : "+ cred[0]);
		pl("  -r Zip resource S3 URI : "+ resourceS3Uri);
		pl("  -j Jobs S3 URI         : "+ jobsS3Uri);
		pl("  -l Node Logs S3 URI    : "+ logsS3Uri);
		pl("  -d Local work dir      : "+ workDirectory);
		pl("  -t Terminate node on exit    : "+ terminateInstance);
		pl("  -x Replace S3 job with local : "+ (syncDirs==false));

		pl("\nJob Runner Info:");
		pl("  Host name              : "+ hostName);
		pl("  Number processors      : "+ numberProcessors);
		pl("  GB RAM                 : "+ ram);
		pl("  Avail Disk             : "+ availableDisk);
		pl("  Date                   : "+ date);
		
		if (availabilityZone != null) {
			pl("  Availability Zone      : "+ availabilityZone);
			pl("  Instance ID            : "+ instanceId);
			pl("  Instance Type          : "+ instanceType);
			pl("  Terminate upon exit    : "+ terminateInstance);
		}
		
		if (verbose && environment != null) {
			pl("\nEnvironment:");
			for (String s: environment) pl("  "+s);
		}
		
		
	}

	public static void printDocs(){
		System.out.println("\n" +
				"****************************************************************************************************************************\n" +
				"**                                              AWS Job Runner : November 2021                                            **\n" +
				"****************************************************************************************************************************\n" +
				"JR is an app for running bash scripts on AWS EC2 nodes. It downloads and uncompressed your resource bundle and looks for\n"+
				"xxx.sh_JR_START files in your S3 Jobs directories. For each, it copies over the directory contents, executes the\n"+
				"associated xxx.sh script, and transfers back the results.  This is repeated until no un run jobs are found. Launch many\n"+
				"EC2 JR nodes, each running the an instance of the JR, to process hundreds of jobs in parallel.\n"+
				
				"\nTo use:\n"+
				"1) Install and configure the aws cli on your local workstation, see https://aws.amazon.com/cli/\n"+
				"2) Upload your aws credentials file into a private bucket on aws, e.g.\n"+
				"     aws s3 cp ~/.aws/credentials s3://my-jr/aws.cred.txt\n"+
				"3) Generate a secure 24hr timed URL for the credentials file, e.g.\n"+
				"     aws --region us-west-2  s3 presign s3://my-jr/aws.cred.txt  --expires-in 259200\n"+
				"4) Upload a zip archive containing resources to run your jobs into S3, e.g.\n"+
				"     aws s3 cp ~/TNRunnerResourceBundle.zip s3://my-jr/TNRunnerResourceBundle.zip\n"+
				"     This will be copied into the /JRDir/ directory and then unzipped.\n"+
				"5) Upload script and job files into a 'Jobs' directory on S3, e.g.\n"+
				"     aws s3 cp ~/JRJobs/A/ s3://my-jr/Jobs/A/ --recursive\n"+
				"6) Optional, upload bash script files ending with JR_INIT.sh and or JR_TERM.sh. These are executed by JR before and after\n"+
				"     running the main bash script.  Use these to copy in sample specific resources, e.g. fastq/ cram/ bam files, and to run\n"+
				"     post job clean up.\n"+
				"7) Upload a file named XXX_JR_START to let the JobRunner know the bash script named XXX is ready to run, e.g.\n"+
				"     aws s3 cp s3://my-jr/emptyFile s3://my-jr/Jobs/A/dnaAlignQC.sh_JR_START\n"+
				"8) Launch the JobRunner.jar on one or more JR configured EC2 nodes. See https://ri-confluence.hci.utah.edu/x/gYCgBw\n"+

				"\nJob Runner Options:\n"+
				"-c URL to your secure timed config credentials file.\n"+
				"-r S3URI to your zipped resource bundle.\n"+
				"-j S3URI to your root Jobs directory containing folders with job scripts to execute.\n"+
				"-l S3URI to your Log folder for node logs.\n"+
				
				"\nDefault Options:\n"+
				"-d Directory on the local worker node, full path, in which resources and job files will be processed, defaults to /JRDir/\n"+
				"-t Terminate the EC2 node upon job completion or node error. Defaults to leaving the JobRunner looking for jobs for an hour.\n"+
				"-x Replace S3 job directories with processed analysis, defaults to syncing local with S3. WARNING, if selected, don't place\n"+
				"     any files in these S3 jobs directories that cannot be replaced. JR will delete them.\n"+
				"-v Verbose debugging output.\n"+

				"\nExample: java -jar -Xmx1G JobRunner.jar\n"+
				"     -r s3://my-jr/TNRunnerResourceBundle.zip\n"+
				"     -j s3://my-jr/Jobs/\n"+
				"     -l s3://my-jr/NodeLogs/\n"+
				"     -c 'https://my-jr.s3.us-west-2.amazonaws.com/aws.cred.txt?X-Amz-Algorithm=AWS4-HMXXX...'\n\n"+
		
				

				"****************************************************************************************************************************\n");

	}




}
