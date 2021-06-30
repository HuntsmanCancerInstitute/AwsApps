package edu.utah.hci.aws.apps.jobrunner;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.utah.hci.aws.util.Util;
import java.util.ArrayList;
import java.util.HashMap;


/**Looks for bash scripts in a particular s3 bucket, reanames each, downloads, and runs, and transfers back jobs results 
 * using AWS S3 and the AWS CLI. Before running a job, it copies over a 
 * zip archive and uncompresses it in a resource directory
 * 
 * 24hr url
 * aws --region us-west-2 s3 presign s3://hcibioinfo-jobrunner/Credentials/aws.cred.txt  --expires-in 86400
 * 
cd /Users/u0028003/Downloads/JobRunner
aws s3 rm s3://hcibioinfo-jobrunner/NodeLogs/ --recursive
aws s3 rm s3://hcibioinfo-jobrunner/Jobs/ --recursive
aws s3 cp /Users/u0028003/Downloads/JobRunner/testJob.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/testJob.sh
aws s3 cp /Users/u0028003/Downloads/JobRunner/testJob.sh s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/mockDoc.txt
aws s3 ls s3://hcibioinfo-jobrunner/ --recursive
 * 
 * while read line; do echo $line; done < toDownload.txt
 * 
 */
public class JobRunner {

	//user defined fields
	private File workDirectory = null;
	private String resourceS3Uri = null;
	private String jobsS3Uri = null;
	private String logsS3Uri = null;
	private String credentialsUrl = null;
	private boolean verbose = false;
	private String jobSuffix = ".sh";

	//internal fields
	private String numberProcessors = "NA";
	private String ram = "NA";
	private String awsPath = "aws";
	private StringBuilder hostLog = new StringBuilder();
	private File awsCredentialsDir = null;
	private HashMap<String, String> envPropToAdd = new HashMap <String, String>();
	private String hostName = null;
	private boolean resourceBundleDownloaded = false;
	private File credentialsFile = null;
	private StringBuilder jobSummaries = new StringBuilder();
	private long totalRunTime = 0;

	//possible files in the jobsS3Uri/
	/*
	scriptToRun.sh
	hostName_jobRunning_date_scriptToRun.sh
	hostName_jobError_date_scriptToRun.sh
	hostName_jobComplete_date_scriptToRun.sh

	//possible files in logsS3Uri/
	hostName_nodeInstatiated_date.txt
	hostName_nodeError_date.txt
	hostName_nodeComplete_date.txt
	 */
	private static final String nodeInstantiated = "_nodeInstantiated_";
	private static final String nodeError = "_nodeError_";
	private static final String nodeComplete = "_nodeComplete_";
	private static final String jobRunning = "_jobRunning_";
	private static final String jobError = "_jobError_";
	private static final String jobComplete = "_jobComplete_";
	private static final String jobLog = "_jobLog_";

	//working job info
	//  s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/JobC/testJob.sh
	private String originalJobScriptS3Uri = null;
	//  testJob.sh
	private String originalJobScriptFileName = null;
	//  s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/JobC/u00280003.lan_jobRunning_18June2021-7:30_testJob.sh
	private String runningJobScriptS3Uri = null;
	//  
	private File jobScript = null;
	//  s3://hcibioinfo-jobrunner/Jobs/Patient1/JobA/JobC/
	private String runningJobDirS3Uri = null;
	//  /Users/u0028003/Downloads/MockAWSJobRunnerWorkDir/Jobs/Patient1/JobA/JobC
	private File localJobDir = null;
	private File jobLogFile  = null;
	private PrintWriter jobLogOut = null;
	
	public JobRunner (String[] args){

		try {
			long startTimeTotal = System.currentTimeMillis();
			
			processArgs(args);

			loadCredentials();

			checkAwsCli();

			checkResourceBundle();

			writeInstantiationLog();

			//loop until no new jobs
			while (true) {
				long startTime = System.currentTimeMillis();

				//fetch next job
				if (fetchJob() == false) break;

				//check and if not present, download and unzip the data bundle, only do this if there is a job to run, will exit if a problem is found
				downloadResourceBundle();
				

				pl("\n----------------------- Launching a new job "+originalJobScriptS3Uri+" -----------------------");

				//delete, then make the working job dir
				makeJobDirectory();

				//sync S3Job dir with local Job directory
				syncS3JobDirWithLocal();

				//execute the job
				String message = executeJobScript();

				//sync local job dir with S3JobDir
				syncLocalJobDirWithS3JobDir();

				//cp jobLogFile to S3JobDir and delete
				uploadJobLogToS3JobDir();
				
				long diffTime = Math.round(((double)(System.currentTimeMillis() -startTime))/60000);
				
				jobSummaries.append(originalJobScriptS3Uri+"\t"+diffTime+"\t"+message+"\n");
				
				pl("\n----------------------- Job "+originalJobScriptS3Uri+" finished in "+diffTime+" min, status: "+message+"   -----------------------");
			}

			//shutdown node
			pl("\nNo more jobs...\nShutting down node...");
			totalRunTime = Math.round(((double)(System.currentTimeMillis() -startTimeTotal))/60000);
			writeSummaryStats();
			writeNodeComplete();
			shutDown(0);
			

		} catch (Exception e) {
			el("ERROR: "+e.getMessage());
			if (verbose) el(Util.getStackTrace(e));
			//attempt to restore the original job so another JobRunner can pick it up
			try {
				if (runningJobScriptS3Uri != null && originalJobScriptS3Uri != null) {
					el("Attempting to put job back in original state, "+runningJobScriptS3Uri+" -> "+ originalJobScriptS3Uri );
					moveS3File(runningJobScriptS3Uri, originalJobScriptS3Uri, true);
				}
			} catch (Exception e1) {e1.printStackTrace();}
			el("\nShutting down node...");
			try {
				writeNodeError(Util.getStackTrace(e));
			} catch (IOException e1) {e1.printStackTrace();}

			shutDown(1);

		}
	}

	private void writeSummaryStats() {
		pl("\nJob Summary Statistics:\n\nJob Name\tRun Time (min)\tExit Status");
		pl(jobSummaries.toString());
		
		pl("Total Run Time (min):\t"+totalRunTime);
		
	}

	private void uploadJobLogToS3JobDir() throws Exception {
		if (verbose) pl("\tUploading "+jobLogFile+" to "+runningJobDirS3Uri+" ...");
		String[] cmd = {awsPath, "s3", "cp", jobLogFile.getCanonicalPath(), runningJobDirS3Uri};
		int exitCode = executeReturnExitCode(cmd, false);
		if (exitCode == 0) {
			jobLogFile.delete();
			return;
		}
		//this should not fail unless a major problem occurs
		throw new IOException("\tFailed to upload "+jobLogFile+" to "+runningJobDirS3Uri);

	}

	private void syncLocalJobDirWithS3JobDir() throws Exception {
		if (verbose) pl("\tSyncing "+localJobDir+"/ with "+runningJobDirS3Uri+" ...");
		String[] cmd = {awsPath, "s3", "sync", localJobDir.getCanonicalPath(), runningJobDirS3Uri};
		int exitCode = executeReturnExitCode(cmd, false);
		if (exitCode == 0) return;
		//this should not fail unless a major problem occurs
		throw new IOException("\tFailed to sync "+localJobDir+" with "+runningJobDirS3Uri);
	}

	/*writeNodeLogCode 0=noWrite, 1=error, 2=complete*/
	private void shutDown(int exitCode) {

		//remove the credentials dir
		if (awsCredentialsDir != null) Util.deleteDirectory(awsCredentialsDir);

		//delete the working directory
		Util.deleteDirectory(workDirectory);

		System.exit(exitCode);
	}

	private void writeInstantiationLog() throws Exception {
		pl("\n"+hostName+" successfully instantiated...");
		String minSinceEpoc = Util.getMinutesSinceEpoch();
		
		File hostLogFile = new File (workDirectory, hostName+ nodeInstantiated +minSinceEpoc + ".txt");
		Util.write(hostLog.toString(), hostLogFile);
		String[] cmd = {awsPath, "s3", "cp", hostLogFile.getCanonicalPath(), logsS3Uri+ hostName+ nodeInstantiated+ minSinceEpoc+ ".txt"};
		if (executeReturnExitCode(cmd, false) == 0) return;
		//this should not fail unless a major problem is found
		throw new IOException("\tFailed to upload the node instantiation log "+hostLogFile.getCanonicalPath()+" to "+ logsS3Uri );
	}

	private void writeNodeError(String string) throws IOException {
		el(string);

		String minSinceEpoc = Util.getMinutesSinceEpoch();
		File hostLogFile = new File (workDirectory, hostName+ nodeError +minSinceEpoc+ ".txt");
		Util.write(hostLog.toString(), hostLogFile);

		String[] cmd = {awsPath, "s3", "cp", hostLogFile.getCanonicalPath(), logsS3Uri+ hostName+ nodeError+ minSinceEpoc+ ".txt"};
		String[] out = executeViaProcessBuilder(cmd, false);
		for (String s: out) {
			if (s.startsWith("upload:")) return;
		}
		//this should not fail unless a major problem is found
		throw new IOException("\tFailed to upload the node error log "+hostLogFile.getCanonicalPath()+" to "+ logsS3Uri);
	}

	private void writeNodeComplete() throws IOException {
		String minSinceEpoc = Util.getMinutesSinceEpoch();
		
		File hostLogFile = new File (workDirectory, hostName+ nodeComplete +minSinceEpoc+ ".txt");
		Util.write(hostLog.toString(), hostLogFile);

		String[] cmd = {awsPath, "s3", "cp", hostLogFile.getCanonicalPath(), logsS3Uri+ hostName+ nodeComplete+ minSinceEpoc+ ".txt"};
		String[] out = executeViaProcessBuilder(cmd, false);
		for (String s: out) {
			if (s.startsWith("upload:")) return;
		}
		//this should not fail unless a major problem is found
		throw new IOException("\tFailed to upload the node complete log "+hostLogFile.getCanonicalPath()+" to "+ logsS3Uri );
	}

	private String executeJobScript() throws Exception {
		//TODO covert to CloudWatch!

		String minSinceEpoc = Util.getMinutesSinceEpoch();
		jobLogFile = new File (workDirectory, hostName+ jobLog+ minSinceEpoc+"_"+ originalJobScriptFileName);
		jobLogFile.deleteOnExit();
		jobLogOut = new PrintWriter( new FileWriter (jobLogFile));

		if (verbose) pl("\tExecuting "+jobScript.getCanonicalPath());
		String[] cmd = {jobScript.getCanonicalPath()};
		int exitCode = executeReturnExitCode(cmd, true);

		//Success? rename running job file to complete
		String finalUri = null;
		String message = null;
		if (exitCode == 0) {
			finalUri = runningJobDirS3Uri+ hostName+ jobComplete+ minSinceEpoc+"_"+ originalJobScriptFileName;
			if (verbose) pl("\n\tOK job script exit code");
			message = "OK";
		}
		else {
			finalUri = runningJobDirS3Uri+ hostName+ jobError+ minSinceEpoc+"_"+ originalJobScriptFileName;
			if (verbose) pl("\n\tERROR job script exit code, check "+jobLogFile.getName()+" in "+runningJobDirS3Uri);
			message = "ERROR";
		}
		jobLogOut.close();
		jobLogOut = null;

		//this shouldn't error unless there's a major problem
		if (moveS3File(runningJobScriptS3Uri, finalUri, true) == false) throw new IOException("\nProblem renaming the job script "+runningJobScriptS3Uri+" to "+finalUri+"!") ;
		
		//delete the jobScript, this already exists in the runningJobDirS3Uri and will be renamed
		jobScript.delete();
		
		return message;
	}

	private void makeJobDirectory() throws IOException {
		if (localJobDir.exists()) Util.deleteDirectory(localJobDir);
		localJobDir.mkdirs();
		if (localJobDir.exists() == false) throw new IOException("ERROR: Failed to create new job directory "+localJobDir);
		envPropToAdd.put("JR_JOB_DIR", localJobDir.getCanonicalPath());
	}

	private boolean moveS3File(String startingS3Uri, String endingS3Uri, boolean printErrorMessage) throws Exception {
		if (startingS3Uri == null || endingS3Uri == null) return false;
		String[] cmd = {awsPath, "s3", "mv", startingS3Uri, endingS3Uri};
		if (executeReturnExitCode(cmd, false) == 0) return true;
		if (printErrorMessage || verbose) el("\tFailed to mv "+startingS3Uri+" to "+endingS3Uri);
		return false;
	}

	private void downloadResourceBundle() throws Exception {
		//already downloaded?
		if (resourceBundleDownloaded == true) return;

		pl("\nDownloading and uncompressing "+resourceS3Uri+ " into "+workDirectory+" ...");
		String[] cmd = {awsPath, "s3", "cp", resourceS3Uri, workDirectory.getCanonicalPath()};
		String[] out = executeViaProcessBuilder(cmd, false);
		//download: s3://hcibioinfo-jobrunner/TNRunnerResTest.zip to Downloads/MockAWSJobRunnerWorkDir/TNRunnerResTest.zip
		for (String s: out) {
			if (s.startsWith("download:")) {
				String[] fields = Util.FORWARD_SLASH.split(resourceS3Uri);
				String fileName = fields[fields.length-1];
				File zipBundle = new File (workDirectory, fileName);
				if (zipBundle.exists() == false) break;

				String workingDir = workDirectory.getCanonicalPath()+"/";
				cmd = new String[]{"unzip", "-oqd", workingDir, zipBundle.getCanonicalPath()};
				String[] unzipOut = executeViaProcessBuilder(cmd, true);
				if (unzipOut.length !=0) break;

				//remove the bundle to save space, these can be quite large
				zipBundle.delete();
				resourceBundleDownloaded = true;
				return;
			}
		}
		//failed! major issue
		//restore the original job so another JobRunner can pick it up
		moveS3File(runningJobScriptS3Uri, originalJobScriptS3Uri, true);
		throw new IOException("\n\tFailed to download or uncompress the resource zip archive "+resourceS3Uri+" "+Util.stringArrayToString(out, " "));
	}

	private void syncS3JobDirWithLocal() throws Exception {
		if (verbose) pl("\tSyncing "+runningJobDirS3Uri+" with "+localJobDir+"/ ...");
		String[] cmd = {awsPath, "s3", "sync", runningJobDirS3Uri, localJobDir.getCanonicalPath()};
		int exitCode = executeReturnExitCode(cmd, false);
		if (exitCode == 0 && jobScript.exists() && jobScript.setExecutable(true)) return;
		//this should not fail unless a major problem occurs
		throw new IOException("\tFailed to sync "+runningJobDirS3Uri+" to "+localJobDir+" or find/ make executable the script "+jobScript);
	}

	/**Returns the s3://hcibioinfo-jobrunner/JobsToRun/running_host_xxx Uri or null if no new jobs. */
	private boolean fetchJob() throws Exception {
		while (true) {
			//null the old ones
			runningJobScriptS3Uri = null;
			originalJobScriptS3Uri = null;

			//pull all of the job files: complete, other, running, error
			ArrayList<String>[] jobFiles = fetchJobScripts(jobsS3Uri);

			//any others left? these represent waiting jobs to run
			if (jobFiles[1].size() == 0) return false;
			else {
				startJob(jobFiles[1].get(0));
				if (runningJobScriptS3Uri != null) return true;
			}
		}
	}

	/* jobRelPathName = Jobs/Patient2/JobA/testJob.sh 
	 * jobsS3Uri = s3://hcibioinfo-jobrunner/Jobs 
	 * */
	private boolean startJob(String jobRelPathName) throws Exception {
		//null old
		runningJobScriptS3Uri = null;
		originalJobScriptS3Uri = null;
		originalJobScriptFileName = null;
		runningJobDirS3Uri = null;
		localJobDir = null;
		jobScript = null;

		//Jobs/Patient2/JobA/testJob.sh
		//Jobs/Patient1/JobA/JobC/testJob.sh 
		//Jobs/testJob.sh
		String[] splitRelPath = Util.FORWARD_SLASH.split(jobRelPathName);

		//make new script name
		String scriptName = splitRelPath[splitRelPath.length-1];
		scriptName = hostName+ jobRunning+ Util.getMinutesSinceEpoch()+ "_"+ scriptName;

		//make new runningJobS3Uri
		StringBuilder sb = new StringBuilder();
		//skip first and last
		int last = splitRelPath.length-1;
		for (int i=1; i<last; i++){
			sb.append(splitRelPath[i]);
			sb.append("/");
		}

		runningJobScriptS3Uri = jobsS3Uri+ sb + scriptName;
		originalJobScriptFileName = splitRelPath[splitRelPath.length-1];
		originalJobScriptS3Uri = jobsS3Uri+ sb + originalJobScriptFileName;
		
		runningJobDirS3Uri = jobsS3Uri+ sb;
		localJobDir = new File(workDirectory, splitRelPath[0]+"/"+sb.toString());
		jobScript = new File (localJobDir, scriptName);
		jobScript.deleteOnExit();
		
		
		if (verbose) {
			pl("\nJobParams:");
			pl("\trunningJobScriptS3Uri\t"+runningJobScriptS3Uri);
			pl("\toriginalJobScriptS3Uri\t"+originalJobScriptS3Uri);
			pl("\toriginalJobScriptFileName\t"+originalJobScriptFileName);
			pl("\trunningJobDirS3Uri\t"+runningJobDirS3Uri);
			pl("\tlocalJobDir\t"+localJobDir);
			pl("\tjobScript\t"+jobScript);
		}

		//rename the script, if not successful then another worker node has likely grabbed it
		if (moveS3File(originalJobScriptS3Uri, runningJobScriptS3Uri, false)) return true;

		//failed! this is prob OK, another JobRunner might have picked it up
		pl("\tFailed to start "+originalJobScriptS3Uri+" looking for another job");

		runningJobScriptS3Uri = null;
		originalJobScriptS3Uri = null;
		originalJobScriptFileName = null;
		runningJobDirS3Uri = null;
		localJobDir = null;
		jobScript = null;

		return false;
	}

	private ArrayList<String>[] fetchJobScripts(String s3Uri) throws IOException{
		String[] cmd = {awsPath, "s3", "ls", jobsS3Uri, "--recursive"};
		String[] out = executeViaProcessBuilder(cmd, false);
		ArrayList<String> runningAL = new ArrayList<String>();
		ArrayList<String> completeAL = new ArrayList<String>();
		ArrayList<String> otherAL = new ArrayList<String>();
		ArrayList<String> errorAL = new ArrayList<String>();
		for (String s: out) {
			s= s.trim();
			//empty
			if (s.length() == 0) continue;
			//folder?
			if (s.startsWith("PRE ")) continue;
			String[] fields = Util.WHITESPACE.split(s);
			//empty file?
			String fileName = fields[fields.length-1];
			if (fileName.equals("0") || fileName.endsWith(jobSuffix) == false || fileName.contains(jobLog)) continue;
			else if (fileName.contains(jobComplete)) completeAL.add(fileName);
			else if (fileName.contains(jobRunning)) runningAL.add(fileName);
			else if (fileName.contains(jobError)) errorAL.add(fileName);
			else  otherAL.add(fileName);
		}
		//otherAL
		//Jobs/Patient1/JobA/JobC/testJob.sh, 
		//Jobs/Patient1/JobA/testJob.sh, 
		//Jobs/Patient1/JobB/testJob.sh, 
		//Jobs/Patient2/JobA/testJob.sh, 
		//Jobs/testJob.sh]

		return new ArrayList[] {completeAL, otherAL, runningAL, errorAL};
	}

	/**Uses ProcessBuilder to execute a cmd, combines standard error and standard out into one and returns their output.
	 * @throws IOException */
	public String[] executeViaProcessBuilder(String[] command, boolean printToLog) throws IOException{
		if (verbose) pl ("Executing: '"+Util.stringArrayToString(command, " ")+"'");
		ArrayList<String> al = new ArrayList<String>();
		ProcessBuilder pb = new ProcessBuilder(command);
		//add enviro props?
		pb.environment().putAll(envPropToAdd);
		
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
	public int executeReturnExitCode(String[] command, boolean printToLog) throws Exception{
		if (verbose) pl ("Executing: "+Util.stringArrayToString(command, " "));
		ProcessBuilder pb = new ProcessBuilder(command);
		//add enviro props?
		pb.environment().putAll(envPropToAdd);
		pb.redirectErrorStream(true);
		Process proc = pb.start();
		BufferedReader data = new BufferedReader(new InputStreamReader(proc.getInputStream()));
		String line;
		while ((line = data.readLine()) != null) if (printToLog || verbose) pl(line);
		return proc.waitFor();
	}

	void pl(String s) {
		System.out.println(s);
		hostLog.append(s);
		hostLog.append("\n");
		System.out.flush();
		if (jobLogOut != null) {
			jobLogOut.println(s);
			jobLogOut.flush();
		}
	}
	void p(String s) {
		System.out.print(s);
		hostLog.append(s);
		System.out.flush();
		if (jobLogOut != null) {
			jobLogOut.print(s);
			jobLogOut.flush();
		}
	}
	void el(String s) {
		System.err.println(s);
		hostLog.append(s);
		hostLog.append("\n");
		System.err.flush();
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
		String[] out = executeViaProcessBuilder(cmd, false);

		if (out[0].contains("aws-cli") == false) {
			for (String s: out) el(s);
			throw new IOException("Error: 'aws --version' failed to return an appropriate response.");
		}
		else pl("  "+out[0]);
	}

	private void checkResourceBundle() throws IOException {
		pl("\nChecking resource bundle...");

		String[] cmd = {awsPath, "s3", "ls", resourceS3Uri};
		String[] out = executeViaProcessBuilder(cmd, false);

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

		//it's important to not place this in the work directory since this is often a persistent mount
		awsCredentialsDir = new File (System.getProperty("user.home")+"/.tempAwsDeleteMe");
		awsCredentialsDir.mkdirs();
		if (awsCredentialsDir.exists() == false) throw new IOException("Error: failed to make "+awsCredentialsDir+" for saving the aws credentials file");
		credentialsFile = new File (awsCredentialsDir, "credentials").getCanonicalFile();
		credentialsFile.deleteOnExit();

		//fetch the file with curl, could use wget but curl is installed by default on macos
		//String[] cmd = {"wget", credentialsUrl, "-O", credentials.toString()};
		String[] cmd = {"curl", credentialsUrl, "-o", credentialsFile.toString(), "-s", "-S"};
		String[] out = executeViaProcessBuilder(cmd, false);
		if (credentialsFile.exists() == false) {
			for (String s: out) el(s);
			throw new IOException("Error: failed to download the aws credentials from -> "+credentialsUrl);
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

		//add workingDir
		envPropToAdd.put("JR_WORKING_DIR", workDirectory.getCanonicalPath());

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
					case 't': workDirectory = new File(args[++i]); break;
					case 'r': resourceS3Uri = args[++i]; break;
					case 'c': credentialsUrl = args[++i]; break;
					case 'j': jobsS3Uri = args[++i]; break;
					case 'l': logsS3Uri = args[++i]; break;
					case 'h': printDocs(); System.exit(0);
					default: Util.printExit("\nProblem, unknown option! " + mat.group());
					}
				}
				catch (Exception e){
					Util.printExit("\nSorry, something doesn't look right with this parameter: -"+test+"\n");
				}
			}
		}

		//set node info
		loadHostInfo();

		printParams();

		// required args?
		if (workDirectory == null || resourceS3Uri == null || credentialsUrl == null || jobsS3Uri == null || logsS3Uri == null) {
			throw new IOException("Error: failed to find one or more of the required arguments, see Parameters above.");
		}

		//fix jobsS3Uri and logsS3Uri
		if (jobsS3Uri.endsWith("/") == false) jobsS3Uri = jobsS3Uri+"/";
		if (logsS3Uri.endsWith("/") == false) logsS3Uri = logsS3Uri+"/";

		//work directory
		Util.deleteDirectory(workDirectory);
		workDirectory.mkdirs();
		if (workDirectory.exists() == false || workDirectory.canWrite() == false) {
			throw new IOException("Error: failed to find a writable work directory -> "+ workDirectory);
		}
		executeViaProcessBuilder(new String[]{"rm","-rf",workDirectory.getCanonicalPath()+"/*"}, false);

		//check S3Uri
		if (resourceS3Uri.startsWith("s3://") == false || jobsS3Uri.startsWith("s3://") == false) {
			throw new IOException("Error: S3URIs must start with s3:// "+ workDirectory);
		}
		if (resourceS3Uri.endsWith(".zip") == false) throw new IOException("Error: the zip resource S3Uri must end with xxx.zip, see "+resourceS3Uri);	

	}	

	/*Attempt to get machine info, doesn't work on a mac*/
	private void loadHostInfo() throws UnknownHostException {
		hostName = InetAddress.getLocalHost().getHostName();
		String[] out;
		try {
			out = executeViaProcessBuilder(new String[]{"nproc"}, false);
			if (out!= null && out.length == 1) numberProcessors = out[0];
			out = Util.executeShellScript("expr `free -g | grep -oP '\\d+' | head -n 1` - 2", workDirectory);
			if (out!= null && out.length == 1) ram = out[0];
		} catch (IOException e) {}

	}

	private void printParams() {
		pl("Required Parameters:");
		String[] cred = Util.QUESTION.split(credentialsUrl);
		pl("  -c Credentials URL     : "+ cred[0]);
		pl("  -r Zip resource S3 URI : "+ resourceS3Uri);
		pl("  -j Jobs S3 URI         : "+ jobsS3Uri);
		pl("  -l Node Logs S3 URI    : "+ logsS3Uri);
		pl("  -t Temporary work dir  : "+ workDirectory);

		pl("\nJob Runner Info:");
		pl("  Host name              : "+ hostName);
		pl("  Number processors      : "+ numberProcessors);
		pl("  GB RAM                 : "+ ram);
	}

	public static void printDocs(){
		System.out.println("\n" +
				"**************************************************************************************\n" +
				"**                                  AWS Job Runner : June 2021                      **\n" +
				"**************************************************************************************\n" +
				"Install and place in path curl, aws cli, unzip, and everything needed to run your job\n" +
				"scripts.\n"+
				
				"JR_WORKING_DIR and JR_JOB_DIR \n" +

				"\nRequired:\n"+
				"-c URL to your config credentials file. Use the aws cli to generate a secure timed URL,\n"+
				"     e.g. aws s3 presign s3://hcibioinfo-jobrunner/aws.cred.txt  --expires-in 86400\n"+
				"     Example config file (no leading spaces, email info optional):\n"+
				"       [default]\n"+
				"       region = us-west-2\n"+
				"       aws_access_key_id = AKICVRTUUZZSDRCXWWJK6A\n"+
				"       aws_secret_access_key = BgDV2UHZvT5adfWRy226GV65HZMpUQ\n"+
				"-t Temporary directory on the worker node, full path, in which resources and job files will be processed.\n"+
				"-r Resource bundle, zip compressed, S3URI. It will be copied into the work directory\n"+
				"     and unzipped.\n"+
				"-s File name suffix for job scripts to execute, defaults to '.sh'\n"+
				"-j Root job folder on S3 containing job scripts to execute. These will be renamed \n"+
				"     running_xxx in S3, copied into the job directory and executed. If a file named\n"+
				"     COMPLETE is found in the job directory upon job completion, the _running_ S3\n"+
				"     object will be renamed _complete_, otherwise it is renamed _error_. Recursive. \n"+
				"-l Log folder on S3 to write node logs.\n"+

				"\nExample: java -jar pathTo/JobRunner.jar\n"+
				"-r s3://hcibioinfo-jobrunner/TNRunnerResourceBundle.zip\n"+
				"-j s3://hcibioinfo-jobrunner/Jobs/\n"+
				"-l s3://hcibioinfo-jobrunner/NodeLogs/\n"+
				"-t /mnt/TempWorkDir/\n"+
				"-c 'https://hcibioinfo-jobrunner.s3.us-west-2.amazonaws.com/aws.cred.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIARHBYAZBR33RCJK6A%2F20210608%2Fus-west-2%2Fs3%2Faws4_request&X-Amz-Date=20210608T141053Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=8cc26200af48a0c218c0f1db128ae0649eb2d6db5fa7f99ec6c243df19afdf90'\n\n"+

				"**************************************************************************************\n");

	}




}
