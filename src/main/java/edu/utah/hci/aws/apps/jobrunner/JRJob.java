package edu.utah.hci.aws.apps.jobrunner;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class JRJob {
	
	//fields
	
	// s3://hcibioinfo-jobrunner/Jobs/Patient1/JobB/
	private String s3UrlRunningJobPath = null;
	
	// testJob.sh
	private String scriptFileName = null;
	
	// testJob.sh_JR_START
	private String startScriptFileName = null;
	
	// testJob.sh_JR_REQUESTED_u00280003.local
	private String requestFileName = null;
	
	// testJob.sh_JR_RUNNING_u00280003.local
	private String runningFileName = null;

	// workingDir/RunningJob/Patient1/JobB/
	private File localJobDir;	
	
	// workingDir/RunningJob/Patient1/JobB/testJob.sh_JR_LOG.txt
	private File jobLogFile;
	private PrintWriter jobLogOut = null;
	
	//  completed without issue?
	private boolean completedOK;

	//constructor
	public JRJob(String s3UrlRunningJobPath, String jobScript, String jobStartScript) {
		this.s3UrlRunningJobPath = s3UrlRunningJobPath;
		this.scriptFileName = jobScript;
		this.startScriptFileName = jobStartScript;
	}

	public String getS3UrlRunningJobPath() {
		return s3UrlRunningJobPath;
	}

	public String getScriptFileName() {
		return scriptFileName;
	}

	public void setJobRequestFileName(String requestFileName) {
		this.requestFileName = requestFileName;
	}

	public void setJobRunningFileName(String jobRunningFileName) {
		runningFileName = jobRunningFileName;
	}

	public String getRequestFileName() {
		return requestFileName;
	}

	public String getRunningFileName() {
		return runningFileName;
	}

	public void setLocalJobDir(File localJobDir) {
		this.localJobDir= localJobDir;
	}

	public File getLocalJobDir() {
		return localJobDir;
	}

	public void setJobLogFile(File jobLogFile) throws IOException {
		this.jobLogFile = jobLogFile;
		jobLogOut = new PrintWriter( new FileWriter (jobLogFile));
	}

	public File getJobLogFile() {
		return jobLogFile;
	}

	public PrintWriter getJobLogOut() {
		return jobLogOut;
	}

	public String getStartScriptFileName() {
		return startScriptFileName;
	}

	public boolean isCompletedOK() {
		return completedOK;
	}

	public void setCompletedOK(boolean completedOK) {
		this.completedOK = completedOK;
	}
}
