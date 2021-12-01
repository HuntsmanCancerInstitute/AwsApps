package edu.utah.hci.aws.apps.jobrunner;

import java.io.File;

public class Job {
	String s3UrlShellScript;
	String s3UrlInitScript;
	String s3UrlTermScript;
	String s3UrlRequest;
	String s3UrlRunning;
	String s3UrlErrorLog;
	String s3UrlCompleteLog;
	File localLog;
	
}
