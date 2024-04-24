package edu.utah.hci.aws.apps.copy;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.utah.hci.aws.util.Util;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tier;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

/**
 * Copies archived and non archived S3 objects to new S3 URIs.  Can run as a daemon checking every hour to look for restored files it can then copy over. Recursive or single files.
 * Three types of coping:
 * 		1) Object to Object s3://bucket/folder/object.txt > s3://bucket2/folder/subfolder/object2.txt
 * 		2) Recursive both ending s3://bucket/folder/ > s3://bucket2/folder/subfolder/
 *      3) Recursive with prefix s3://bucket/folder/obj > s3://bucket2/folder/subfolder/
 *      	Both recursive destinations MUST end with /
 *      4) Ditto for local downloads, does interp . ./ ~/
 * 
 * 	2) Create this file ~/.aws/credentials with your access, secret, and region info, chmod 600 the file and keep it safe.
	 	[default]
		aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A
		aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ
		region = us-west-2
		
	Currently not supported is copying between accounts, see https://github.com/aws/aws-sdk-java/issues/2431 to start, permissions headache!
 */
public class S3Copy {

	//user defined fields
	private String jobString = null;
	private String profile = "default";
	private int numberDaysToRestore = 1;
	private boolean dryRun = false;
	private String email = null;
	private int maxThreads = 8;
	private boolean recursiveCopy = false;
	private Tier restoreTier = Tier.Standard;
	private boolean hardExit = true; //for testing

	//for looping till complete
	private boolean rerunUntilComplete = false;
	private int numMinToSleep = 60;
	private int iterations = 100;

	//internal fields
	private CopyRequest[] copyRequests = null; 
	private StringBuilder log = new StringBuilder();
	private String region = null;
	private boolean resultsCheckOK = true;
	private int maxTries = 5;
	private int secToWait = 15;
	private boolean printStackTrace = true;
	private HashMap<String, AmazonS3> s3 = new HashMap<String, AmazonS3>();
	private long numCopyJobsComplete = 0;
	private long numCopyJobsToCopy = 0;
	private long sizeComplete = 0;
	private long sizeToCopy = 0;
	private ArrayList<CopyJob> copyJobsToProc = new ArrayList<CopyJob>();
	private int indexCopyJobsToProc = -1;
	private ProfileCredentialsProvider credentials;
	private File tmpEmailLog = null;

	public S3Copy (String[] args){
		try {
			long startTime = System.currentTimeMillis();

			processArgs(args);

			//Parse the user supplied copy jobs and look for errors
			if (parseUserCopyRequests() == false) {
				printStackTrace = false;
				throw new Exception("\nFix errors and restart.");
			}
			
			//check access and fetch the region for each bucket
			if (checkAccessFetchRegions() == false) {
				printStackTrace = false;
				throw new Exception("\nFix permission? errors and restart.");
			}

			//TODO: Should thread this
			//Make the individual copy jobs, check status, and look for errors
			if (makeCopyJobs() == false) {
				printStackTrace = false;
				throw new Exception("\nFix errors and restart.");
			}
			checkCopyJobs();
			pl("\n"+numCopyJobsToCopy+" Files to copy ("+Util.formatSize(sizeToCopy)+"), "+numCopyJobsComplete+" Files copied/ exist ("+Util.formatSize(sizeComplete)+")");
			
			//dry run?
			if (dryRun) pl("\nDry run, exiting.");
			
			//anything to copy?
			else if (numCopyJobsToCopy != 0) doWork();

			shutdown();
			double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
			pl("\nDone! "+Math.round(diffTime)+" minutes\n");
			sendEmail();
			if (hardExit) System.exit(0); //Really shouldn't have to do this! The app hangs without it.
			
		} catch (Exception e) {
			resultsCheckOK = false;
			shutdown();
			el(e.getMessage());
			if (printStackTrace) el(Util.getStackTrace(e));
			sendEmail();
			System.exit(1);
		}
	}
	
	private boolean checkAccessFetchRegions() throws Exception {
		pl("\nFetching bucket regions...");

		HashMap<String, String> bucketNameRegion = fetchBucketRegions();
		if (bucketNameRegion == null) return false;
		
		//add em to each CopyRequest
		for (CopyRequest cr: copyRequests) {
			String sourceBucketName = cr.getSourceBucketKey()[0];
			cr.setSourceRegion(bucketNameRegion.get(sourceBucketName));
			//any s3 destination?
			if (cr.getDestinationBucketKey()!=null) {
				String destinationBucketName = cr.getDestinationBucketKey()[0];
				cr.setDestinationRegion(bucketNameRegion.get(destinationBucketName));
			}
		}
		return true;
		
	}

	private HashMap<String, String> fetchBucketRegions() {
		
		//pull all the bucket names
		HashSet<String> bucketNames = new HashSet<String>();
		for (CopyRequest cr: copyRequests) {
			bucketNames.add(cr.getSourceBucketKey()[0]);
			if (cr.getDestinationBucketKey()!=null) bucketNames.add(cr.getDestinationBucketKey()[0]);
		}
		
		//for each one
		HashMap<String, String> bucketNameRegion = new HashMap<String, String>();
		AmazonS3 userS3 = fetchS3Client(region);
		ArrayList<String> errorMessages = new ArrayList<String>();
		for (String bn: bucketNames) {
			try {
				String bucketRegion = Util.fetchBucketRegion(userS3, bn);
				 bucketNameRegion.put(bn, bucketRegion);
				//if (regionAccess[1].equals("OK")) bucketNameRegion.put(bn, regionAccess[0]);
				//else errorMessages.add("Cannot access bucket: "+bn);
			} catch (Exception e) {
				errorMessages.add(e.getMessage());
			}
		}
		//any errors?
		if (errorMessages.size()!=0) {
			el("\t"+Util.arrayListToString(errorMessages, "\n\t"));
			return null;
		}
		return bucketNameRegion;
	}

	public S3Copy() {}

	private boolean makeCopyJobs() throws Exception {
		pl("\nMaking individual CopyJobs, checking status...");
		
		//for each user copy request
		boolean ok = true;
		for (CopyRequest cr: copyRequests) {
			pl(cr.toString());
			cr.makeCopyJobs();
			//any errors?
			if (cr.getErrorMessage().size()!=0 || cr.isCopyJobError()==true) {
				ok = false;
				for (String e: cr.getErrorMessage()) el(e);
			}
		}
		return ok;
	}
	
	/**Close down the AmazonS3 clients and any associated TransferManagers.*/
	private void shutdown() {
		for (AmazonS3 s : s3.values()) s.shutdown();
	}
	
	private AmazonS3 fetchS3Client(String region) {
		AmazonS3 s3Client = s3.get(region);
		if (s3Client == null) {
			s3Client = AmazonS3ClientBuilder.standard().withCredentials(credentials).withRegion(region).build();
			s3.put(region, s3Client);
		}
		return s3Client;
	}
	

	private void doWork() throws Exception {
		
		//loop till done attempting restores and copies on individual jobs 
		while (iterations-- > 0) {
			pl("\nProcessing CopyJobs...");
			
			//make workers
			int numWorkers = (int)numCopyJobsToCopy;
			if (numWorkers > maxThreads) numWorkers = maxThreads;
			CopyJobWorker[] workers = new CopyJobWorker[numWorkers];
			for (int i=0; i< numWorkers; i++) {
				workers[i] = new CopyJobWorker(this, "Worker_"+i);
			}
			
			ExecutorService executor = Executors.newFixedThreadPool(numWorkers);
			for (CopyJobWorker l: workers) executor.execute(l);
			executor.shutdown();
			while (!executor.isTerminated()) {}  //wait here until complete

			//check loaders 
			for (CopyJobWorker l: workers) {
				if (l.isFailed()) throw new IOException("ERROR: CopyJobWorker failed "+l.getName());
			}
		
			//check CopyJobs, this resets the counters, might all be done if no restores were required
			checkCopyJobs();
			if (numCopyJobsToCopy != 0) pl(numCopyJobsToCopy+" File(s) to copy ("+Util.formatSize(sizeToCopy)+"), "+numCopyJobsComplete+" File(s) copied/exist ("+Util.formatSize(sizeComplete)+")");
			else {
				pl("\n"+numCopyJobsComplete+" File(s) successfully copied ("+Util.formatSize(sizeComplete)+")");
				return; 
			}
			
			pl("Waiting "+numMinToSleep+" min");
			Thread.sleep(1000*60*numMinToSleep);
		}
	}
	
	public synchronized CopyJob fetchNextCopyJob() {
		if (indexCopyJobsToProc < copyJobsToProc.size()) {
			return copyJobsToProc.get(indexCopyJobsToProc++);
		}
		return null;
	}

	private void checkCopyJobs() {

		//reset counters
		numCopyJobsComplete = 0;
		numCopyJobsToCopy = 0;
		sizeComplete = 0;
		sizeToCopy = 0;
		copyJobsToProc.clear();
		indexCopyJobsToProc = 0;

		for (CopyRequest cr: copyRequests) {
			//numComplete, numToCopy, sizeComplete, sizeToCopy
			long[] numCompleteToCopy = cr.fetchCopyJobNumbers();
			this.numCopyJobsComplete += numCompleteToCopy[0];
			this.numCopyJobsToCopy += numCompleteToCopy[1];
			this.sizeComplete += numCompleteToCopy[2];
			this.sizeToCopy += numCompleteToCopy[3];
			if (cr.isComplete() == false) cr.addIncompleteCopyJobs(copyJobsToProc);
		}

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


	private void sleep(String message) {
		try {
			pl(message+", sleeping "+secToWait+" seconds");
			TimeUnit.SECONDS.sleep(secToWait);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	//for diff region copying see https://stackoverflow.com/questions/59980898/how-to-copy-files-from-s3-bucket-from-one-region-to-another-region-using-aws-jav


	void sendEmail() {
		if (email == null) return;
		try {
			//sendmail 'david.austin.nix@gmail.com,david.nix@hci.utah.edu' < sm.txt
			//write out the message
			String status = " - COMPLETE - ";
			if (resultsCheckOK == false) status = " - ERROR - ";
			log.append("\n");
			String message = "Subject: S3Copy" +status+ Util.getDateTime()+"\nFrom: noreply_s3copy@hci.utah.edu\n"+log.toString();
			File tmpDir = new File(System.getProperty("java.io.tmpdir"));
			if (tmpDir.exists()==false) throw new Exception("ERROR: failed to find tmp dir. "+tmpDir);
			tmpEmailLog = new File(tmpDir,"S3Copy."+ Util.getRandomString(4)+ ".tmp.txt");
			tmpEmailLog.deleteOnExit();
			Util.write(message, tmpEmailLog);
			
			//execute via a shell script
			String cmd = "sendmail '"+email+"' < "+tmpEmailLog.getCanonicalPath();
			int exit = Util.executeShellScriptReturnExitCode(cmd, tmpDir);
			Util.pl("Sent email: "+cmd);
			//this never happens?
			if (exit != 0) throw new IOException ("\nERROR sending email with "+cmd);
		} catch (Exception e) {
			el("\nError sending email");
			el(Util.getStackTrace(e));
			System.exit(1);
		}
	}

	public synchronized void pl(String s) {
		System.out.println(s);
		log.append(s);
		log.append("\n");
	}
	public synchronized void p(String s) {
		System.out.print(s);
		log.append(s);
	}
	public synchronized void el(String s) {
		System.err.println(s);
		log.append(s);
	}

	public ArrayList<S3ObjectSummary> fetchS3Objects(String bucketName, String prefix, String region, boolean exactKeyMatch) throws IOException {
		ArrayList<S3ObjectSummary> toReturn = new ArrayList<S3ObjectSummary>();

		try {
			//whole bucket?
			if (prefix == null || prefix.length()==0) {
				S3Objects.inBucket(fetchS3Client(region), bucketName).forEach((S3ObjectSummary os) -> {
					toReturn.add(os);
				});
			}

			//with prefix
			else {
				S3Objects.withPrefix(fetchS3Client(region), bucketName, prefix).forEach((S3ObjectSummary os) -> {
					if (exactKeyMatch) {
						if (os.getKey().equals(prefix)) toReturn.add(os);
					}
					else toReturn.add(os);
				});
			}
		} catch (AmazonS3Exception ae) {
			//access denied?
			if (ae.getStatusCode() == 403) return null;
			else throw new IOException (ae.getErrorMessage());
		}

		return toReturn;
	}
	

	

	public static void main(String[] args) {
		if (args.length ==0){
			new S3Copy().printDocs();
			System.exit(0);
		}
		new S3Copy(args);
	}	

	/**This method will process each argument and assign new variables*/
	public void processArgs(String[] args) {
		try {
			Pattern pat = Pattern.compile("-[a-z]");
			pl("S3Copy Arguments: "+Util.stringArrayToString(args, " ")+"\n");

			for (int i = 0; i<args.length; i++){
				String lcArg = args[i].toLowerCase();
				Matcher mat = pat.matcher(lcArg);
				if (mat.matches()){
					char test = args[i].charAt(1);
					try{
						switch (test){
						case 'j': jobString = args[++i]; break;
						case 'e': email = args[++i]; break;
						case 'n': numberDaysToRestore = Integer.parseInt(args[++i]); break;
						case 'd': dryRun = true; break;
						case 'r': recursiveCopy = true; break;
						case 'x': restoreTier = Tier.Expedited; numMinToSleep = 1; break;
						case 'l': rerunUntilComplete = true; break;
						case 't': maxThreads = Integer.parseInt(args[++i]); break;
						case 'p': profile = args[++i]; break;
						case 's': hardExit = false; break;
						case 'h': printDocs(); System.exit(0);
						case 'a': printDiffAccountInstructions(); 
						default: Util.printExit("\nProblem, unknown option! " + mat.group());
						}
					}
					catch (Exception e){
						Util.printExit("\nSorry, something doesn't look right with this parameter: -"+test+"\n");
					}
				}
			}
			
			//set number of threads
			int availThreads = Runtime.getRuntime().availableProcessors()-1;
			if (maxThreads > availThreads) maxThreads = availThreads;

			printOptions();

			if (jobString == null) Util.printErrAndExit("\nERROR: please provide a file path or string with copy job information, see the help menu.\n");

			region = Util.getRegionFromCredentials(profile);		
			if (region == null) Util.printErrAndExit("\nERROR: failed to find your profile and or region in ~/.aws/credentials, "+profile);
			credentials = new ProfileCredentialsProvider(profile);

		} catch (Exception e) {
			el("\nError processing arguments");
			el(Util.getStackTrace(e));
			System.exit(1);
		}
	}	

	private boolean parseUserCopyRequests() throws IOException {
		pl("\nParsing CopyRequests...");
		String[] lines = null;

		//is it a file
		File jobs = new File (jobString);
		if (jobs.exists()) lines = Util.loadTxtFile(jobs);

		//must be a cmdLine
		else lines = Util.COMMA.split(jobString);

		copyRequests = new CopyRequest[lines.length]; 
		boolean allOK = true;
		for (int i=0; i< copyRequests.length; i++) {
			copyRequests[i] = new CopyRequest(lines[i], this);
			if (copyRequests[i].isPassing()) pl(copyRequests[i]+"\tOK");
			else {
				pl(Util.arrayListToString(copyRequests[i].getErrorMessage(), "\n\t"));
				allOK = false;
			}
		}
		return allOK;
	}

	private void printOptions() {
		pl("Options:");
		pl("  -j Copy job input                : "+ jobString);
		pl("  -d Dry run                       : "+ dryRun);
		pl("  -e Email                         : "+ email);
		pl("  -l Rerun until complete          : "+ rerunUntilComplete);
		pl("  -r Recursive copy                : "+ recursiveCopy);
		pl("  -x Archive restore tier          : "+ restoreTier.toString());
		pl("  -n # days to keep restored files : "+ numberDaysToRestore);
		pl("  -t Max number threads            : "+ maxThreads);
		pl("  -p Credentials profile           : "+ profile);
	}
	
	public static void printDiffAccountInstructions() {
		String inst = 
				"To copy files between accounts:\n"+
				"\n"+
				"1) Fetch the source user account ARN, e.g. 'arn:aws:iam::xxx:user/xxxx' \n"+
				"	Log into the source account in the AWS Console\n"+
				"	Search for 'IAM' in the search bar at the top of the AWS Console landing page\n"+
				"	Navigate to the IAM page, select 'Users', select the user name that has access to the source bucket and files\n"+
				"	Copy and save the \"User ARN\" by clicking the copy symbol to the right of the ARN under the 'Summary' section\n"+
				"	\n"+
				"2) In the destination account, create a new temporary staging bucket in the preferred region. \n"+
				"	Log into the destination account in the AWS Console\n"+
				"	Search for 'S3' in the search bar at the top of the AWS Console landing page\n"+
				"	Navigate to the S3 page, select 'Buckets'\n"+
				"	Select the big orange 'Create bucket'\n"+
				"	Enter a name like 'temp-s3transfer-delete-me'\n"+
				"	Select the region where your primary data files are kept\n"+
				"	Create the bucket and save its name\n"+
				"\n"+
				"3) Use a txt editor to copy paste in the json policy below, replace the source 'arn:aws:iam::xxx:user/xxxx' and destination bucket name 'temp-s3transfer-delete-me' with your info. Be careful that tabs are not introduced while modifying. This policy will allow the named user full S3 access to the bucket and the files within.\n"+
				"{\n"+
				"    \"Version\": \"2012-10-17\",\n"+
				"    \"Statement\": [\n"+
				"        {\n"+
				"            \"Sid\": \"S3CopyAllS3Access\",\n"+
				"            \"Effect\": \"Allow\",\n"+
				"            \"Principal\": {\n"+
				"                \"AWS\": [\n"+
				"                    \"arn:aws:iam::xxx:user/xxxx\"\n"+
				"                ]\n"+
				"            },\n"+
				"            \"Action\": \"s3:*\",\n"+
				"            \"Resource\": [\n"+
				"                \"arn:aws:s3:::temp-s3transfer-delete-me\",\n"+
				"                \"arn:aws:s3:::temp-s3transfer-delete-me/*\"\n"+
				"            ]\n"+
				"        }\n"+
				"    ]\n"+
				"}\n"+
				"\n"+
				"4) Add this policy to the temporary staging bucket to enable access to the source user.\n"+
				"	Log into the destination account in the AWS Console, navigate to the S3 page, select 'Buckets', then select the temporary bucket.\n"+
				"	Under the 'Permissions' tab, edit the 'Bucket policy' replacing it with the above.\n"+
				"\n"+
				"5) Run S3Copy to copy your files into the destination temporary bucket.  Then have the destination owner move the files into a permanent bucket. Lastly, delete the temporary bucket.\n";
		Util.pl(inst);
		System.exit(0);
	}

	public void printDocs(){
		pl("\n" +
				"**************************************************************************************\n" +
				"**                                  S3 Copy : April 2024                            **\n" +
				"**************************************************************************************\n" +
				"SC copies AWS S3 objects, unarchiving them as needed, within the same or different\n"+
				"accounts or downloads them to your local computer. Run this as a daemon with -l or run\n"+
				"repeatedly until complete. To upload files to S3, use the AWS CLI. \n"+

				"\nTo use the app:\n"+ 
				"Create a ~/.aws/credentials file with your access, secret, and region info, chmod\n"+
				"  600 the file and keep it private. Use a txt editor or the AWS CLI configure\n"+
				"  command, see https://aws.amazon.com/cli   Example ~/.aws/credentials file:\n"+
				"      [default]\n"+
				"      aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A\n"+
				"      aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ\n"+
				"      region = us-west-2\n"+
				"Repeat these entries for multiple accounts replacing the word 'default' with a single\n"+
				"unique account name.\n"+

				"\nRequired:\n"+
				"-j Provide a comma delimited string of copy jobs or a txt file with one per line.\n"+
				"      A copy job consists of a full S3 URI as the source and a destination separated\n"+
				"      by '>', e.g. 's3://source/tumor.cram > s3://destination/collabTumor.cram' or\n"+
				"      folders 's3://source/alignments/tumor > s3://destination/Collab/' or local\n"+
				"      's3://source/alignments/tumor > .' Note, the trailing '/' is required in the\n"+
				"      S3 destination for a recursive copy or when the local folder doesn't exist.\n"+

				"\nOptional/ Defaults:\n" +
				"-d Perform a dry run to list the actions that would be taken\n"+
				"-r Perform a recursive copy, defaults to an exact source key match\n"+
				"-e Email addresse(s) to send status messages, comma delimited, no spaces. Note, \n"+
				"      the sendmail app must be configured on your system. Test it:\n"+
				"      echo 'Subject: Hello' | sendmail yourEmailAddress@yourProvider.com\n"+
				"-x Expedite archive retrieval, increased cost $0.03/GB vs $0.01/GB, 1-5min vs 3-12hr, \n"+
				"      defaults to standard.\n"+
				"-l Execute every hour (standard) or minute (expedited) until complete\n"+
				"-t Maximum threads to utilize, defaults to 8\n"+
				"-p AWS credentials profile, defaults to 'default'\n"+
				"-n Number of days to keep restored files in S3, defaults to 1\n"+
				"-a Print instructions for copying files between different accounts\n"+

				"\nExample: java -Xmx10G -jar pathTo/S3Copy_x.x.jar -e obama@real.gov -p obama -d -l\n"+
				"   -j 's3://source/Logs.zip>s3://destination/,s3://source/normal > ~/Downloads/' -r\n"+ 

				"**************************************************************************************\n");

	}

	public void setResultsCheckOK(boolean resultsCheckOK) {
		this.resultsCheckOK = resultsCheckOK;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public String getRegion() {
		return region;
	}

	public int getNumberDaysToRestore() {
		return numberDaysToRestore;
	}

	public int getMaxTries() {
		return maxTries;
	}

	public int getSecToWait() {
		return secToWait;
	}

	public ProfileCredentialsProvider getCredentials() {
		return credentials;
	}

	public String getProfile() {
		return profile;
	}

	public boolean isRecursiveCopy() {
		return recursiveCopy;
	}

	public File getTmpEmailLog() {
		return tmpEmailLog;
	}

	public Tier getRestoreTier() {
		return restoreTier;
	}






}
