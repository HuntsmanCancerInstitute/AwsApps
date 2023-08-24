package edu.utah.hci.aws.apps.versions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.utah.hci.aws.util.Util;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectVersionsIterable;

/**
# first run this tool with these options to empty the bucket -b hcibioinfo-nix-test -r -d -x
# upload the first
aws s3 cp ~/Code/AwsApps/TestData/VersionManager/testA.txt s3://hcibioinfo-nix-test/test.txt
# overwrite the first with testB.txt, this will be the latest and shouldn't be deleted
aws s3 cp ~/Code/AwsApps/TestData/VersionManager/testB.txt s3://hcibioinfo-nix-test/test.txt
# upload a second 
aws s3 cp ~/Code/AwsApps/TestData/VersionManager/testA.txt s3://hcibioinfo-nix-test/ToDelete/testA.txt
# delete it
aws s3 rm s3://hcibioinfo-nix-test/ToDelete/testA.txt
# upload a third, it should never be touched since it is the latest
aws s3 cp ~/Code/AwsApps/TestData/VersionManager/testA.txt s3://hcibioinfo-nix-test/Untouched/testA.txt
# upload a forth with a uniq extension, and delete it
aws s3 cp ~/Code/AwsApps/TestData/VersionManager/testA.txt s3://hcibioinfo-nix-test/testA.delme
aws s3 rm s3://hcibioinfo-nix-test/testA.delme
# upload a fifth with a uniq prefix, and delete it
aws s3 cp ~/Code/AwsApps/TestData/VersionManager/testA.txt s3://hcibioinfo-nix-test/Delme/testA.txt
aws s3 rm s3://hcibioinfo-nix-test/Delme/testA.txt

# list the contents
aws s3api list-object-versions --bucket hcibioinfo-nix-test
 */
public class VersionManager {

	//user defined fields
	private String bucketName = null;
	private int minDaysOld = 30;
	private String[] suffixes = null;
	private String[] prefixes = null;
	private boolean dryRun = true;
	private String profile = "default";
	private boolean verbose = false;
	private int maxThreads = 8;

	//internal fields
	private ArrayList<ListObjectVersionsResponse> jobsToProcess = new ArrayList<ListObjectVersionsResponse>();
	private boolean lastJobAdded = false;
	private String region = null;
	private S3Client s3 = null;
	private long bytesDeleted = 0;
	private long numberKeysScanned = 0;
	private long numberObjectsScanned = 0;
	private long numberObjectsDeleted = 0;
	private long numberMarksScanned = 0;
	private long numberMarksDeleted = 0;
	private int exitCode = -1;
	private ProfileCredentialsProvider cred = null;


	public VersionManager (String[] args){
		try {
			long startTime = System.currentTimeMillis();

			processArgs(args);

			doWork();

			double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
			pl("\nDone! "+Util.formatNumber(diffTime, 2)+" minutes\n");
			exitCode = 0;

		} catch (Exception ex) {
			exitCode = 1;
			if (s3 != null) s3.close();
			el("\nERROR processing versioned objects: ");
			ex.printStackTrace();
			System.exit(1);
		} 

	}

	private void doWork() throws Exception {

		//make the client
		s3 = S3Client.builder()
				.credentialsProvider(cred)
				.region(Region.of(region))
				.build();

		walkObjectVersions();

		printResults();

		if (s3 != null) s3.close();
	}

	public void walkObjectVersions() throws Exception {
		if (verbose) pl("\nScanning objects and deletion markers (KeyName Type Message VersionID IsLatest DaysOld GBSize):");
		else  p("\nScanning objects and deletion markers");

		ListObjectVersionsIterable versionList = s3.listObjectVersionsPaginator(ListObjectVersionsRequest.builder()
				.maxKeys(1000)
				.bucket(bucketName)
				.build());

		VersionWorker[] vws = new VersionWorker[maxThreads];
		for (int i=0; i< maxThreads; i++) vws[i] = new VersionWorker(this, "T"+(i+1));

		ExecutorService executor = Executors.newFixedThreadPool(maxThreads);
		for (int i=0; i< maxThreads; i++) {
			vws[i] = new VersionWorker(this, "T"+(i+1));
			executor.execute(vws[i]);
		}

		//for each response of 1000 keys
		int numResponsesAdded = 0;
		for (ListObjectVersionsResponse response : versionList) {
			if (verbose==false) p(".");
			numResponsesAdded++;
			jobsToProcess.add(response);
		}
		lastJobAdded = true;
		if (verbose==false) pl("");

		executor.shutdown();
		while (!executor.isTerminated()) {}  //wait here until complete

		//check loaders and update counters
		int responsesProcessed = 0;
		for (VersionWorker vw: vws) {
			if (vw.isFailed() || vw.isComplete()== false) throw new IOException("ERROR: VersionWorker failed "+vw.getThreadName());
			numberKeysScanned += vw.getNumberKeysScanned();
			numberObjectsScanned += vw.getNumberObjectsScanned();
			numberObjectsDeleted += vw.getNumberObjectsDeleted();
			numberMarksScanned += vw.getNumberMarksScanned();
			numberMarksDeleted += vw.getNumberMarksDeleted();
			bytesDeleted += vw.getBytesDeleted();
			responsesProcessed+= vw.getNumberResponsesReceived();
		}

		if (responsesProcessed != numResponsesAdded) throw new IOException ("ResponseJobCheck failed: "+numResponsesAdded+" is not equal to "+responsesProcessed);

	}



	synchronized ListObjectVersionsResponse fetchNextJob() {
		if (jobsToProcess.size()!=0) return jobsToProcess.remove(jobsToProcess.size()-1);
		else return null;
	}

	private void printResults() throws IOException {

		pl("\nProcessing Statistics:");
		pl(numberKeysScanned +"\tKeys scanned");
		pl(numberObjectsScanned +"\tObjects scanned");
		pl(numberObjectsDeleted +"\tObjects deleted");
		pl(numberMarksScanned +"\tMarkers scanned");
		pl(numberMarksDeleted +"\tMarkers deleted");
		pl(Util.formatNumber(Util.gigaBytes(bytesDeleted),3) +"\tTotal GBs deleted");

		if (dryRun) pl("\nNOTE: this was a dry run, nothing was deleted, rerun with the -r option to actually delete objects and deletion marks.");

	}


	public static void pl(String s) {
		System.out.println(s);
	}
	public static void p(String s) {
		System.out.print(s);
	}
	public synchronized void el(String s) {
		System.err.println(s);
	}

	public static void main(String[] args) {
		if (args.length ==0){
			printDocs();
			System.exit(0);
		}
		new VersionManager(args);
	}	

	/**This method will process each argument and assign new variables
	 * @throws Exception */
	public void processArgs(String[] args) throws Exception {

		Pattern pat = Pattern.compile("-[a-z]");
		pl("VersionManager Arguments: "+Util.stringArrayToString(args, " ")+"\n");
		for (int i = 0; i<args.length; i++){
			String lcArg = args[i].toLowerCase();
			Matcher mat = pat.matcher(lcArg);
			if (mat.matches()){
				char test = args[i].charAt(1);
				switch (test){
				case 'b': bucketName = args[++i]; break;
				case 'c': profile = args[++i]; break;
				case 'a': minDaysOld = Integer.parseInt(args[++i]); break;
				case 's': suffixes = Util.COMMA.split(args[++i]); break;
				case 'p': prefixes = Util.COMMA.split(args[++i]); break;
				case 'r': dryRun = false; break;
				case 'v': verbose = false; break;
				case 't': maxThreads = Integer.parseInt(args[++i]); break;
				case 'h': printDocs(); System.exit(0);
				default: Util.printExit("\nProblem, unknown option! " + mat.group());
				}
			}
		}

		//check bucket
		if (bucketName == null) {
			el("\nError: please provide the name of a versioned AWS S3 bucket.\n");
			System.exit(1);
		}

		//set number of threads
		int availThreads = Runtime.getRuntime().availableProcessors()-1;
		if (maxThreads > availThreads) maxThreads = availThreads;

		//fetch region from credentials
		region = Util.getRegionFromCredentials(profile);		
		if (region == null) Util.printErrAndExit("\nERROR: failed to find your region in ~/.aws/credentials for the profile ["+profile+"]");
		cred = ProfileCredentialsProvider.builder().profileName(profile).build();

		printOptions();

	}	



	private void printOptions() {
		pl("Options:");
		pl("  -c Credentials profile name : "+ profile);
		pl("  -b Bucket name              : "+ bucketName);
		pl("  -l Bucket region            : "+ region);
		pl("  -s Obj key suffixes         : "+ Util.stringArrayToString(suffixes, ","));
		pl("  -p Obj key prefixes         : "+ Util.stringArrayToString(prefixes, ","));
		pl("  -a Min age, days            : "+ minDaysOld);
		pl("  -r Dry run?                 : "+ dryRun);
		pl("  -v Verbose output           : "+ verbose);
		pl("  -t Maximum threads          : "+ maxThreads);
	}

	public static void printDocs(){
		pl("\n" +
				"**************************************************************************************\n" +
				"**                             AWS S3 Version Manager :  August 2023                **\n" +
				"**************************************************************************************\n" +
				"Bucket versioning in S3 protects objects from being deleted or overwritten by hiding\n"+
				"the original when 'deleting' or over writing an existing object. Use this tool to \n"+
				"delete these hidden S3 objects and any deletion marks from your buckets. Use the\n"+
				"options to select particular redundant objects to delete in a dry run, review the\n"+
				"actions, and rerun it with the -r option to actually delete them. This app will not\n"+
				"delete any isLatest=true object.\n"+

				"\nWARNING! This app has the potential to destroy precious data. TEST IT on a\n"+
				"pilot system before deploying in production. Although extensively unit tested, this\n"+
				"app is provided with no guarantee of proper function.\n"+

				"\nTo use the app:\n"+ 
				"1) Enable S3 Object versioning on your bucket.\n"+
				"2) Install and configure the aws cli with your region, access and secret keys. See\n"+
				"   https://aws.amazon.com/cli\n"+
				"3) Use cli commands like 'aws s3 rm s3://myBucket/myObj.txt' or the AWS web Console to\n"+
				"   'delete' particular objects. Then run this app to actually delete them.\n"+


				"\nRequired Parameters:\n"+
				"-b Versioned S3 bucket name\n"+

				"\nOptional Parameters:\n" +
				"-r Perform a real run, defaults to a dry run where no objects are deleted\n"+
				"-c Credentials profile name, defaults to 'default'\n"+
				"-a Minimum age, in days, of object to delete, defaults to 30\n"+
				"-s Object key suffixes to delete, comma delimited, no spaces\n"+
				"-p Object key prefixes to delete, comma delimited, no spaces\n"+
				"-v Verbose output\n"+
				"-t Maximum threads to use, defaults to 8\n"+

				"\nExample: java -Xmx10G -jar pathTo/VersionManager_X.X.jar -b mybucket-vm-test \n"+
				"     -s .cram,.bam,.gz,.zip -a 7 -c MiloLab\n\n"+

				"**************************************************************************************\n");
	}

	public boolean isVerbose() {
		return verbose;
	}

	public int getExitCode() {
		return exitCode;
	}

	public long getNumberKeysScanned() {
		return numberKeysScanned;
	}

	public long getNumberObjectsScanned() {
		return numberObjectsScanned;
	}

	public long getNumberObjectsDeleted() {
		return numberObjectsDeleted;
	}

	public long getNumberMarksScanned() {
		return numberMarksScanned;
	}

	public long getNumberMarksDeleted() {
		return numberMarksDeleted;
	}

	public ProfileCredentialsProvider getCred() {
		return cred;
	}

	public String getRegion() {
		return region;
	}

	public String getBucketName() {
		return bucketName;
	}

	public int getMinDaysOld() {
		return minDaysOld;
	}

	public String[] getSuffixes() {
		return suffixes;
	}

	public String[] getPrefixes() {
		return prefixes;
	}

	public boolean isDryRun() {
		return dryRun;
	}

	public boolean isLastJobAdded() {
		return lastJobAdded;
	}
}
