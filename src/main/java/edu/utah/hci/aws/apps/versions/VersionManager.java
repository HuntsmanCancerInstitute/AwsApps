package edu.utah.hci.aws.apps.versions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.utah.hci.aws.util.Util;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ObjectVersion;
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
	private boolean verbose = true;

	//internal fields
	private LinkedHashMap<String, AwsKey> keyObj = new LinkedHashMap<String, AwsKey>();
	private String region = null;
	private S3Client s3 = null;
	private long bytesDeleted = 0;
	private long numberKeysScanned = 0;
	private long numberObjectsScanned = 0;
	private long numberObjectsDeleted = 0;
	private long numberMarksScanned = 0;
	private long numberMarksDeleted = 0;
	private int exitCode = -1;
	

	public VersionManager (String[] args){
		try {
			long startTime = System.currentTimeMillis();

			processArgs(args);

			doWork();

			double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
			pl("\nDone! "+Math.round(diffTime)+" minutes\n");
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
		ProfileCredentialsProvider cred = ProfileCredentialsProvider.builder().profileName(profile).build();
		s3 = S3Client.builder()
				.credentialsProvider(cred)
				.region(Region.of(region))
				.build();

		if (verbose) pl("\nScanning objects and deletion markers (KeyName Type Message VersionID IsLatest DaysOld GBSize):");
		walkObjectVersions();
		
		printResults();

		if (s3 != null) s3.close();
	}

	public void walkObjectVersions() {

			ListObjectVersionsIterable versionList = s3.listObjectVersionsPaginator(ListObjectVersionsRequest.builder()
					.maxKeys(1000)
					.bucket(bucketName)
					.build());
			
			//for each response of 1000 keys
			for (ListObjectVersionsResponse response : versionList) {
				keyObj.clear();
				loadVersions(response);
				loadMarks(response);
				processKeys();
			}
	}

	private void processKeys() {
		numberKeysScanned+= keyObj.size();
		
		for (String key: keyObj.keySet()) {
			
			String message = null;
			//any must match suffixes?
			if (suffixes != null) {
				boolean suffixMatched = false;
				for (String s: suffixes) {
					if (key.endsWith(s)) {
						suffixMatched = true;
						break;
					}
				}
				if (suffixMatched == false) message = "KEEP_NoMatchingSuffix";
			}
			//any must match prefix?
			if (message == null && prefixes != null) {
				boolean prefixesMatched = false;
				for (String s: prefixes) {
					if (key.startsWith(s)) {
						prefixesMatched = true;
						break;
					}
				}
				if (prefixesMatched == false) message = "KEEP_NoMatchingPrefix";
			}

			//process the key?
			AwsKey ak = keyObj.get(key);
			if (message == null) ak.trimObjects(minDaysOld, this, key);
			else if (verbose) ak.writeAll(message);
		}
	}

	private void loadMarks(ListObjectVersionsResponse response) {
		for (DeleteMarkerEntry marker : response.deleteMarkers()) {
			numberMarksScanned++;
			AwsKey key = keyObj.get(marker.key());
			if (key == null) {
				key = new AwsKey();
				keyObj.put(marker.key(), key);
			}
			key.addMarks(marker);
		}
	}

	private void loadVersions(ListObjectVersionsResponse response) {
		for (ObjectVersion version : response.versions()) {
			numberObjectsScanned++;
			String key = version.key();
			AwsKey awskey = keyObj.get(key);
			if (awskey == null) {
				awskey = new AwsKey();
				keyObj.put(key, awskey);
			}
			awskey.addVersion(version);
		}
	}
	
	public void deleteObject(ObjectVersion ov, String message) {
		numberObjectsDeleted++;
		bytesDeleted+= ov.size();
		if (verbose) Util.pl(AwsKey.objectToString(ov, message));
		
		//really delete it?
		if (dryRun == false) {
			s3.deleteObject(DeleteObjectRequest.builder()
					.bucket(bucketName)
					.key(ov.key())
					.versionId(ov.versionId())
					.build()); 
		}
	}

	public void deleteMarkers(ArrayList<DeleteMarkerEntry> markers, String message) {
		if (markers != null) {
			for (DeleteMarkerEntry m : markers) deleteMarker(m, message);
		}
		
	}

	private void deleteMarker(DeleteMarkerEntry m, String message) {
		numberMarksDeleted++;
		if (verbose) Util.pl(AwsKey.markerToString(m, message));
		
		//really delete it?
		if (dryRun == false) {
			s3.deleteObject(DeleteObjectRequest.builder()
					.bucket(bucketName)
					.key(m.key())
					.versionId(m.versionId())
					.build());
		}
	}

	/*
	private String deleteMarkerOld(DeleteMarkerEntry marker) {
		double daysOld = daysOld(marker.lastModified());
		String key = marker.key();
		StringBuilder sb = new StringBuilder("\t");
		sb.append(key); sb.append("\t");
		sb.append(marker.versionId()); sb.append("\t");
		sb.append(Util.formatNumber(daysOld, 2)); sb.append("\t0");
		String tailMessage = sb.toString();
		
		if (deleteAll) return "DELETE_All" + tailMessage;
	
		//any must match suffixes?
		if (suffixes != null) {
			boolean suffixMatched = false;
			for (String s: suffixes) {
				if (key.endsWith(s)) {
					suffixMatched = true;
					break;
				}
			}
			if (suffixMatched == false) return "KEEP_NoMatchingSuffix"+ tailMessage;
		}

		//any must match prefix?
		if (prefixes != null) {
			boolean prefixesMatched = false;
			for (String s: prefixes) {
				if (key.startsWith(s)) {
					prefixesMatched = true;
					break;
				}
			}
			if (prefixesMatched == false) return "KEEP_NoMatchingPrefix"+ tailMessage;
		}
		
		//check age
		if (minDaysOld != 0) {
			if (daysOld < minDaysOld) return "KEEP_DaysOld"+ tailMessage;
		}

		return "DELETE"+ tailMessage;
	}

	private String deleteVersion(ObjectVersion version) {
		
		double daysOld = daysOld(version.lastModified());
		double gbSize = Util.gigaBytes(version.size());
		String key = version.key();
		
		StringBuilder sb = new StringBuilder("\t");
		sb.append(key); sb.append("\t");
		sb.append(version.versionId()); sb.append("\t");
		sb.append(Util.formatNumber(daysOld, 2)); sb.append("\t");
		if (gbSize >= 0.01) sb.append(Util.formatNumber(gbSize, 2));
		else sb.append(Util.formatNumber(gbSize, 9));
		String tailMessage = sb.toString();
		
		//Message S3Uri VersionID DaysOld GBSize
		
		if (deleteAll) return "DELETE_All" + tailMessage;

		//is this the latest with no deletion mark then don't delete it
		if (version.isLatest()) return "KEEP_LatestVersion"+ tailMessage;

		//any must match suffixes?
		if (suffixes != null) {
			boolean suffixMatched = false;
			for (String s: suffixes) {
				if (key.endsWith(s)) {
					suffixMatched = true;
					break;
				}
			}
			if (suffixMatched == false) return "KEEP_NoMatchingSuffix"+ tailMessage;
		}

		//any must match prefix?
		if (prefixes != null) {
			boolean prefixesMatched = false;
			for (String s: prefixes) {
				if (key.startsWith(s)) {
					prefixesMatched = true;
					break;
				}
			}
			if (prefixesMatched == false) return "KEEP_NoMatchingPrefix"+ tailMessage;
		}
		
		//check age
		if (minDaysOld != 0) {
			if (daysOld < minDaysOld) return "KEEP_DaysOld"+ tailMessage;
		}
		//check size
		if (minGigaBytes !=0) {
			double gb = Util.gigaBytes(version.size());
			if (gb < minGigaBytes) return gb+"KEEP_Size"+ tailMessage;
		}

		return "DELETE"+ tailMessage;
	}
	
	private double daysOld(Instant i) {
		long secLastMod = i.getEpochSecond();
		long secNow = Instant.now().getEpochSecond();
		long secOld = secNow - secLastMod;
		return (double)secOld/86400.0;
	}


	public static void listBucketObjects(S3Client s3, String bucketName ) {

		try {

			boolean isTruncated = true;
			while(isTruncated) {
				ListObjectsV2Iterable objectListing = s3.listObjectsV2Paginator(ListObjectsV2Request.builder()
						.bucket(bucketName)
						.build());
				for (ListObjectsV2Response response : objectListing) {
					for (S3Object s3Object : response.contents()) {
						//do something
						s3.deleteObject(DeleteObjectRequest.builder()
								.bucket(bucketName)
								.key(s3Object.key())
								.build());
					}
					isTruncated = response.isTruncated();
				}
			}

		} catch (S3Exception e) {
			System.err.println(e.awsErrorDetails().errorMessage());
			System.exit(1);
		}
	}*/


	/**Attempts 's3.deleteObject(bucketName, key)' maxTries before throwing error message
	private void tryDeleteObject(String bucketName, String key) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				//s3.deleteObject(bucketName, key);
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 's3.deleteObject(bucketName, key)' trying again, "+attempt);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 's3.deleteObject(bucketName, key)' trying again, "+attempt);
			};
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed to delete "+key+" from "+bucketName+" S3 error message:\n"+error);
	}
	private void sleep(String message) {
		try {
			pl(message+", sleeping "+minToWait+" minutes");
			TimeUnit.MINUTES.sleep(minToWait);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}*/

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
	public static void el(String s) {
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
				case 'q': verbose = false; break;
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

		//pull region from credentials
		region = Util.getRegionFromCredentials();

		printOptions();

	}	


	private void printOptions() {
		pl("Options:");
		pl("  -c Credentials profile name : "+ profile);
		pl("  -b Bucket name and region   : "+ bucketName +" in "+region);
		pl("  -s Obj key suffixes         : "+ Util.stringArrayToString(suffixes, ","));
		pl("  -p Obj key prefixes         : "+ Util.stringArrayToString(prefixes, ","));
		pl("  -a Min age, days            : "+ minDaysOld);
		pl("  -r Dry run?                 : "+ dryRun);
		pl("  -q Verbose output           : "+ verbose);
	}

	public static void printDocs(){
		pl("\n" +
				"**************************************************************************************\n" +
				"**                             AWS S3 Version Manager : January 2022                **\n" +
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
				"-q Quiet output.\n"+

				"\nExample: java -Xmx10G -jar pathTo/VersionManager_X.X.jar -b mybucket-vm-test \n"+
				"     -s .cram,.bam,.gz,.zip -a 7 -c MiloLab \n\n"+

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
}
