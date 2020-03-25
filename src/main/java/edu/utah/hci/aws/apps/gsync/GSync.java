package edu.utah.hci.aws.apps.gsync;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.mail.MessagingException;
import edu.utah.hci.aws.util.Util;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

/**Syncs a local file system with an S3 bucket. Moves local files that meet criteria (age, size, extension) up to a bucket and replaces them with a 
 * placeholder file containing info about the S3 object (bucket, key, size, ...).  If the moved file has an index file (e.g. xxx.bai, xxx.tbi, etc), 
 * that is moved as well.
 * 
 * To use the app, 
 * 	1) Create a bucket on S3 dedicated to this purpose and enable an AbortIncompleteMultipartUpload lifecycle rule to delete partial failed uploads
 * 	2) Create this file ~/.aws/credentials with your access, secret, and region info, chmod 600 the file and keep it safe.
	 	[default]
		aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A
		aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ
		region = us-west-2
 */
public class GSync {

	//user defined fields
	private File[] dirsToScan = null;
	private String bucketName = null;
	private int minDaysOld = 120;
	private double minGigaBytes = 5;
	private String[] fileExtensions = {".bam", ".cram",".gz", ".zip"};
	private boolean dryRun = true;
	private boolean verbose = false;
	private boolean deleteUploaded = false;
	private boolean updateS3Keys = false;
	private boolean restorePlaceholderFiles = false;  //not implemented
	private String smtpHost = "hci-mail.hci.utah.edu";
	private String email = null;

	//for looping till complete
	private boolean rerunUntilComplete = false;
	private int numMinToSleep = 360;
	private int iterations = 72;
	private boolean runAgain = false;

	//internal fields
	private StringBuilder log = new StringBuilder();
	private String dirsToScanString = null;
	private String deleteFromKey = null;
	private HashMap<String, File> candidatesForUpload = new HashMap<String, File>();

	private TreeSet<File> placeholderFiles = new TreeSet<File>();
	private ArrayList<Placeholder> failingPlaceholders = new ArrayList<Placeholder>();
	private HashMap<String, Placeholder> placeholders = null;
	private HashMap<String, Placeholder> keyPlaceholderToUpdate = new HashMap<String, Placeholder>();

	private String region = null;
	private boolean resultsCheckOK = true;
	private static final int DAYS_IN_S3 = 7;

	//uploads
	private ArrayList<File> localFileAlreadyUploaded = new ArrayList<File>();
	private ArrayList<File> localFileAlreadyUploadedButDiffSize = new ArrayList<File>();
	private ArrayList<File> localFileAlreadyUploadedNoPlaceholder = new ArrayList<File>();
	//private ArrayList<String> s3KeyWithNoLocal = new ArrayList<String>();
	private LinkedHashMap<String, S3ObjectSummary> s3KeyWithNoLocal = new LinkedHashMap<String, S3ObjectSummary>();

	//ready for restore or delete
	private ArrayList<Placeholder> restorePlaceholders = new ArrayList<Placeholder>();
	private ArrayList<Placeholder> deletePlaceholders = new ArrayList<Placeholder>();

	private AmazonS3 s3 = null;

	public GSync (String[] args){
		long startTime = System.currentTimeMillis();

		processArgs(args);

		//loop till done
		while (iterations-- > 0) {
			doWork();
			if (resultsCheckOK == false) {
				System.err.println("\nError found, aborting.");
				System.exit(1);
			}
			if (dryRun == true || runAgain == false || rerunUntilComplete == false) break;
			else {
				try {
					pl("\nWaiting... "+iterations);
					Thread.sleep(1000*60*numMinToSleep);
				} catch (InterruptedException e) {}
			}
		}


		double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
		pl("\nDone! "+Math.round(diffTime)+" minutes\n");

		sendEmail();
	}

	void doWork() {
		try {
			region = Util.getRegionFromCredentials();

			initializeFields();

			scanLocalDir();

			parsePlaceholderFiles();
			if (resultsCheckOK == false) {
				if (updateS3Keys && keyPlaceholderToUpdate.size() != 0) {
					String error = updateKeys();
					if (error != null) throw new Exception(error);
				}
				return;
			}

			scanBucket();

			checkPlaceholders();
			if (resultsCheckOK == false) throw new Exception("Problem with placeholder files.");

			removeLocalFromUploadCandidates();

			printResults();

			if (dryRun == false && resultsCheckOK == true) {

				//delete already uploaded local files from prior GSync run
				if (deleteUploaded) deleteAlreadyUploaded();

				upload();

				if (deletePlaceholders.size() != 0) delete();

				if (restorePlaceholders.size() != 0) {
					String error = restore();
					if (error.length() != 0) throw new IOException(error);
				}
			}
			if (s3 != null) s3.shutdown();

		} catch (Exception ex) {
			if (s3 != null) s3.shutdown();
			if (verbose) el(Util.getStackTrace(ex));
			else el(ex.getMessage());
			resultsCheckOK = false;
		} 
	}

	private void initializeFields() {
		runAgain = false;
		candidatesForUpload = new HashMap<String, File>();
		placeholderFiles = new TreeSet<File>();
		failingPlaceholders = new ArrayList<Placeholder>();
		placeholders = null;
		keyPlaceholderToUpdate = new HashMap<String, Placeholder>();
		resultsCheckOK = true;
		localFileAlreadyUploaded = new ArrayList<File>();
		localFileAlreadyUploadedButDiffSize = new ArrayList<File>();
		localFileAlreadyUploadedNoPlaceholder = new ArrayList<File>();
		//s3KeyWithNoLocal = new ArrayList<String>();
		s3KeyWithNoLocal = new LinkedHashMap<String, S3ObjectSummary>();
		restorePlaceholders = new ArrayList<Placeholder>();
		deletePlaceholders = new ArrayList<Placeholder>();
	}


	/**Attempts to update the S3 keys to match the current placeholder file
	 * @throws IOException 
	 * @return null if no problems or error statement.*/
	private String updateKeys() throws IOException {
		String error = null;
		Placeholder working = null;
		try {
			pl("\nUpdating S3 keys to match local placeholder paths...");

			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

			//for each key needing to be updated
			int numUpdated = 0;
			for (Placeholder p: keyPlaceholderToUpdate.values()) {
				working = p;
				//create new key
				String cp = p.getPlaceHolderFile().getCanonicalPath().replaceFirst(deleteFromKey, "");
				String newKey = Placeholder.PLACEHOLDER_PATTERN.matcher(cp).replaceFirst("");

				//pull old key
				String oldKey = p.getAttribute("key");
				pl("\t"+oldKey +" -> "+newKey);

				//check if old key exists
				if (s3.doesObjectExist(bucketName, oldKey) == false) throw new IOException ("Failed to update key "+oldKey+" to "+newKey+". Original key doesn't exist.");

				//check storage class, may need to trigger a pull from glacier to S3.
				ObjectMetadata omd = s3.getObjectMetadata(bucketName, oldKey);

				boolean ready = true;
				String sc = omd.getStorageClass();
				if ( sc != null) ready = requestArchiveRestore(oldKey, s3);
				if (ready == false) continue;

				//check if new key already exists
				if (s3.doesObjectExist(bucketName, newKey)) throw new IOException ("Failed to update key "+oldKey+" to "+newKey+". It already exists.");

				//copy object to new location
				copyObject(oldKey, newKey);

				//delete the original reference
				s3.deleteObject(bucketName, oldKey);

				//fetch meta data on new object
				ObjectMetadata newOmd = s3.getObjectMetadata(bucketName, newKey);
				
				//update placeholder file
				p.getAttributes().put("key", newKey);
				p.getAttributes().put("etag", newOmd.getETag()); //the etag will change since the number of threads to transfer likely differ.
				p.writePlaceholder(p.getPlaceHolderFile());
				numUpdated++;
			}
			if (verbose) pl("\t"+ numUpdated +" keys sucessfully updated. Relaunch GSync.");
			s3.shutdown();

		} catch (Exception e) {
			error = "ERROR in updating keys for:\n"+working.getMinimalInfo()+"\n"+e.getMessage();
			el(error);
			if (verbose) el(Util.getStackTrace(e));
			if (s3 != null) s3.shutdown();
		} 

		return error;
	}

	private void copyObject(String sourceObjectKey, String destObjectKey) throws Exception{
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).withMultipartCopyThreshold((long) (256 * 1024 * 1024)).build();
		Copy copy = tm.copy(bucketName, sourceObjectKey, bucketName, destObjectKey);
		copy.waitForCompletion();
	}

	private void delete() throws Exception{
		pl("\nDeleting S3 Objects, their delete placeholders, and any matching local files...");
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		for (Placeholder p : deletePlaceholders) {
			pl("\t"+p.getAttribute("key")+"\t"+p.getPlaceHolderFile()+"\t"+p.getLocalFile()+"\t"+p.getStorageClass());
			s3.deleteObject(bucketName, p.getAttribute("key"));
			p.getPlaceHolderFile().delete();
			if (p.getLocalFile() != null) p.getLocalFile().delete();
		}
		pl("\t"+deletePlaceholders.size()+" AWS and local resources deleted (versioned S3 objects can still be recovered)");
		s3.shutdown();
	}

	void sendEmail() {
		if (email == null) return;
		try {
			String status = " - OK - ";
			if (resultsCheckOK == false) status = " - ERROR - ";
			Util.postMail(email, "GSync Run" +status+ Util.getDateTime(), log.toString(), "noreply_gsync@hci.utah.edu", smtpHost);
		} catch (MessagingException e) {
			el("\nError sending email");
			el(Util.getStackTrace(e));
			System.exit(1);
		}
	}

	private String restore() {
		pl("\nRestoring "+restorePlaceholders.size()+" S3 Objects and renaming their restore placeholders to standard...");

		File localFile = null;
		File tempFile = null;
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).build();

		int numRestored = 0;
		try {

			for (Placeholder p : restorePlaceholders) {
				String key = p.getAttribute("key");
				p("\t"+key+"\t"+p.getPlaceHolderFile()+"\t"+p.getStorageClass()+"\t");

				//check storage class, may need to trigger a pull from glacier to S3.
				boolean download = true;
				if (p.getStorageClass().equals("STANDARD") == false) download = requestArchiveRestore(key, s3);

				//good to go?
				if (download) {
					long startTime = System.currentTimeMillis();
					localFile = p.getLocalFile();
					tempFile = new File(localFile.getParentFile(), "tempRestore_"+localFile.getName());

					downloadFile(tm, bucketName, key, tempFile, false) ;

					//check the size
					long placeholderSize = Long.parseLong(p.getAttribute("size"));
					if (placeholderSize != tempFile.length()) throw new IOException("The restored file's size ("+tempFile.length()+"does not match the placeholder size\n"+p.getMinimalInfo());

					//rename the temp to local
					tempFile.renameTo(localFile);

					//rename the placeholder file
					File stdPlaceholder = new File (localFile.getCanonicalPath()+Placeholder.PLACEHOLDER_EXTENSION);
					p.getPlaceHolderFile().renameTo(stdPlaceholder);
					numRestored++;

					//calc time
					double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
					pl(Util.formatNumber(diffTime, 1)+" min");
				}
			}
			tm.shutdownNow();
		} catch (Exception e) {
			//delete the temp and local
			if (localFile != null) localFile.delete();
			if (tempFile != null) tempFile.delete();
			tm.shutdownNow();

			//return the error
			if (verbose) el(Util.getStackTrace(e));
			return "\nRESTORE ERROR: "+e.getMessage();
		} 

		pl("\t"+numRestored+" resources restored");
		return "";
	}

	public static void downloadFile(TransferManager tm, String bucket_name, String key_name, File f, boolean pause) throws Exception {
		Download xfer = tm.download(bucket_name, key_name, f);
		xfer.waitForCompletion();
	}



	public boolean requestArchiveRestore(String key, AmazonS3 s3Client) throws IOException {
		//check if restore in progress
		ObjectMetadata response = s3Client.getObjectMetadata(bucketName, key);
		Boolean restoreFlag = response.getOngoingRestore();
		//request never received
		if (restoreFlag == null) {
			pl("\t\t"+response.getStorageClass()+" restore request placed, relaunch GSync in a few hours.");
			// Create and submit a request to restore an object from Glacier to S3 for xxx days.
			RestoreObjectRequest requestRestore = new RestoreObjectRequest(bucketName, key, DAYS_IN_S3);
			s3Client.restoreObjectV2(requestRestore);
		}
		//true, in progress
		else if (restoreFlag == true) pl("\t\t"+response.getStorageClass()+" restore in progress, relaunch GSync in a few hours.");
		//false, ready for download
		else return true;

		runAgain = true;
		return false;
	}


	private void deleteAlreadyUploaded() throws IOException {
		if (localFileAlreadyUploaded.size() != 0) {
			pl("\nThe following local files have already been successfully uploaded to S3, have a correct placeholder, and are now deleted.");
			for (File f: localFileAlreadyUploaded) {
				pl("\t"+f.getCanonicalPath());
				f.delete();
			}
		}
		localFileAlreadyUploaded.clear();
	}


	/**For unit testing.*/
	public GSync () {}

	private void upload() throws AmazonServiceException, AmazonClientException, IOException, InterruptedException {
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).withMultipartUploadThreshold((long) (256 * 1024 * 1024)).build();
	
		//anything to upload?  all of these methods throw an IOException 
		if (candidatesForUpload.size() !=0) {
			long totalSize = 0;
			int numUpload = candidatesForUpload.size();
			pl("\nUploading "+numUpload+ " files...");
			ArrayList<File> toDelete = new ArrayList<File>();
			int counter = 1;

			for (String key: candidatesForUpload.keySet()) {
				File toUpload = candidatesForUpload.get(key);
				totalSize+= toUpload.length();
				String etagData = upload(key, toUpload, tm, counter+"/"+numUpload);
				toDelete.add(toUpload);
				writePlaceholder(toUpload, etagData);
				counter++;
			}
			//cannot reach this point if there was an error
			double numGb = (double)totalSize/ (double)1073741824;

			pl("\tAll S3 uploads ("+ Util.formatNumber(numGb, 2) +"GB) successfully completed.");
			//delete
			if (deleteUploaded) {
				for (File x: toDelete) {
					if (verbose) pl("\tDeleting "+x.getCanonicalPath());
					x.delete();
				}
			}
			else {
				pl("\nThese files can be safely deleted:");
				for (File x: toDelete) pl("\trm -f "+x.getCanonicalPath());
			}
		}
		tm.shutdownNow();
	}

	private void writePlaceholder(File f, String etag) throws IOException {
		Placeholder placeholder = new Placeholder();
		HashMap<String, String> att = placeholder.getAttributes();
		att.put("bucket", bucketName);
		att.put("key", f.getCanonicalPath().replaceFirst(deleteFromKey, ""));
		att.put("etag", etag);
		att.put("size", new Long(f.length()).toString());
		File p = new File(f.getCanonicalPath()+Placeholder.PLACEHOLDER_EXTENSION);
		placeholder.writePlaceholder(p);
	}

	private File writePlaceholder(File f, S3ObjectSummary os) throws IOException {
		Placeholder placeholder = new Placeholder();
		HashMap<String, String> att = placeholder.getAttributes();
		att.put("bucket", bucketName);
		att.put("key", os.getKey());
		att.put("etag", os.getETag());
		att.put("size", new Long(os.getSize()).toString());
		File p = new File(f.getCanonicalPath()+Placeholder.PLACEHOLDER_EXTENSION);
		placeholder.writePlaceholder(p);
		return  p;
	}

	/**Looks for files with bam, cram, and gz and their indexes bai, crai, tbi.
	 * @return index file or null if not found.*/
	private File findIndex(File file) {
		String name = file.getName();
		File index = null;
		//looks for xxx.bam.bai and xxx.bai
		if (name.endsWith(".bam")) {
			index = new File (file.getParentFile(), name+".bai");
			if (index.exists() == false) index = new File (file.getParentFile(), name.substring(0, name.length()-4)+".bai");
		}
		//looks for xxx.cram.crai and xxx.crai
		else if (name.endsWith(".cram")){
			index = new File (file.getParentFile(), name+".crai");
			if (index.exists() == false) index = new File (file.getParentFile(), name.substring(0, name.length()-5)+".crai");
		}
		//looks for xxx.gz.tbi 
		else if (name.endsWith(".gz")){
			index =  new File (file.getParentFile(), name+".tbi");
		}

		if (index != null && index.exists()) return index;
		return null;

	}

	private String upload(String key, File file, TransferManager tm, String prePend) throws AmazonServiceException, AmazonClientException, IOException, InterruptedException {
		long startTime = System.currentTimeMillis();

		p("\t"+prePend+"\t"+bucketName+"/"+key+"\t");
		Upload u = tm.upload(bucketName, key, file);
		//Util.showTransferProgress(u);
		if (Util.waitForCompletion(u) == false) throw new IOException("Failed S3 upload "+file);

		//calc time
		double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
		pl(Util.formatNumber(diffTime, 1)+" min");

		return u.waitForUploadResult().getETag();
	}

	private void removeLocalFromUploadCandidates() throws IOException {
		// remove localFileAlreadyUploaded from candidatesForUpload
		for (File f: localFileAlreadyUploaded) candidatesForUpload.remove(f.getCanonicalPath().replaceFirst(deleteFromKey, ""));

		// remove localFileAlreadyUploadedButDiffSizeEtag from candidatesForUpload
		for (File f: localFileAlreadyUploadedButDiffSize) candidatesForUpload.remove(f.getCanonicalPath().replaceFirst(deleteFromKey, ""));

		// remove localFileAlreadyUploadedNoPlaceholder from candidatesForUpload
		for (File f: localFileAlreadyUploadedNoPlaceholder) candidatesForUpload.remove(f.getCanonicalPath().replaceFirst(deleteFromKey, ""));

	}

	private void printResults() throws IOException {
		pl("\nGSync status...");

		//local files, only process here if not deleting
		if (deleteUploaded == false) {
			if (localFileAlreadyUploaded.size() != 0) {
				pl("\nThe following local files have already been successfully uploaded to S3, have a correct placeholder, and are ready for deletion.");
				for (File f: localFileAlreadyUploaded) pl("\trm -f "+f.getCanonicalPath());
			}
			else if (verbose) pl("\nNo local files were found that have already been uploaded.");
		}

		//local files that have a presence in s3 but no placeholder
		if (localFileAlreadyUploadedNoPlaceholder.size() != 0) {
			pl("\nThe following local files have a counterpart in S3 but no placeholder file. Major discrepancy, resolve ASAP. Partial upload? Delete S3 objects to initiate a new upload?");
			for (File f: localFileAlreadyUploadedNoPlaceholder) pl("\t"+f.getCanonicalPath());
			resultsCheckOK = false;
		}
		else if (verbose) pl("\nNo local files were found that had an S3 counterpart and were missing placeholder files.");

		//local files that diff in size with their counterpart in s3
		if (localFileAlreadyUploadedButDiffSize.size() != 0) {
			pl("\nThe following local files have a counterpart in S3 that differs in size or etag. Major discrepancy, resolve ASAP. Partial upload? Delete S3 object?");
			for (File f: localFileAlreadyUploadedButDiffSize) pl("\t"+f.getCanonicalPath());
			resultsCheckOK = false;
		}
		else if (verbose) pl("\nNo local files were found that differed in size or etag with their S3 counterpart.");

		//s3 keys
		if (s3KeyWithNoLocal.size() != 0) {
			//just warn?
			if (restorePlaceholderFiles  ==  false ) {
				pl("\nThe following are S3 objects with no local placeholder file. Deleted? Consider deleting the S3 Object or recreating the placeholder with the -c option. Address and restart.");
				for (String f: s3KeyWithNoLocal.keySet()) pl("\t"+f+"\t"+s3KeyWithNoLocal.get(f).getSize());
				resultsCheckOK = false;
			}
			//not a dry run?
			else if (dryRun  == false){
				pl("\nThe following are s3 objects with no local placeholder file. Attempting to recreate each...");
				boolean restored = restorePlaceholders(s3KeyWithNoLocal);
				if (restored) pl("\t\tAll placeholders recreated.");
				else {
					el("\t\tProblems encountered with recreating placeholder file(s).  Address and restart.");
					resultsCheckOK = false;
				}
			}
		}
		else if (verbose) pl("\nNo S3 objects were found that lacked local placeholder references.");

		//All OK?
		if (resultsCheckOK) {

			//ready for upload
			if (candidatesForUpload.size() !=0) {
				if (dryRun) {
					pl("\nThe following local files meet criteria and are ready for upload. Restart with the -r option.");
					for (String f: candidatesForUpload.keySet()) pl("\t"+f);
				}
			}
			else if (verbose) pl("\nNo local files were found that are ready for upload.");

			//any restores
			if (restorePlaceholders.size() != 0) {
				if (dryRun) {
					pl("\nThe following S3 Objects are ready for download and will replace their associated restore placeholder files.");
					for (Placeholder p : restorePlaceholders) pl("\t"+p.getAttribute("key")+" \t "+p.getPlaceHolderFile());
				}
			}
			else if (verbose) pl("\nNo restore placeholder files were found.");

			//any deletes
			if (deletePlaceholders.size() != 0) {
				if (dryRun) {
					pl("\nThe following S3 Objects, their associated delete placeholder file, and any corresponding local file are ready for deletion.");
					for (Placeholder p : deletePlaceholders) pl("\t"+p.getAttribute("key")+"\t"+p.getPlaceHolderFile()+"\t"+p.getLocalFile());
				}
			}
			else if (verbose) pl("\nNo delete placeholder files were found.");


			//all synced?
			if (candidatesForUpload.size() == 0 && localFileAlreadyUploaded.size() == 0 && restorePlaceholders.size() == 0 && deletePlaceholders.size() == 0) pl("\nAll synced, nothing to do.");
		}



	}

	/*Not tested!*/
	private boolean restorePlaceholders(LinkedHashMap<String, S3ObjectSummary> s3KeyWithNoLocal2) throws IOException {
		if (restorePlaceholderFiles == false) return false;
		// Need to build a placeholder file using the s3 object and write it to the appropriate place
		//string PatientAnalysis/Avatar/AJobs/1100529/Alignment/1100529_TumorRNA/Bam/1100529_TumorRNA_Hg38.bai

		for (String key: s3KeyWithNoLocal2.keySet() ) {
			S3ObjectSummary os = s3KeyWithNoLocal2.get(key);

			//check if file exists, doesn't have to but if it does want to check the size
			File f = new File(deleteFromKey+key);

			if (f.exists()) {
				//check size
				long size = os.getSize();
				if (size != f.length()) {
					pl("\tFailed, existing file size ("+ f.length()+ ") differs from S3 Object ("+ size+ ") for "+f+" Either delete the local file or the S3 object.");
					return false;
				}
			}

			//check that the parent dir exists, it must
			File dir= f.getParentFile();
			if (dir.exists() == false) {
				pl("\tFailed, the parent directory does not exist, create it and restart ' mkdir -pv "+ dir+ " '");
				return false;
			}

			//check bucket, should never get this incorrect
			if (os.getBucketName().equals(bucketName) == false) {
				pl ("\tFailed, different bucket names for "+f+" S3: "+os.getBucketName()+" vs "+bucketName);
				return false;
			}

			//write the placeholder
			File p = writePlaceholder(f, os);
			pl("\t"+p+" recreated");
		}

		return true;

	}

	private void checkPlaceholders() throws IOException {

		pl("\nChecking placeholder files against S3 objects...");
		for (Placeholder p: placeholders.values()) {
			boolean ok = true;
			boolean restoreType = false;
			boolean deleteType = false;

			//create the local file, it may or may not exist
			File local = new File(deleteFromKey+p.getAttribute("key")).getCanonicalFile();		
			p.setLocalFile(local);

			//found in S3
			if (p.isFoundInS3() == false) {
				ok = false;
				p.addErrorMessage("Failed to find the associated S3 Object.");
			}
			else {
				//for those with match in S3
				if (p.isS3SizeMatches() == false) {
					ok = false;
					p.addErrorMessage("S3 object size doesn't match the size in this placeholder file.");
				}
				if (p.isS3EtagMatches() == false) {
					ok = false;
					p.addErrorMessage("S3 object etag doesn't match the etag in this placeholder file.");
				}
				//restore? 
				if (p.getType().equals(Placeholder.TYPE_RESTORE)) {
					restoreType = true;
					if (local.exists()) {
						ok = false;
						//check size
						long pSize = Long.parseLong(p.getAttribute("size"));
						if (pSize == local.length()) p.addErrorMessage("The local file already exists. Size matches. File looks as if it is already restored? Rename the restore placeholder?\n\t\tmv "+ p.getPlaceHolderFile() +" "+p.getPlaceHolderFile().toString().replace(".restore", ""));
						else p.addErrorMessage("The local file already exists. The sizes do not match! Suspect partial restore. Recommend deleting the local file.\n\t\trm -f "+p.getLocalFile());
					}
				}
				//delete?
				else if (p.getType().equals(Placeholder.TYPE_DELETE)) deleteType = true;
			}
			//save it
			if (ok == false) failingPlaceholders.add(p);
			else {
				if (restoreType) restorePlaceholders.add(p);
				else if (deleteType) deletePlaceholders.add(p);
			}
		}

		//any failing placeholders?
		if (failingPlaceholders.size() !=0) {
			pl("\tIssues were identified when parsing the placeholder files, address and restart:\n");
			for (Placeholder ph: failingPlaceholders) pl(ph.getMinimalInfo());
			resultsCheckOK = false;
		}
	}

	void pl(String s) {
		System.out.println(s);
		log.append(s);
		log.append("\n");
	}
	void p(String s) {
		System.out.print(s);
		log.append(s);
	}
	void el(String s) {
		System.err.println(s);
		log.append(s);
	}

	private void scanBucket() throws IOException {

		pl("\nScanning S3 bucket...");
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

		S3Objects.inBucket(s3, bucketName).forEach((S3ObjectSummary os) -> {
			String key = os.getKey();

			//check candidatesForUpload
			if (candidatesForUpload.containsKey(key)) {
				//OK, a local fat old file with the same name has already been uploaded to S3
				//check size and etag
				if (os.getSize() == candidatesForUpload.get(key).length()) {
					//check for a placeholder
					if (placeholders.containsKey(key))localFileAlreadyUploaded.add(candidatesForUpload.get(key));
					else  localFileAlreadyUploadedNoPlaceholder.add(candidatesForUpload.get(key));
				}
				else localFileAlreadyUploadedButDiffSize.add(candidatesForUpload.get(key));
			}

			//check placeholders
			if (placeholders.containsKey(key)) {
				//OK, a local placeholder file contains the aws key
				Placeholder p = placeholders.get(key);
				p.setFoundInS3(true);
				//Set storage class STANDARD, DEEP_ARCHIVE, GLACIER
				p.setStorageClass(os.getStorageClass());
				//check size
				String size = p.getAttribute("size");
				if (Long.parseLong(size) == os.getSize()) p.setS3SizeMatches(true);
				else p.setS3SizeMatches(false);
				//check etag
				if (p.getAttribute("etag").equals(os.getETag())) p.setS3EtagMatches(true);
				else p.setS3EtagMatches(false);
			}

			else if (os.getSize() != 0) {
				//OK, this is an unknown s3 object with no local reference
				//s3KeyWithNoLocal.add(key);
				s3KeyWithNoLocal.put(key, os);
			}
		});
		s3.shutdown();
	}

	/**Check the placeholders for issues.*/
	private void parsePlaceholderFiles() throws IOException {
		pl("\nChecking local placeholder files...");
		placeholders = new HashMap<String, Placeholder>();
		for (File f: placeholderFiles) {
			Placeholder p = new Placeholder(f,deleteFromKey, this);
			String key = p.getAttribute("key");

			//does the path match the key?
			if (p.isKeyMatchesLocalPlaceholderPath()) {

				//duplicate placeholders?
				if (placeholders.containsKey(key)) {
					p.addErrorMessage("Multiple placeholder files found with the same key '"+key+"'. Can only have one std, restore, or delete placeholder at a time." );
					failingPlaceholders.add(p);
				}
				else placeholders.put(key, p);

			}
			else {
				p.addErrorMessage("The current file path does not match S3 key path. Was this placeholder file moved? If so attempt a fix by running GSync with the -u option." );
				failingPlaceholders.add(p);
				keyPlaceholderToUpdate.put(key, p);
			}
		}
		//any failing paths?
		if (failingPlaceholders.size() !=0) {
			//is it just key mismatches and they want to update them
			if (failingPlaceholders.size() != keyPlaceholderToUpdate.size() || updateS3Keys == false) {
				pl("\tIssues were identified when parsing the placeholder files, address and restart:\n");
				for (Placeholder ph: failingPlaceholders) pl(ph.getMinimalInfo());
			}
			//otherwise don't print the error message
			resultsCheckOK = false;
		}
		else if (verbose) pl("\t"+placeholders.size()+" passing placeholder files");
	}

	void scanLocalDir() throws IOException {
		pl("\nScanning local directory...");
		for (File d: dirsToScan) scanDirectory(d, fileExtensions);
		if (verbose) pl("\t"+candidatesForUpload.size()+" candidate files with indexes for upload");
	}

	/**Fetches all files with a given extension, min size, min age, not symbolic links. Also save any that are aws uploaded placeholder files and restore placeholder files.
	 * Includes indexes for bam, cram, and tabix gz if present*/
	private void scanDirectory (File directory, String[] fileExtensions) throws IOException{ 
		//is the dir symlinked?
		if (Files.isSymbolicLink(directory.toPath())) return;

		File[] list = directory.listFiles();
		for (int i=0; i< list.length; i++){
			if (list[i].isDirectory()) scanDirectory (list[i], fileExtensions);
			else {
				//matching extension?
				String fileName = list[i].getName();
				boolean match = false;
				for (String ext: fileExtensions) {
					if (fileName.endsWith(ext)) {
						match = true;
						break;
					}
				}
				//symlink?
				Path path = list[i].toPath();
				boolean symlink = Files.isSymbolicLink(path);

				//don't proc for now, major issues
				//if (updateSymlinks && symlink) updateSymlink(path);

				if (match) {
					if (symlink == false) {
						//size?
						double size = Util.gigaBytes(list[i]);
						if (size >= minGigaBytes) {

							//age in days?
							int age = Util.ageOfFileInDays(list[i]);
							if (age >= minDaysOld) {
								candidatesForUpload.put(list[i].getCanonicalPath().replaceFirst(deleteFromKey, ""), list[i].getCanonicalFile());
								if (verbose) pl("\tAdding upload candidate "+ list[i].getCanonicalFile());
								//look for index file
								File index = findIndex(list[i]);
								if (index != null) {
									candidatesForUpload.put(index.getCanonicalPath().replaceFirst(deleteFromKey, ""), index.getCanonicalFile());
									if (verbose) pl("\tAdding upload candidate index "+ index.getCanonicalPath());
								}
							}
						}
					}
				}
				else if (fileName.contains(Placeholder.PLACEHOLDER_EXTENSION) && symlink == false) {
					placeholderFiles.add(list[i].getCanonicalFile());
					if (verbose) pl("\tPlaceholder found "+ list[i].getCanonicalPath());
				}
			}
		}
	}

	public static void main(String[] args) {
		if (args.length ==0){
			new GSync().printDocs();
			System.exit(0);
		}
		new GSync(args);
	}	

	/**This method will process each argument and assign new variables*/
	public void processArgs(String[] args) {
		try {
			Pattern pat = Pattern.compile("-[a-z]");
			pl("GSync Arguments: "+Util.stringArrayToString(args, " ")+"\n");
			String dirString = null;
			for (int i = 0; i<args.length; i++){
				String lcArg = args[i].toLowerCase();
				Matcher mat = pat.matcher(lcArg);
				if (mat.matches()){
					char test = args[i].charAt(1);
					try{
						switch (test){
						case 'd': dirString = args[++i]; break;
						case 'b': bucketName = args[++i]; break;
						case 'e': email = args[++i]; break;
						case 's': smtpHost = args[++i]; break;
						case 'a': minDaysOld = Integer.parseInt(args[++i]); break;
						case 'g': minGigaBytes = Double.parseDouble(args[++i]); break;
						case 'f': fileExtensions = Util.COMMA.split(args[++i]); break;
						case 'r': dryRun = false; break;
						case 'v': verbose = true; break;
						case 'u': updateS3Keys = true; break;
						case 'c': restorePlaceholderFiles = true; break;
						case 'k': deleteUploaded = true; break;
						case 'x': rerunUntilComplete = true; break;
						case 'h': printDocs(); System.exit(0);
						default: Util.printExit("\nProblem, unknown option! " + mat.group());
						}
					}
					catch (Exception e){
						Util.printExit("\nSorry, something doesn't look right with this parameter: -"+test+"\n");
					}
				}
			}

			//create dirs
			if (dirString == null) {
				el("\nError: please provide one or more directories to sync with S3, comma delimited, no spaces, relative path.\n");
				System.exit(1);
			}

			setScanPaths(dirString);

			//check bucket
			if (bucketName == null) {
				el("\nError: please provide the name of a dedicated GSync AWS S3 bucket.\n");
				System.exit(1);
			}
			if (dryRun) {
				deleteUploaded = false;
				if (restorePlaceholderFiles) {
					el("\nCannot restore placeholders with -c when performing a dry run.");
					System.exit(1);
				}
			}

			printOptions();

		} catch (Exception e) {
			el("\nError processing arguments");
			el(Util.getStackTrace(e));
			System.exit(1);
		}
	}	

	private void setScanPaths(String dirString) throws IOException {
		String[] splitDirString = Util.COMMA.split(dirString);

		dirsToScan = new File[splitDirString.length];
		StringBuilder sb = new StringBuilder();	
		for (int i=0; i< dirsToScan.length; i++) {
			dirsToScan[i] = new File(splitDirString[i]).getCanonicalFile();
			if (dirsToScan[i].exists() == false) throw new IOException("Fails to exist "+dirsToScan[i]);
			sb.append(dirsToScan[i].toString());
			if (i!=(dirsToScan.length-1)) sb.append(", ");
		}
		dirsToScanString = sb.toString();	

		//find index of last common parent folder
		String[] firstDirs = Util.FORWARD_SLASH.split(dirsToScan[0].toString().substring(1));
		String prior = "/";

		//check each dir in the test
		for (int x=0; x<firstDirs.length; x++) {		
			String test = prior+ firstDirs[x]+ "/";
			//for each path to scan
			boolean stop = false;
			for (int i=1; i< dirsToScan.length; i++) {
				if (dirsToScan[i].toString().startsWith(test) == false) {
					stop = true;
					break;
				}
			}
			if (stop) break;
			else prior = test;
		}

		//create the deleteFromKey field
		File parent = new File(prior).getCanonicalFile();
		deleteFromKey = parent.getParentFile().getCanonicalPath()+"/";
		if (deleteFromKey.equals("//")) deleteFromKey = "/";	
	}

	private void printOptions() {
		pl("Options:");
		pl("  -d Local directories         : "+ dirsToScanString);
		pl("  -b S3 Bucket name            : "+ bucketName);
		pl("  -f File extensions           : "+ Util.stringArrayToString(fileExtensions, ","));
		pl("  -a Min file days             : "+ minDaysOld);
		pl("  -g Min file GB               : "+ minGigaBytes);
		pl("  -r Dry run                   : "+ dryRun);
		pl("  -u Update S3 keys            : "+ updateS3Keys);
		pl("  -c Recreate placeholders     : "+ restorePlaceholderFiles);
		pl("  -v Verbose output            : "+ verbose);
		pl("  -e Email                     : "+ email);
		pl("  -s Smtp host                 : "+ smtpHost);
		pl("  -k Delete local after upload : "+ deleteUploaded);
		pl("  -x Rerun till complete       : "+ rerunUntilComplete);
	}

	public void printDocs(){
		pl("\n" +
				"**************************************************************************************\n" +
				"**                                   GSync : Feb 2020                               **\n" +
				"**************************************************************************************\n" +
				"GSync pushes files with a particular extension that exceed a given size and age to \n" +
				"Amazon's S3 object store. Associated genomic index files are also moved. Once \n"+
				"correctly uploaded, GSync replaces the original file with a local txt placeholder file \n"+
				"containing information about the S3 object. Files are restored or deleted by modifying\n"+
				"the name of the placeholder file. Symbolic links are ignored.\n"+

				"\nWARNING! This app has the potential to destroy precious genomic data. TEST IT on a\n"+
				"pilot system before deploying in production. BACKUP your local files and ENABLE S3\n"+
				"Object Versioning before running.  This app is provided with no guarantee of proper\n"+
				"function.\n"+

				"\nTo use the app:\n"+ 
				"1) Create a new S3 bucket dedicated solely to this purpose. Use it for nothing else.\n"+
				"2) Enable S3 Object Locking and Versioning on the bucket to assist in preventing \n"+
				"   accidental object overwriting. Add lifecycle rules to\n"+
				"   AbortIncompleteMultipartUpload and move objects to Deep Glacier.\n"+
				"3) It is a good policy when working on AWS S3 to limit your ability to accidentally\n"+
				"   delete buckets and objects. To do so, create and assign yourself to an AWS Group \n"+
				"   called AllExceptS3Delete with a custom permission policy that denies s3:Delete*:\n"+
				"   {\"Version\": \"2012-10-17\", \"Statement\": [\n" + 
				"      {\"Effect\": \"Allow\", \"Action\": \"*\", \"Resource\": \"*\"},\n" + 
				"      {\"Effect\": \"Deny\", \"Action\": \"s3:Delete*\", \"Resource\": \"*\"} ]}\n"+ 
				"   For standard upload and download gsyncs, assign yourself to the AllExceptS3Delete\n"+
				"   group. When you need to delete objects or buckets, switch to the Admin group, then\n"+
				"   switch back. Accidental overwrites are OK since object versioning is enabled.\n"+
				"   To add another layer of protection, apply object legal locks via the aws cli.\n"+
				"3) Create a ~/.aws/credentials file with your access, secret, and region info, chmod\n"+
				"   600 the file and keep it private. Use a txt editor or the aws cli configure\n"+
				"   command, see https://aws.amazon.com/cli   Example ~/.aws/credentials file:\n"+
				"   [default]\n"+
				"   aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A\n"+
				"   aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ\n"+
				"   region = us-west-2\n"+
				"4) Execute GSync to upload large old files to S3 and replace them with a placeholder\n"+
				"   file named xxx"+Placeholder.PLACEHOLDER_EXTENSION+"\n"+
				"5) To download and restore an archived file, rename the placeholder\n"+
				"   xxx"+Placeholder.RESTORE_PLACEHOLDER_EXTENSION+ " and run GSync.\n"+
				"6) To delete an S3 archived file, it's placeholder, and any local files, rename the \n"+
				"   placeholder xxx"+Placeholder.DELETE_PLACEHOLDER_EXTENSION+" and run GSync.\n"+
				"   Before executing, switch the GSync/AWS user to the Admin group.\n"+
				"7) Placeholder files may be moved, see -u\n"+ 

				"\nRequired:\n"+
				"-d One or more local directories with the same parent to sync. This parent dir\n"+
				"     becomes the base key in S3, e.g. BucketName/Parent/.... Comma delimited, no\n"+
				"     spaces, see the example.\n"+
				"-b Dedicated S3 bucket name\n"+

				"\nOptional:\n" +
				"-f File extensions to consider, comma delimited, no spaces, case sensitive. Defaults\n"+
				"     to '.bam,.cram,.gz,.zip'\n" +
				"-a Minimum days old for archiving, defaults to 120\n"+
				"-g Minimum gigabyte size for archiving, defaults to 5\n"+
				"-r Perform a real run, defaults to just listing the actions that would be taken.\n"+
				"-k Delete local files that were successfully uploaded.\n"+
				"-u Update S3 Object keys to match current placeholder paths.\n"+
				"-c Recreate deleted placeholder files using info from orphaned S3 Objects.\n"+
				"-v Verbose output.\n"+
				"-e Email addresses to send gsync messages, comma delimited, no spaces.\n"+
				"-s Smtp host, defaults to hci-mail.hci.utah.edu\n"+
				"-x Execute every 6 hrs until complete, defaults to just once, good for downloading\n"+
				"    latent glacier objects.\n"+

				"\nExample: java -Xmx20G -jar pathTo/GSync_X.X.jar -r -u -k -b hcibioinfo_gsync_repo \n"+
				"     -v -a 90 -g 1 -d -d /Repo/DNA,/Repo/RNA,/Repo/Fastq -e obama@real.gov\n\n"+

				"**************************************************************************************\n");

	}

	public void setLocalDir(File localDir) throws IOException {
		dirsToScan = new File[1];
		dirsToScan[0] = localDir;
		deleteFromKey = localDir.getParentFile().getCanonicalPath()+"/";
	}

	public HashMap<String, File> getCandidatesForUpload() {
		return candidatesForUpload;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public void setMinDaysOld(int minDaysOld) {
		this.minDaysOld = minDaysOld;
	}

	public void setMinGigaBytes(double minGigaBytes) {
		this.minGigaBytes = minGigaBytes;
	}

	public void setDryRun(boolean dryRun) {
		this.dryRun = dryRun;
	}


	public void setDeleteUploaded(boolean deleteUploaded) {
		this.deleteUploaded = deleteUploaded;
	}


	public boolean isResultsCheckOK() {
		return resultsCheckOK;
	}


	public String getBucketName() {
		return bucketName;
	}


	public TreeSet<File> getPlaceholderFiles() {
		return placeholderFiles;
	}


	public HashMap<String, Placeholder> getPlaceholders() {
		return placeholders;
	}


	public ArrayList<File> getLocalFileAlreadyUploaded() {
		return localFileAlreadyUploaded;
	}


	public ArrayList<File> getLocalFileAlreadyUploadedButDiffSize() {
		return localFileAlreadyUploadedButDiffSize;
	}


	public ArrayList<File> getLocalFileAlreadyUploadedNoPlaceholder() {
		return localFileAlreadyUploadedNoPlaceholder;
	}


	public LinkedHashMap<String, S3ObjectSummary> getS3KeyWithNoLocal() {
		return s3KeyWithNoLocal;
	}


	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	public ArrayList<Placeholder> getFailingPlaceholders() {
		return failingPlaceholders;
	}


	public void setUpdateS3Keys(boolean updateS3Keys) {
		this.updateS3Keys = updateS3Keys;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public void setRestorePlaceholderFiles(boolean restorePlaceholderFiles) {
		this.restorePlaceholderFiles = restorePlaceholderFiles;
	}

}
