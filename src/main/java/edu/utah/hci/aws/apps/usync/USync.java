package edu.utah.hci.aws.apps.usync;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.mail.MessagingException;
import edu.utah.hci.aws.util.Util;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.GetObjectTaggingRequest;
import com.amazonaws.services.s3.model.GetObjectTaggingResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.Tag;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;

/**Syncs a local file system with an S3 bucket. Moves local files that meet criteria (age, size) up to a bucket and replaces them with a 
 * placeholder file containing info about the S3 object (bucket, key, size, ...).
 * 
 * To use the app, 
 * 	1) Create a bucket on S3 dedicated to this purpose and enable an AbortIncompleteMultipartUpload lifecycle rule to delete partial failed uploads
 * 	2) Create this file ~/.aws/credentials with your access, secret, and region info, chmod 600 the file and keep it safe.
	 	[default]
		aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A
		aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ
		region = us-west-2
 */
public class USync {

	//user defined fields
	private File directoryToScan = null;
	private String bucketName = null;
	private int minDaysOld = 120;
	private double minGigaBytes = 1;
	private boolean dryRun = true;
	private boolean verbose = true;
	private String[] fileExtensionsToExclude = {".txt",".log"};
	private boolean restoreDeletedPlaceholders = false;
	private String smtpHost = "hci-mail.hci.utah.edu";
	private String email = null;

	//internal fields
	private StringBuilder log = new StringBuilder();
	private int deleteFromPath = -1;
	private ArrayList<File> candidatesForUpload = new ArrayList<File>();

	private TreeSet<File> placeholderFiles = new TreeSet<File>();
	private HashMap<String, UPlaceholder> keyPlaceholders = null;
	private HashMap<String, UPlaceholder> keyPlaceholderToUpdate = new HashMap<String, UPlaceholder>();

	private String region = null;
	private boolean resultsCheckOK = true;
	private static final int DAYS_IN_S3 = 3;

	//uploads
	private ArrayList<File> localFileAlreadyUploaded = new ArrayList<File>();
	private ArrayList<File> localFileAlreadyUploadedButDiffSize = new ArrayList<File>();
	private ArrayList<File> localFileAlreadyUploadedNoPlaceholder = new ArrayList<File>();
	private LinkedHashMap<String, S3ObjectSummary> s3KeyWithNoLocal = new LinkedHashMap<String, S3ObjectSummary>();

	//ready for restore or delete
	private ArrayList<UPlaceholder> restorePlaceholders = new ArrayList<UPlaceholder>();

	private AmazonS3 s3 = null;

	public USync (String[] args){
		long startTime = System.currentTimeMillis();

		processArgs(args);

		doWork();
		
		if (resultsCheckOK == false) {
			pl("\nResults check failed, error found, aborting.");
			sendEmail();
			System.exit(1);
		}
			
		double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
		pl("\nDone! "+Math.round(diffTime)+" minutes\n");

		sendEmail();
	}

	void doWork() {
		try {
			region = Util.getRegionFromCredentials();

			initializeFields();

			//walks dirs collecting placeholder files and those that are big and old
			scanLocalDir();

			//load the placeholder files, look for duplicates, return if found.
			parsePlaceholderFiles();
			if (resultsCheckOK == false) return;

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
		failingPlaceholders = new ArrayList<UPlaceholder>();
		keyPlaceholders = null;
		keyPlaceholderToUpdate = new HashMap<String, UPlaceholder>();
		resultsCheckOK = true;
		localFileAlreadyUploaded = new ArrayList<File>();
		localFileAlreadyUploadedButDiffSize = new ArrayList<File>();
		localFileAlreadyUploadedNoPlaceholder = new ArrayList<File>();
		s3KeyWithNoLocal = new LinkedHashMap<String, S3ObjectSummary>();
		restorePlaceholders = new ArrayList<UPlaceholder>();
		deletePlaceholders = new ArrayList<UPlaceholder>();
	}


	/**Attempts to update the S3 keys to match the current placeholder file
	 * @throws IOException 
	 * @return null if no problems or error statement.*/
	private String updateKeys() throws IOException {
		String error = null;
		UPlaceholder working = null;
		try {
			pl("\nUpdating S3 keys to match local placeholder paths...");

			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

			//for each key needing to be updated
			int numUpdated = 0;
			for (UPlaceholder p: keyPlaceholderToUpdate.values()) {
				working = p;
				//create new key
				String cp = p.getPlaceHolderFile().getCanonicalPath().replaceFirst(deleteFromKey, "");
				String newKey = UPlaceholder.PLACEHOLDER_PATTERN.matcher(cp).replaceFirst("");

				//pull old key
				String oldKey = p.getAttribute("key");
				pl("\t"+oldKey +" -> "+newKey);

				//check if old key exists
				if (tryDoesObjectExist(bucketName, oldKey) == false) throw new IOException ("Failed to update key "+oldKey+" to "+newKey+". Original key doesn't exist.");

				//check storage class, may need to trigger a pull from glacier to S3.
				ObjectMetadata omd = tryGetObjectMetadata(bucketName, oldKey);

				boolean ready = true;
				String sc = omd.getStorageClass();
				if ( sc != null) ready = requestArchiveRestore(oldKey);
				if (ready == false) continue;

				//check if new key already exists
				if (tryDoesObjectExist(bucketName, newKey)) throw new IOException ("Failed to update key "+oldKey+" to "+newKey+". It already exists.");

				//copy object to new location
				tryCopyObject(oldKey, newKey);

				//delete the original reference
				tryDeleteObject(bucketName, oldKey);

				//fetch meta data on new object
				ObjectMetadata newOmd = tryGetObjectMetadata(bucketName, newKey);
				
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
	
	/**Attempts 's3.deleteObject(bucketName, key)' maxTries before throwing error message*/
	private void tryDeleteObject(String bucketName, String key) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				s3.deleteObject(bucketName, key);
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
	
	/**Attempts 's3.doesObjectExist(bucketName, key)' maxTries before throwing error message*/
	private ObjectMetadata tryGetObjectMetadata(String bucketName, String key) throws IOException {		
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				return s3.getObjectMetadata(bucketName, key);
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

	/**Attempts 's3.doesObjectExist(bucketName, key)' maxTries before throwing error message*/
	private boolean tryDoesObjectExist(String bucketName, String key) throws IOException {		
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				return s3.doesObjectExist(bucketName, key);
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 's3.doesObjectExist(bucketName, key)' trying again, "+attempt);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 's3.doesObjectExist(bucketName, key)' trying again, "+attempt);
			};
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR looking for "+key+" in "+bucketName+" S3 error message:\n"+error);
	}
	
	private void sleep(String message) {
		try {
			pl(message+", sleeping "+minToWait+" minutes");
			TimeUnit.MINUTES.sleep(minToWait);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**Attempts 'tm.copy(bucketName, sourceObjectKey, bucketName, destObjectKey)' maxTries before throwing error message*/
	private void tryCopyObject(String sourceObjectKey, String destObjectKey) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).withMultipartCopyThreshold((long) (256 * 1024 * 1024)).build();
				Copy copy = tm.copy(bucketName, sourceObjectKey, bucketName, destObjectKey);
				copy.waitForCompletion();
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 'tm.copy(bucketName, sourceObjectKey, bucketName, destObjectKey)' trying again, "+attempt);
			}
			catch ( InterruptedException ie) {
				error = Util.getStackTrace(ie);
				sleep("\tWARNING: failed 'tm.copy(bucketName, sourceObjectKey, bucketName, destObjectKey)' trying again, "+attempt);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 'tm.copy(bucketName, sourceObjectKey, bucketName, destObjectKey)' trying again, "+attempt);
			};
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed tm.copy("+bucketName+", "+sourceObjectKey+", "+bucketName+", "+destObjectKey+") S3 error message:\n"+error);
	}


	private void delete() throws Exception{
		pl("\nDeleting S3 Objects, their delete placeholders, and any matching local files...");
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		for (UPlaceholder p : deletePlaceholders) {
			pl("\t"+p.getAttribute("key")+"\t"+p.getPlaceHolderFile()+"\t"+p.getLocalFile()+"\t"+p.getStorageClass());
			tryDeleteObject(bucketName, p.getAttribute("key"));
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

			for (UPlaceholder p : restorePlaceholders) {
				String key = p.getAttribute("key");
				p("\t"+key+"\t"+p.getPlaceHolderFile()+"\t"+p.getStorageClass()+"\t");

				//check storage class, may need to trigger a pull from glacier to S3.
				boolean download = true;
				if (p.getStorageClass().equals("STANDARD") == false) download = requestArchiveRestore(key);

				//good to go?
				if (download) {
					long startTime = System.currentTimeMillis();
					localFile = p.getLocalFile();
					tempFile = new File(localFile.getParentFile(), "tempRestore_"+localFile.getName());

					tryDownload(bucketName, key, tempFile, tm);

					//check the size
					long placeholderSize = Long.parseLong(p.getAttribute("size"));
					if (placeholderSize != tempFile.length()) throw new IOException("The restored file's size ("+tempFile.length()+"does not match the placeholder size\n"+p.getMinimalInfo());

					//rename the temp to local
					tempFile.renameTo(localFile);

					//rename the placeholder file
					File stdPlaceholder = new File (localFile.getCanonicalPath()+UPlaceholder.PLACEHOLDER_EXTENSION);
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
	
	/**Attempts 'tm.download(bucketName, key)' maxTries before throwing error message*/
	private void tryDownload(String bucketName, String key, File tempFile, TransferManager tm) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				Download xfer = tm.download(bucketName, key, tempFile);
				xfer.waitForCompletion();
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt);
			}
			catch ( InterruptedException ie) {
				error = Util.getStackTrace(ie);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 'tm.download(bucketName, key)' trying again, "+attempt);
			};
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed tm.download("+bucketName+", "+key+") S3 error message:\n"+error);
	}


	public boolean requestArchiveRestore(String key) throws IOException {
		
		//check if restore in progress
		ObjectMetadata response = tryGetObjectMetadata(bucketName, key);
		Boolean restoreFlag = response.getOngoingRestore();
		//request never received
		if (restoreFlag == null) {
			tryRestoreObjectV2(bucketName, key);
			pl("\t\t"+response.getStorageClass()+" restore request placed, relaunch GSync in a few hours.");
		}
		//true, in progress
		else if (restoreFlag == true) pl("\t\t"+response.getStorageClass()+" restore in progress, relaunch GSync in a few hours.");
		//false, ready for download
		else return true;

		runAgain = true;
		return false;
	}
	
	/**Attempts 'ts3.restoreObjectV2()' maxTries before throwing error message*/
	private void tryRestoreObjectV2(String bucketName, String key) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				RestoreObjectRequest requestRestore = new RestoreObjectRequest(bucketName, key, DAYS_IN_S3);
				s3.restoreObjectV2(requestRestore);
				return;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 'tm.copy(bucketName, sourceObjectKey, bucketName, destObjectKey)' trying again, "+attempt);
			}
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed send a s3.restoreObjectV2 ("+bucketName+", "+key+") S3 error message:\n"+error);
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
	public USync () {}

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
		UPlaceholder uPlaceholder = new UPlaceholder();
		HashMap<String, String> att = uPlaceholder.getAttributes();
		att.put("bucket", bucketName);
		att.put("key", f.getCanonicalPath().replaceFirst(deleteFromKey, ""));
		att.put("etag", etag);
		att.put("size", new Long(f.length()).toString());
		File p = new File(f.getCanonicalPath()+UPlaceholder.PLACEHOLDER_EXTENSION);
		uPlaceholder.writePlaceholder(p);
	}

	private File writePlaceholder(File f, S3ObjectSummary os) throws IOException {
		UPlaceholder uPlaceholder = new UPlaceholder();
		HashMap<String, String> att = uPlaceholder.getAttributes();
		att.put("bucket", bucketName);
		att.put("key", os.getKey());
		att.put("etag", os.getETag());
		att.put("size", new Long(os.getSize()).toString());
		File p = new File(f.getCanonicalPath()+UPlaceholder.PLACEHOLDER_EXTENSION);
		uPlaceholder.writePlaceholder(p);
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

	private String upload(String key, File file, TransferManager tm, String prePend) throws IOException {
		long startTime = System.currentTimeMillis();
		
		p("\t"+prePend+"\t"+bucketName+"/"+key+"\t");
		String eTag = tryUpload(bucketName, key, file, tm);
		
		//calc time
		double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
		pl(Util.formatNumber(diffTime, 1)+" min");

		return eTag;
	}
	
	/**Attempts 'tm.upload(bucketName, key, file)' maxTries before throwing error message*/
	private String tryUpload(String bucketName, String key, File file, TransferManager tm) throws IOException {	
		int attempt = 0;
		String error = null;
		while (attempt++ < maxTries) {
			try {
				Upload u = tm.upload(bucketName, key, file);
				u.waitForCompletion();
				String etag = null;
		        if (u.getState().equals(Transfer.TransferState.Completed)) {
		        	etag = u.waitForUploadResult().getETag();
		        }
		        if (etag == null) throw new IOException("ERROR failed tm.upload("+bucketName+", "+key+", "+file+")");
		        return etag;
			} catch (AmazonServiceException ase) {
				error = Util.getStackTrace(ase);
				sleep("\tWARNING: failed 'tm.upload(bucketName, key, file)' trying again, "+attempt);
			}
			catch ( InterruptedException ie) {
				error = Util.getStackTrace(ie);
				sleep("\tWARNING: failed 'tm.upload(bucketName, key, file)' trying again, "+attempt);
			}
			catch (SdkClientException sce) {
				error = Util.getStackTrace(sce);
				sleep("\tWARNING: failed 'tm.upload(bucketName, key, file)' trying again, "+attempt);
			};
		}
		//only hits this if all the attempts failed
		throw new IOException("ERROR failed tm.upload("+bucketName+", "+key+", "+file+") S3 error message:\n"+error);
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
					for (UPlaceholder p : restorePlaceholders) pl("\t"+p.getAttribute("key")+" \t "+p.getPlaceHolderFile());
				}
			}
			else if (verbose) pl("\nNo restore placeholder files were found.");

			//any deletes
			if (deletePlaceholders.size() != 0) {
				if (dryRun) {
					pl("\nThe following S3 Objects, their associated delete placeholder file, and any corresponding local file are ready for deletion.");
					for (UPlaceholder p : deletePlaceholders) pl("\t"+p.getAttribute("key")+"\t"+p.getPlaceHolderFile()+"\t"+p.getLocalFile());
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
				boolean dirsMade = dir.mkdirs();
				if (dirsMade == false) {
					pl("\tFailed to make the parent directory cannot remake the placeholder, create the parent dir and restart ' mkdir -pv "+ dir+ " '");
					return false;
				}
				
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
		for (UPlaceholder p: keyPlaceholders.values()) {
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
				if (p.getType().equals(UPlaceholder.TYPE_RESTORE)) {
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
				else if (p.getType().equals(UPlaceholder.TYPE_DELETE)) deleteType = true;
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
			for (UPlaceholder ph: failingPlaceholders) pl(ph.getMinimalInfo());
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

			//check placeholders
			if (keyPlaceholders.containsKey(key)) {
				//OK, a local placeholder file contains the aws key
				UPlaceholder p = keyPlaceholders.get(key);
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
				
				//check the path attribute
				GetObjectTaggingResult tags = s3.getObjectTagging(new GetObjectTaggingRequest(bucketName, key));
				List<Tag> tagSet= tags.getTagSet();
				//Iterate through the list
	            Iterator<Tag> tagIterator = tagSet.iterator();

	            String pathRecordedInS3 = null;
	            while(tagIterator.hasNext()) {
	                Tag tag = tagIterator.next();
	                if(tag.getKey().equals(UPlaceholder.PATH_NAME)) {
	                	pathRecordedInS3 = tag.getValue();
	                	break;
	                }
	                
	            }
	            
	            
	            here
				
			}
			
			//OK, this is an unknown s3 object with no local reference
			else if (os.getSize() != 0) s3KeyWithNoLocal.put(key, os);
		});
		s3.shutdown();
	}

	/**Check the placeholders for issues.*/
	private void parsePlaceholderFiles() throws IOException {
		pl("\nParsing local placeholder files...");
		keyPlaceholders = new HashMap<String, UPlaceholder>();
		ArrayList<UPlaceholder> failingPlaceholders = new ArrayList<UPlaceholder>();

		for (File f: placeholderFiles) {
			UPlaceholder p = new UPlaceholder(f, this);
			String key = p.getAttribute("key");

			//duplicate placeholders?
			if (keyPlaceholders.containsKey(key)) {
				p.addErrorMessage("Multiple placeholder files found with the same key '"+key+"'. Can only have one. Delete\n\t"+f+"\n\t\tor\n\t"+keyPlaceholders.get(key).getLocalFile() );
				failingPlaceholders.add(p);
			}
			else keyPlaceholders.put(key, p);

		}

		//any failing placeholders?
		if (failingPlaceholders.size() !=0) {
			pl("\tIssues were identified when parsing the placeholder files, address and restart:\n");
			for (UPlaceholder ph: failingPlaceholders) pl(ph.getMinimalInfo());
			resultsCheckOK = false;	
		}

	}

	private void scanLocalDir() throws IOException {
		pl("\nScanning local directory...");
		scanDirectory(directoryToScan);
		if (verbose) pl("\t"+candidatesForUpload.size()+" candidate files for upload");
	}

	/**Fetches all files over a min size, min age, and are not symbolic links. Saves placeholder files too.*/
	private void scanDirectory (File directory) throws IOException{ 
		//is the dir symlinked? skip it
		if (Files.isSymbolicLink(directory.toPath())) return;

		File[] list = directory.listFiles();
		for (int i=0; i< list.length; i++){
			if (list[i].isDirectory()) scanDirectory (list[i]);
			else {
				//is the file a symlink?
				if (Files.isSymbolicLink(list[i].toPath())) continue;

				//placeholder?
				String name = list[i].getName();
				if (name.endsWith(UPlaceholder.PLACEHOLDER_EXTENSION) || name.endsWith(UPlaceholder.RESTORE_PLACEHOLDER_EXTENSION)) placeholderFiles.add(list[i].getCanonicalFile());

				else {
					//size?
					double size = Util.gigaBytes(list[i]);
					if (size >= minGigaBytes) {
						//age in days?
						int age = Util.ageOfFileInDays(list[i]);
						if (age >= minDaysOld) {
							//exclude it?
							boolean saveIt = true;
							for (String ext: fileExtensionsToExclude) {
								if (name.endsWith(ext)) {
									saveIt = false;
									break;
								}
							}
							if (saveIt) candidatesForUpload.add(list[i].getCanonicalFile());
						}
					}
				}
			}
		}	
	}

	public static void main(String[] args) {
		if (args.length ==0){
			new USync().printDocs();
			System.exit(0);
		}
		new USync(args);
	}	

	/**This method will process each argument and assign new variables*/
	public void processArgs(String[] args) {
		try {
			Pattern pat = Pattern.compile("-[a-z]");
			pl("GSync Arguments: "+Util.stringArrayToString(args, " ")+"\n");
			for (int i = 0; i<args.length; i++){
				String lcArg = args[i].toLowerCase();
				Matcher mat = pat.matcher(lcArg);
				if (mat.matches()){
					char test = args[i].charAt(1);
					try{
						switch (test){
						case 'd': directoryToScan = new File (args[++i]).getCanonicalFile(); break;
						case 'b': bucketName = args[++i]; break;
						case 'e': email = args[++i]; break;
						case 'm': smtpHost = args[++i]; break;
						case 'a': minDaysOld = Integer.parseInt(args[++i]); break;
						case 's': minGigaBytes = Double.parseDouble(args[++i]); break;
						case 'f': fileExtensionsToExclude = Util.COMMA.split(args[++i]); break;
						case 'r': dryRun = false; break;
						case 'q': verbose = true; break;
						case 'p': restoreDeletedPlaceholders = true; break;
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

			//dir to scan
			if (directoryToScan == null || directoryToScan.isDirectory() == false || directoryToScan.canWrite() == false) {
				el("\nError: a problem was found with -d, your local directory to scan for files to archive to S3. Does it exist? Is it a directory? Can you write to it?\n");
				System.exit(1);
			}

			//check bucket
			if (bucketName == null) {
				el("\nError: please provide the name of a dedicated USync AWS S3 bucket.\n");
				System.exit(1);
			}
			
			//calculate the deleteFromPath int to use in generating relative paths
pl("Checking del from path ");			
			deleteFromPath = directoryToScan.getParentFile().getCanonicalPath().length()+1;
pl(directoryToScan.toString());
pl(directoryToScan.toString().substring(deleteFromPath));


			printOptions();
System.exit(1);
			
		} catch (Exception e) {
			el("\nError processing arguments");
			el(Util.getStackTrace(e));
			System.exit(1);
		}	
	}

	private void printOptions() {
		pl("Options:");
		pl("  -d Local directory to scan      : "+ directoryToScan);
		pl("  -b S3 Bucket name               : "+ bucketName);
		pl("  -f File extensions to exclude   : "+ Util.stringArrayToString(fileExtensionsToExclude, ","));
		pl("  -a Min file age (days)          : "+ minDaysOld);
		pl("  -g Min file size (GB)           : "+ minGigaBytes);
		pl("  -r Dry run                      : "+ dryRun);
		pl("  -p Restore missing placeholders : "+ restoreDeletedPlaceholders);
		pl("  -q Verbose output               : "+ verbose);
		pl("  -e Email                        : "+ email);
		pl("  -m Smtp host                    : "+ smtpHost);
		pl("  -x Rerun till complete          : "+ rerunUntilComplete);
	}

	public void printDocs(){  
		pl("\n" +
				"**************************************************************************************\n" +
				"**                                   USync : June 2020                              **\n" +
				"**************************************************************************************\n" +
				"USync transfers files that exceed a given size and age to Amazon's S3 Object store.\n"+
				"Upon successful upload, USync replaces the original file with a local gzipped txt placeholder file named xxx"+UPlaceholder.RESTORE_PLACEHOLDER_EXTENSION+
				"To restore S3 archived files, rename the placeholder files to xxx.S3.restore "+
				"To delete an S3 archived file, delete the local placeholder file. Placeholder files may be moved and renamed. "+
				"Changes to placeholder files with execute with the next USync run. Symbolic links are ignored.\n"+

				"\nTo use the app:\n"+ 
				"1) Create a S3 bucket dedicated solely for USync. Use it for nothing else.\n"+
				"2) Add lifecycle rules to AbortIncompleteMultipartUploads and if desired, move objects to Deep Glacier immediately. \n"+
				"3) Create a ~/.aws/credentials file with your access, secret, and region info, chmod\n"+
				"   600 the file and keep it private. Use a txt editor or the aws cli configure\n"+
				"   command, see https://aws.amazon.com/cli   Example ~/.aws/credentials file:\n"+
				"   [default]\n"+
				"   aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A\n"+
				"   aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ\n"+
				"   region = us-west-2\n"+
				"4) Execute USync to upload large old files to S3 and replace them with a placeholder\n"+
				"   file named xxx"+UPlaceholder.PLACEHOLDER_EXTENSION+"\n"+
				"5) To download and restore an archived file, rename the placeholder\n"+
				"   xxx"+UPlaceholder.RESTORE_PLACEHOLDER_EXTENSION+ " and run USync. If the S3 object is in Glacier, a restore to S3 request is placed, "
						+ "subsequent USync runs will download it when it becomes available\n"+  
				"6) To delete an S3 archived file, delete the placeholder, run USeq, and follow the AWS CLI instructions. \n"+
				"7) Placeholder files may be moved or renamed so long as the xxx"+UPlaceholder.PLACEHOLDER_EXTENSION+ " is preserved.\n"+

				"\nRequired:\n"+
				"-d A local directory to sync.\n"+
				"-b A dedicated S3 bucket name used exclusively by USync.\n"+

				"\nOptional:\n" +
				"-r Perform a real run, defaults to just listing the actions that would be taken.\n"+
				"-f File extensions (comma delimited, no spaces, case sensitive) to exclude from archiving, defaults to '.txt,.log'  \n" +
				"-a Minimum file age in days for archiving, defaults to 120\n"+
				"-s Minimum file size in gigabytes for archiving, defaults to 1\n"+
	
				"-p Recreate deleted placeholder files from orphaned S3 Objects, defaults to listing them for AWS CLI deletion.\n"+
				"-q Quiet verbose output.\n"+
				"-e Email addresses to send usync messages, comma delimited, no spaces.\n"+
				"-m Email smtp host, defaults to hci-mail.hci.utah.edu\n"+
				"-x Execute every 6 hrs until complete, defaults to just once, good for downloading\n"+
				"    latent glacier objects.\n"+

				"\nExample: java -Xmx20G -jar pathTo/USync_X.X.jar -d /Repo/Groups/WelmALab -b hcigroups_welmalab \n"+
				"     -a 90 -g 5 -r -e obama@real.gov -x -f .txt,.log,.xlxs \n\n"+

				"**************************************************************************************\n");

	}

	public HashMap<String, File> getCandidatesForUpload() {
		return candidatesForUpload;
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
	public HashMap<String, UPlaceholder> getPlaceholders() {
		return keyPlaceholders;
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
	public ArrayList<UPlaceholder> getFailingPlaceholders() {
		return failingPlaceholders;
	}

}
