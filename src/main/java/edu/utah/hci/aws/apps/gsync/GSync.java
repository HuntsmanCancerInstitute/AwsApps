package edu.utah.hci.aws.apps.gsync;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.utah.hci.aws.util.Util;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.iterable.S3Objects;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.RestoreObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
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
	private File localDir = null;
	private String bucketName = null;
	private int minDaysOld = 60;
	private double minGigaBytes = 5;
	private String[] fileExtensions = {".bam", ".cram",".gz", ".zip"};
	private boolean dryRun = true;
	private boolean verbose = false;
	private boolean deleteUploaded = false;
	private boolean updateS3Keys = false;
	//private boolean updateSymlinks = false;

	//internal fields
	private String deleteFromKey = null;
	private HashMap<String, File> candidatesForUpload = new HashMap<String, File>();
	
	private ArrayList<File> placeholderFiles = new ArrayList<File>();
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
	private ArrayList<String> s3KeyWithNoLocal = new ArrayList<String>();
	
	//ready for restore or delete
	private ArrayList<Placeholder> restorePlaceholders = new ArrayList<Placeholder>();
	private ArrayList<Placeholder> deletePlaceholders = new ArrayList<Placeholder>();

	private AmazonS3 s3 = null;

	public GSync (String[] args){
		try {
			long startTime = System.currentTimeMillis();
			
			processArgs(args);

			doWork();
			
			double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
			p("\nDone! "+Math.round(diffTime)+" minutes\n");

		} catch (Exception ex) {
			if (s3 != null) s3.shutdown();
			if (verbose) ex.printStackTrace();
			else e(ex.getMessage());
			System.exit(1);
		} finally {
			
		}
	}
	
	
	void doWork() throws Exception{
		
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

		s3.shutdown();
		
	}
	
	/**Attempts to update the S3 keys to match the current placeholder file
	 * @throws IOException 
	 * @return null if no problems or error statement.*/
	private String updateKeys() throws IOException {
		String error = null;
		Placeholder working = null;
		try {
			p("\nUpdating S3 keys to match local placeholder paths...");

			String region = Util.getRegionFromCredentials();
			s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();

			//for each key needing to be updated
			for (Placeholder p: keyPlaceholderToUpdate.values()) {
				working = p;
				//create new key
				String cp = p.getPlaceHolderFile().getCanonicalPath().replaceFirst(deleteFromKey, "");
				String newKey = Placeholder.PLACEHOLDER_PATTERN.matcher(cp).replaceFirst("");

				//pull old key
				String oldKey = p.getAttribute("key");
				if (verbose) p("\t"+oldKey +" -> "+newKey);

				//check if old key exists
				if (s3.doesObjectExist(bucketName, oldKey) == false) throw new IOException ("Failed to update key "+oldKey+" to "+newKey+". Original key doesn't exist.");

				//check if new key already exists
				if (s3.doesObjectExist(bucketName, newKey)) throw new IOException ("Failed to update key "+oldKey+" to "+newKey+". It already exists.");

				//copy object to new location
				copyObject(oldKey, newKey);
				
				//delete the original reference
				s3.deleteObject(bucketName, oldKey);
				
				//update placeholder file
				p.getAttributes().put("key", newKey);
				p.writePlaceholder(p.getPlaceHolderFile());
			}
			if (verbose) p("\tAll keys sucessfully updated. Ready to relaunch GSync.");
			s3.shutdown();

		} catch (Exception e) {
			error = "ERROR in updating keys for:\n"+working.getMinimalInfo()+"\n"+e.getMessage();
			e(error);
			if (verbose) e.printStackTrace();
		} 

		return error;
	}


	private void copyObject(String sourceObjectKey, String destObjectKey) throws IOException{
		
		//how big is it? If over 5GB must use multipart
        GetObjectMetadataRequest metadataRequest = new GetObjectMetadataRequest(bucketName, sourceObjectKey);
        ObjectMetadata metadataResult = s3.getObjectMetadata(metadataRequest);
        long objectSize = metadataResult.getContentLength();
		
		if (objectSize < 5368709120l) s3.copyObject(bucketName, sourceObjectKey, bucketName, destObjectKey);
		
		else {
			// Initiate the multipart upload.
            InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, destObjectKey);
            InitiateMultipartUploadResult initResult = s3.initiateMultipartUpload(initRequest);
            // Copy the object using 5 MB parts.
            long partSize = 5 * 1024 * 1024;
            long bytePosition = 0;
            int partNum = 1;
            List<CopyPartResult> copyResponses = new ArrayList<CopyPartResult>();
            while (bytePosition < objectSize) {
                // The last part might be smaller than partSize, so check to make sure
                // that lastByte isn't beyond the end of the object.
                long lastByte = Math.min(bytePosition + partSize - 1, objectSize - 1);

                // Copy this part.
                CopyPartRequest copyRequest = new CopyPartRequest()
                        .withSourceBucketName(bucketName)
                        .withSourceKey(sourceObjectKey)
                        .withDestinationBucketName(bucketName)
                        .withDestinationKey(destObjectKey)
                        .withUploadId(initResult.getUploadId())
                        .withFirstByte(bytePosition)
                        .withLastByte(lastByte)
                        .withPartNumber(partNum++);
                copyResponses.add(s3.copyPart(copyRequest));
                bytePosition += partSize;
            }

            // Complete the upload request to concatenate all uploaded parts and make the copied object available.
            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
            		bucketName,
                    destObjectKey,
                    initResult.getUploadId(),
                    getETags(copyResponses));
            s3.completeMultipartUpload(completeRequest);
		}
	}
	
    // This is a helper function to construct a list of ETags.
    private static List<PartETag> getETags(List<CopyPartResult> responses) {
        List<PartETag> etags = new ArrayList<PartETag>();
        for (CopyPartResult response : responses) {
            etags.add(new PartETag(response.getPartNumber(), response.getETag()));
        }
        return etags;
    }


	/*Assumes the Path is a symlink,  if broken, looks for the associated placeholder file,
	 * if present, creates a new symlink pointed at the placeholder and deletes the original symlink.
	 * Only works with full paths, not relative paths.
	 * 
	 * 
	 * Causing major problems! disable for now.
	 * 
	 * 
	public void updateSymlink(Path symlink) throws IOException {

		Path newSymlink = null;	
		try {
			//is it a placeholder?
			if (symlink.toString().endsWith(Placeholder.PLACEHOLDER_EXTENSION)) {
				Path target = Files.readSymbolicLink(symlink);

				//does the target directory contain a delete or restore placeholder?  If so do nothing. Wait for gsync to delete or restore the primary
				Path restore = Paths.get(target.toString().replaceFirst(Placeholder.PLACEHOLDER_EXTENSION, Placeholder.RESTORE_PLACEHOLDER_EXTENSION));
				Path delete = Paths.get(target.toString().replaceFirst(Placeholder.PLACEHOLDER_EXTENSION, Placeholder.RESTORE_PLACEHOLDER_EXTENSION));

				if (Files.exists(restore)==false && Files.exists(delete)==false) {
					//has the primary file been restored?
					String primary = target.toString().replaceFirst(Placeholder.PLACEHOLDER_EXTENSION, "");
					File restoredFile = new File(primary).getCanonicalFile();
					if (restoredFile.exists()) {
						Path newLink = Paths.get(symlink.toString().replaceFirst(Placeholder.PLACEHOLDER_EXTENSION, ""));
						if (dryRun) {
							if (verbose) p("\tDryRun - Replace placeholder symlink "+symlink+" with symlink to restored file "+newLink);
						}
						else {
							if (verbose) p("\tReplacing placeholder symlink "+symlink+" with symlink to restored file "+newLink);
							//make new symlink
							Files.createSymbolicLink(newLink, restoredFile.toPath());
							//delete old
							Files.delete(symlink);
						}
					}
					else {
						if (dryRun) {
							if (verbose) p("\tDryRun - Delete broken symlink "+symlink);
						}
						else {
							if (verbose) p("\tDeleting broken symlink "+symlink);
							Files.delete(symlink);
						}
					}
				}
			}
			//symlink not a placeholder
			else {		
				//look for a placeholder xxx.S3.txt
				Path target = Files.readSymbolicLink(symlink);
				Path archivedTarget = Paths.get(target.toString()+Placeholder.PLACEHOLDER_EXTENSION);
				if (Files.exists(archivedTarget)) {
					//create new
					newSymlink = Paths.get(symlink.toString()+Placeholder.PLACEHOLDER_EXTENSION);
					if (dryRun) {
						if (verbose) p("\tDryRun - Placeholder exists! Create new symlink "+newSymlink+" and delete old "+symlink);
					}
					else {
						if (verbose) p("\tPlaceholder exists! Creating new symlink "+newSymlink+" and deleting old "+symlink);
						Files.createSymbolicLink(newSymlink, archivedTarget);
						Files.delete(symlink);
					}
				}
				//broken link?
				else if (Files.exists(target)==false){
					if (dryRun) {
						if (verbose) p("\tDelete broken symlink "+symlink);
					}
					else {
						if (verbose) p("\tDeleting broken symlink "+symlink);
						Files.delete(symlink);
					}
				}
			}
		}
		catch (IOException ioe) {
			//attempt to delete the new symlink
			if (newSymlink != null) {
				try {
					Files.deleteIfExists(newSymlink);
				} catch (IOException e) {}
			}
			if (verbose) ioe.printStackTrace();
			throw new IOException ("ERROR updating symlink '"+symlink+"'\n"+ioe.getMessage());
		}
	}*/
	
	private void delete() throws Exception{
		p("\nDeleting S3 Objects, their delete placeholders, and any matching local files...");
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		for (Placeholder p : deletePlaceholders) {
			p("\t"+p.getAttribute("key")+"\t"+p.getPlaceHolderFile()+"\t"+p.getLocalFile()+"\t"+p.getStorageClass());
			s3.deleteObject(bucketName, p.getAttribute("key"));
			p.getPlaceHolderFile().delete();
			if (p.getLocalFile() != null) p.getLocalFile().delete();
		}
		p("\t"+deletePlaceholders.size()+" AWS and local resources deleted (versioned S3 objects can still be recovered)");
		s3.shutdown();
	}
	
	private String restore() {
		p("\nRestoring "+restorePlaceholders.size()+" S3 Objects and renaming their restore placeholders to standard...");
		FileOutputStream fos = null;
		S3ObjectInputStream s3is = null;
		File localFile = null;
		File tempFile = null;
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		int numRestored = 0;
		try {
			for (Placeholder p : restorePlaceholders) {
				String key = p.getAttribute("key");
				p("\t"+key+"\t"+p.getPlaceHolderFile()+"\t"+p.getStorageClass());

				//check storage class, may need to trigger a pull from glacier to S3.
				boolean download = true;
				if (p.getStorageClass().equals("STANDARD") == false) download = requestArchiveRestore(key, s3);

				//good to go?
				if (download) {
					S3Object o = s3.getObject(bucketName, key);
					s3is = o.getObjectContent();
					localFile = p.getLocalFile();
					tempFile = new File(localFile.getParentFile(), "tempRestore_"+localFile.getName());
					fos = new FileOutputStream(tempFile);
					byte[] read_buf = new byte[1024];
					int read_len = 0;
					while ((read_len = s3is.read(read_buf)) > 0) fos.write(read_buf, 0, read_len);
					s3is.close();
					fos.close();

					//check the size
					long placeholderSize = Long.parseLong(p.getAttribute("size"));
					if (placeholderSize != tempFile.length()) throw new IOException("The restored file's size ("+tempFile.length()+"does not match the placeholder size\n"+p.getMinimalInfo());

					//rename the temp to local
					tempFile.renameTo(localFile);
					
					//rename the placeholder file
					File stdPlaceholder = new File (localFile.getCanonicalPath()+Placeholder.PLACEHOLDER_EXTENSION);
					p.getPlaceHolderFile().renameTo(stdPlaceholder);
					numRestored++;
					
				}
			}
		} catch (Exception e) {
			//delete the temp and local
			if (localFile != null) localFile.delete();
			if (tempFile != null) tempFile.delete();

			//close the IO
			try {
				if (s3is != null)s3is.close();
				if (fos != null)fos.close();
				s3.shutdown();
			} catch (IOException ex) {}

			//return the error
			if (verbose) e.printStackTrace();
			String m = e.getMessage();
			if (m.contains("SSL peer shut down incorrectly")) m = "AWS rejected download request, try again later. AWS message '"+m+"'";
			return "\nRESTORE ERROR: "+m;
		} 
		s3.shutdown();
		p("\t"+numRestored+" resources restored");
		return "";
	}
	
	public boolean requestArchiveRestore(String key, AmazonS3 s3Client) throws IOException {
		//check if restore in progress
		ObjectMetadata response = s3Client.getObjectMetadata(bucketName, key);
        Boolean restoreFlag = response.getOngoingRestore();
        //request never received
        if (restoreFlag == null) {
        	p("\t\t"+response.getStorageClass()+" restore request placed, relaunch GSync in a few hours to download.");
            // Create and submit a request to restore an object from Glacier to S3 for xxx days.
           RestoreObjectRequest requestRestore = new RestoreObjectRequest(bucketName, key, DAYS_IN_S3);
           s3Client.restoreObjectV2(requestRestore);
        }
        //true, in progress
        else if (restoreFlag == true) p("\t\t"+response.getStorageClass()+" restore in progress, relaunch GSync in a few hours to download.");
        //false, ready for download
        else return true;
        return false;
    }


	private void deleteAlreadyUploaded() throws IOException {
		if (localFileAlreadyUploaded.size() != 0) {
			p("\nThe following local files have already been successfully uploaded to S3, have a correct placeholder, and are now deleted.");
			for (File f: localFileAlreadyUploaded) {
				p("\t"+f.getCanonicalPath());
				f.delete();
			}
		}
		localFileAlreadyUploaded.clear();
	}


	/**For unit testing.*/
	public GSync () {}

	private void upload() throws AmazonServiceException, AmazonClientException, IOException, InterruptedException {
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).build();
		
		//anything to upload?  all of these methods throw an IOException 
		if (candidatesForUpload.size() !=0) {
			int numUpload = candidatesForUpload.size();
			p("\nUploading "+numUpload+ " files...");
			ArrayList<File> toDelete = new ArrayList<File>();
			int counter = 1;
			
			for (String key: candidatesForUpload.keySet()) {
				File toUpload = candidatesForUpload.get(key);
				String etagData = upload(key, toUpload, tm, counter+"/"+numUpload);
				toDelete.add(toUpload);
				writePlaceholder(toUpload, etagData);
				counter++;
			}
			//cannot reach this point if there was an error
			p("\tAll S3 uploads successfully completed.");
			//delete
			if (deleteUploaded) {
				for (File x: toDelete) {
					if (verbose) p("\tDeleting "+x.getCanonicalPath());
					x.delete();
				}
			}
			else {
				p("\nThese files can be safely deleted:");
				for (File x: toDelete) p("\trm -f "+x.getCanonicalPath());
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
		
		System.out.print("\t"+prePend+"\t"+bucketName+"/"+key+"\t");
		Upload u = tm.upload(bucketName, key, file);
		//Util.showTransferProgress(u);
	    if (Util.waitForCompletion(u) == false) throw new IOException("Failed S3 upload "+file);
	    
	    //calc time
	    double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
		p(Util.formatNumber(diffTime, 1)+" min");
		
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
		p("\nGSync status...");
		
		//local files, only process here if not deleting
		if (deleteUploaded == false) {
			if (localFileAlreadyUploaded.size() != 0) {
				p("\nThe following local files have already been successfully uploaded to S3, have a correct placeholder, and are ready for deletion.");
				for (File f: localFileAlreadyUploaded) p("\trm -f "+f.getCanonicalPath());
			}
			else if (verbose) p("\nNo local files were found that have already been uploaded.");
		}
		
		//local files that have a presence in s3 but no placeholder
		if (localFileAlreadyUploadedNoPlaceholder.size() != 0) {
			p("\nThe following local files have a counterpart in S3 but no placeholder file. Major discrepancy, resolve ASAP. Partial upload? Delete S3 objects to initiate a new upload?");
			for (File f: localFileAlreadyUploadedNoPlaceholder) p("\t"+f.getCanonicalPath());
			resultsCheckOK = false;
		}
		else if (verbose) p("\nNo local files were found that had an S3 counterpart and were missing placeholder files.");
		
		//local files that diff in size with their counterpart in s3
		if (localFileAlreadyUploadedButDiffSize.size() != 0) {
			p("\nThe following local files have a counterpart in S3 that differs in size or etag. Major discrepancy, resolve ASAP. Partial upload? Delete S3 object?");
			for (File f: localFileAlreadyUploadedButDiffSize) p("\t"+f.getCanonicalPath());
			resultsCheckOK = false;
		}
		else if (verbose) p("\nNo local files were found that differed in size or etag with their S3 counterpart.");
		
		//s3 keys
		if (s3KeyWithNoLocal.size() != 0) {
			p("\nThe following are s3 objects with no local placeholder file. Was it deleted? Consider deleting the S3 Object?");
			for (String f: s3KeyWithNoLocal) p("\t"+f);
			resultsCheckOK = false;
		}
		else if (verbose) p("\nNo S3 objects were found that lacked local placeholder references.");
		
		if (resultsCheckOK) {
			
			//ready for upload
			if (candidatesForUpload.size() !=0) {
				if (dryRun) {
					p("\nThe following local files meet criteria and are ready for upload. Restart with the -r option.");
					for (String f: candidatesForUpload.keySet()) p("\t/"+f);
				}
			}
			else if (verbose) p("\nNo local files were found that are ready for upload.");
			
			//any restores
			if (restorePlaceholders.size() != 0) {
				if (dryRun) {
					p("\nThe following S3 Objects are ready for download and will replace their associated restore placeholder files.");
					for (Placeholder p : restorePlaceholders) p("\t"+p.getAttribute("key")+" \t "+p.getPlaceHolderFile());
				}
			}
			else if (verbose) p("\nNo restore placeholder files were found.");
			
			//any deletes
			if (deletePlaceholders.size() != 0) {
				if (dryRun) {
					p("\nThe following S3 Objects, their associated delete placeholder file, and any corresponding local file are ready for deletion.");
					for (Placeholder p : deletePlaceholders) p("\t"+p.getAttribute("key")+"\t"+p.getPlaceHolderFile()+"\t"+p.getLocalFile());
				}
			}
			else if (verbose) p("\nNo delete placeholder files were found.");
			
			
			//all synced?
			if (candidatesForUpload.size() == 0 && localFileAlreadyUploaded.size() == 0 && restorePlaceholders.size() == 0 && deletePlaceholders.size() == 0) p("\nAll synced, nothing to do.");
		}
		
		
		
	}
	



	private void checkPlaceholders() throws IOException {
		
		p("\nChecking placeholder files against S3 objects...");
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
						if (pSize == local.length()) p.addErrorMessage("The local file already exists. Size matches. File looks as if it is already restored? Delete the restore placeholder?\n\t\trm -f "+ p.getPlaceHolderFile());
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
			p("\tIssues were identified when parsing the placeholder files, address and restart:\n");
			for (Placeholder ph: failingPlaceholders) p(ph.getMinimalInfo());
			resultsCheckOK = false;
		}
	}

	private static void p(String s) {
		System.out.println(s);
	}
	private static void e(String s) {
		System.err.println(s);
	}
	
	private void scanBucket() throws IOException {
		
		p("\nScanning S3 bucket...");
		region = Util.getRegionFromCredentials();
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
        		s3KeyWithNoLocal.add(key);
        	}
		});
        s3.shutdown();
	}

	/**Check the placeholders for issues.*/
	private void parsePlaceholderFiles() throws IOException {
		p("\nChecking local placeholder files...");
		placeholders = new HashMap<String, Placeholder>();
		for (File f: placeholderFiles) {
			Placeholder p = new Placeholder(f,deleteFromKey);
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
				p("\tIssues were identified when parsing the placeholder files, address and restart:\n");
				for (Placeholder ph: failingPlaceholders) p(ph.getMinimalInfo());
			}
			//otherwise don't print the error message
			resultsCheckOK = false;
		}
		else if (verbose) p("\t"+placeholders.size()+" passing placeholder files");
	}

	void scanLocalDir() throws IOException {
		p("\nScanning local directory...");
		scanDirectory(localDir, fileExtensions);
		if (verbose) p("\t"+candidatesForUpload.size()+" candidate files with indexes for upload");
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
								if (verbose) p("\tAdding upload candidate "+ list[i].getCanonicalFile());
								//look for index file
								File index = findIndex(list[i]);
								if (index != null) {
									candidatesForUpload.put(index.getCanonicalPath().replaceFirst(deleteFromKey, ""), index.getCanonicalFile());
									if (verbose) p("\tAdding upload candidate index "+ index.getCanonicalPath());
								}
							}
						}
					}
				}
				else if (fileName.contains(Placeholder.PLACEHOLDER_EXTENSION) && symlink == false) {
					placeholderFiles.add(list[i].getCanonicalFile());
					if (verbose) p("\tPlaceholder found "+ list[i].getCanonicalPath());
				}
			}
		}
	}

	public static void main(String[] args) {
		if (args.length ==0){
			printDocs();
			System.exit(0);
		}
		new GSync(args);
	}	

	/**This method will process each argument and assign new variables
	 * @throws IOException */
	public void processArgs(String[] args) throws IOException{
		Pattern pat = Pattern.compile("-[a-z]");
		p("GSync Arguments: "+Util.stringArrayToString(args, " ")+"\n");
		for (int i = 0; i<args.length; i++){
			String lcArg = args[i].toLowerCase();
			Matcher mat = pat.matcher(lcArg);
			if (mat.matches()){
				char test = args[i].charAt(1);
				try{
					switch (test){
					case 'd': localDir = new File (args[++i]); break;
					case 'b': bucketName = args[++i]; break;
					case 'a': minDaysOld = Integer.parseInt(args[++i]); break;
					case 'g': minGigaBytes = Double.parseDouble(args[++i]); break;
					case 'e': fileExtensions = Util.COMMA.split(args[++i]); break;
					case 'r': dryRun = false; break;
					case 'v': verbose = true; break;
					//case 's': updateSymlinks = true; break;
					case 'u': updateS3Keys = true; break;
					case 'k': deleteUploaded = true; break;
					case 'h': printDocs(); System.exit(0);
					default: Util.printExit("\nProblem, unknown option! " + mat.group());
					}
				}
				catch (Exception e){
					Util.printExit("\nSorry, something doesn't look right with this parameter: -"+test+"\n");
				}
			}
		}

		//check params
		if (localDir == null || localDir.canRead() == false || localDir.isFile()) e("\nError: please provide a directory to sync with S3\n");
		if (bucketName == null) e("\nError: please provide the name of a dedicated GSync AWS S3 bucket.\n");

		localDir = localDir.getCanonicalFile();
		deleteFromKey = localDir.getParentFile().getCanonicalPath()+"/";

		if (dryRun) deleteUploaded = false;
		
		printOptions();
	}	

	private void printOptions() {
		p("Options:");
		p("  -d Local directory           : "+ localDir);
		p("  -b S3 Bucket name            : "+ bucketName);
		p("  -e File extensions           : "+ Util.stringArrayToString(fileExtensions, ","));
		p("  -a Min file days             : "+ minDaysOld);
		p("  -g Min file GB               : "+ minGigaBytes);
		p("  -r Dry run                   : "+ dryRun);
		//p("  -s Update symlinks           : "+ updateSymlinks);
		p("  -u Update S3 keys            : "+ updateS3Keys);
		p("  -v Verbose output            : "+ verbose);
		p("  -k Delete local after upload : "+ deleteUploaded);
	}

	public static void printDocs(){
		p("\n" +
				"**************************************************************************************\n" +
				"**                                  GSync : August 2019                             **\n" +
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
				"2) Enable S3 Object Versioning on the bucket to prevent accidental deletion and\n"+
				"   create an AbortIncompleteMultipartUpload lifecycle rule to delete partial uploads.\n"+
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
				"   Versioned copies on S3 will not be deleted. Use the S3 Console to do so.\n"+
				"7) Placeholder files may be moved, see -u\n"+ //; broken symlinks can be updated, see -s\n"+
				
				"\nRequired:\n"+
				"-d Path to a local directory to sync. This becomes the base key in S3, e.g. \n"+
				"     BucketName/LocalDirName/...\n"+
				"-b Dediated S3 bucket name\n"+

				"\nOptional:\n" +
				"-e File extensions to consider, comma delimited, no spaces, case sensitive. Defaults\n"+
				"     to '.bam,.cram,.gz,.zip'\n" +
				"-a Minimum days old for archiving, defaults to 60\n"+
				"-g Minimum gigabyte size for archiving, defaults to 5\n"+
				"-r Perform a real run, defaults to just listing the actions that would be taken\n"+
				"-k Delete local files that were successfully  uploaded.\n"+
				"-u Update S3 Object keys to match current placeholder paths.\n"+
				//"-s Update symbolic links that point to uploaded and deleted files. This replaces the\n"+
				//"     broken link with a new link named xxx"+Placeholder.PLACEHOLDER_EXTENSION+"\n"+
				"-v Verbose output\n"+

				"\nExample: java -Xmx20G -jar pathTo/USeq/Apps/GSync -d Repo/ -b hcibioinfo_gsync_repo \n"+
				"     -v -a 90 -g 1 \n\n"+

				"**************************************************************************************\n");

	}

	public void setLocalDir(File localDir) throws IOException {
		this.localDir = localDir;
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


	public ArrayList<File> getPlaceholderFiles() {
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


	public ArrayList<String> getS3KeyWithNoLocal() {
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


	//public void setUpdateSymlinks(boolean updateSymlinks) {
	//	this.updateSymlinks = updateSymlinks;
	//}
}
