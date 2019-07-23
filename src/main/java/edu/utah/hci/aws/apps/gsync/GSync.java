package edu.utah.hci.aws.apps.gsync;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import edu.utah.hci.aws.util.Util;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
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
	private boolean debug = false;
	private boolean deleteUploaded = false;

	//internal fields
	private HashMap<String, File> candidatesForUpload = new HashMap<String, File>();
	
	private ArrayList<File> placeholderFiles = new ArrayList<File>();
	private ArrayList<Placeholder> failingPlaceholders = new ArrayList<Placeholder>();
	private HashMap<String, Placeholder> placeholders = null;
	
	private String region = null;
	private boolean resultsCheckOK = true;

	private ArrayList<File> localFileAlreadyUploaded = new ArrayList<File>();
	private ArrayList<File> localFileAlreadyUploadedButDiffSize = new ArrayList<File>();
	private ArrayList<File> localFileAlreadyUploadedNoPlaceholder = new ArrayList<File>();
	private ArrayList<String> s3KeyWithNoLocal = new ArrayList<String>();

	private AmazonS3 s3 = null;

	public GSync (String[] args){
		try {
			long startTime = System.currentTimeMillis();

			processArgs(args);

			doWork();
			
			double diffTime = ((double)(System.currentTimeMillis() -startTime))/60000;
			p("\nDone! "+Math.round(diffTime)+" minutes\n");

		} catch (Exception e) {
			if (verbose) e.printStackTrace();
			else p(e.getMessage());
			System.exit(1);
		} finally {
			
		}
	}
	
	
	void doWork() throws Exception{
		
		scanLocalDir();

		parsePlaceholderFiles();
		if (resultsCheckOK == false) return;

		scanBucket();
		
		checkPlaceholders();
		if (resultsCheckOK == false) return;
		
		removeLocalFromCandidates();
		
		//call after removeLocalFromCandidates()
		if (deleteUploaded) deleteAlreadyUploaded();
		
		printResults();

		if (dryRun == false && resultsCheckOK == true) execute();

		s3.shutdown();
		
	}

	private void deleteAlreadyUploaded() throws IOException {

		if (localFileAlreadyUploaded.size() != 0) {
			p("\nThe following local files have already been successfully uploaded to S3, have a correct placeholder, and are now deleted.");
			for (File f: localFileAlreadyUploaded) {
				p("\t"+f.getCanonicalPath());
				f.delete();
			}
		}
		else if (verbose) p("\nNo local files were found that have already been uploaded.");
		localFileAlreadyUploaded.clear();
	}


	/**For unit testing.*/
	public GSync () {
		verbose = true;
		debug = true;
	}

	private void execute() throws AmazonServiceException, AmazonClientException, IOException, InterruptedException {
		TransferManager tm = TransferManagerBuilder.standard().withS3Client(s3).build();
		
		//anything to upload?  all of these methods throw an IOException 
		if (candidatesForUpload.size() !=0) {
			p("\nUploading "+candidatesForUpload.size()+ " files...");
			ArrayList<File> toDelete = new ArrayList<File>();
			for (String f: candidatesForUpload.keySet()) {
				File toUpload = candidatesForUpload.get(f);
				String etagData = upload(toUpload, tm);
				toDelete.add(toUpload);
				writePlaceholder(toUpload, etagData);
			}
			//cannot reach this point if there was an error
			p("\tAll S3 uploads successfully completed.");
			//delete
			if (deleteUploaded) {
				for (File x: toDelete) {
					x.delete();
					if (debug  || verbose) p("\tDeleting "+x.getCanonicalPath());
				}
			}
			else {
				p("\nThese files can be safely deleted:");
				for (File x: toDelete) p("rm -f "+x.getCanonicalPath());
			}
		}
		else if (verbose) p("\nNo files to upload.");
		
		tm.shutdownNow();
		
	}

	private void writePlaceholder(File f, String etag) throws IOException {
		File p = new File(f.getCanonicalPath()+Placeholder.PLACEHOLDER_EXTENSION);
		String[] attributes = new String[] {"bucket = "+bucketName, "key = "+ f.getCanonicalPath().substring(1), "etag = "+etag, "size = "+f.length()};
		Util.write(attributes, p);
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

	private String upload(File file, TransferManager tm) throws AmazonServiceException, AmazonClientException, IOException, InterruptedException {
		p("\t"+bucketName+file.getCanonicalPath());
		Upload u = tm.upload(bucketName, file.getCanonicalPath().substring(1), file);
//Util.showTransferProgress(u);
	    if (Util.waitForCompletion(u) == false) throw new IOException("Failed S3 upload "+file);
	    return u.waitForUploadResult().getETag();
	}
	
	private void removeLocalFromCandidates() throws IOException {

		// remove localFileAlreadyUploaded from candidatesForUpload
		for (File f: localFileAlreadyUploaded) candidatesForUpload.remove(f.getCanonicalPath().substring(1));
		
		// remove localFileAlreadyUploadedButDiffSizeEtag from candidatesForUpload
		for (File f: localFileAlreadyUploadedButDiffSize) candidatesForUpload.remove(f.getCanonicalPath().substring(1));
		
		// remove localFileAlreadyUploadedNoPlaceholder from candidatesForUpload
		for (File f: localFileAlreadyUploadedNoPlaceholder) candidatesForUpload.remove(f.getCanonicalPath().substring(1));
		
	}

	private void printResults() throws IOException {
		p("\nGSync results...");
		
		//local files, only process here if not deleting
		if (deleteUploaded == false) {
			if (localFileAlreadyUploaded.size() != 0) {
				p("\nThe following local files have already been successfully uploaded to S3, have a correct placeholder, and are ready for deletion.");
				for (File f: localFileAlreadyUploaded) p("rm -f "+f.getCanonicalPath());
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
		
		//ready for upload
		if (resultsCheckOK) {
			if (candidatesForUpload.size() !=0) {
				if (dryRun) {
					p("\nThe following local files are ready for upload to S3 and replacement with a placeholder file.");
					for (String f: candidatesForUpload.keySet()) p("\t/"+f);
				}
			}
			else {
				if (verbose) p("\nNo local files were found that are ready for upload.");
				else if (localFileAlreadyUploaded.size() == 0) p("\tAll synced, nothing to do.");
			}
		}
		
		
		
	}
	



	private void checkPlaceholders() throws IOException {
		p("\nChecking placeholder files...");
		for (Placeholder p: placeholders.values()) {
			boolean ok = true;
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
					//does the restored file already exist?
					File local = new File("/"+p.getAttribute("key"));
					if (local.exists()) {
						ok = false;
						p.addErrorMessage("The local file already exists. Suspect a failed download.");
					}
				}
			}
			//save it
			if (ok) placeholders.put(p.getAttribute("key"), p);
			else failingPlaceholders.add(p);
		}
		
		
		//any failing paths?
		if (failingPlaceholders.size() !=0) {
			p("\tIssues were identified when parsing the placeholder files, address and restart.\n");
			for (Placeholder ph: failingPlaceholders) p(ph.getMinimalInfo());
			resultsCheckOK = false;
		}
		else if (verbose) {
			p("\nNo problematic placeholder files were found.");
			if (debug) {
				if (placeholders.size() != 0) {
					p("\tList of correct files:");
					for (Placeholder p : placeholders.values()) p("\t"+p.getPlaceHolderFile().getCanonicalPath());
				}
			}
		}
	}

	private static void p(String s) {
		System.out.println(s);
	}

	private void scanBucket() throws IOException {
		
		p("\nScanning S3 bucket ("+ bucketName+ ")...");
		
		region = Util.getRegionFromCredentials();
		s3 = AmazonS3ClientBuilder.standard().withRegion(region).build();
		
		ListObjectsV2Result result = s3.listObjectsV2(bucketName);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        p("\t"+objects.size()+ " S3 objects");
        if (debug) p("\tKey\tSize\tStorage");
        
        for (S3ObjectSummary os: objects) {
        	String key = os.getKey();
            if (debug) p("\t" + key+"\t"+os.getSize()+"\t"+os.getStorageClass());

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
        }
	}

	/**Check the placeholders for issues.*/
	private void parsePlaceholderFiles() throws IOException {
		p("\nParsing local placeholder files...");
		placeholders = new HashMap<String, Placeholder>();
		for (File f: placeholderFiles) {
			Placeholder p = new Placeholder(f);
			String key = p.getAttribute("key");
			
			//does the path match the key?
			if (p.isKeyMatchesLocalPlaceholderPath()) {
				
				//duplicate placeholders?
				if (placeholders.containsKey(key)) {
					p.addErrorMessage("Multiple placeholder files found with the same key. Can only have one std, restore, or delete placeholder at a time." );
					failingPlaceholders.add(p);
				}
				else placeholders.put(key, p);
				
			}
			else {
				p.addErrorMessage("The current file path does not match S3 key path. Was this placeholder file moved? If so move the S3 object to match and update the key attribute in this placeholder file" );
				failingPlaceholders.add(p);
			}
		}
		//any failing paths?
		if (failingPlaceholders.size() !=0) {
			p("\tIssues were identified when parsing the placeholder files, address and restart.\n");
			for (Placeholder ph: failingPlaceholders) p(ph.getMinimalInfo());
			resultsCheckOK = false;
		}
		else p("\t"+placeholders.size()+" placeholder files");
	}

	void scanLocalDir() throws IOException {
		p("\nScanning local directory for candidate files to upload...");

		scanDirectory(localDir, fileExtensions);

		p("\t"+candidatesForUpload.size()+" candidate upload files");
		

	}




	/**Fetches all files with a given extension, min size, min age, not symbolic links. Also save any that are aws uploaded placeholder files and restore placeholder files.
	 * Includes indexes for bam, cram, and tabix gz if present*/
	private void scanDirectory (File directory, String[] fileExtensions) throws IOException{ 
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
				boolean symlink = Files.isSymbolicLink(list[i].toPath());	
						
				if (match) {
					if (debug) p("  Checking "+list[i].getCanonicalPath());
					if (symlink) {
						if (debug)p("   is a symlink");
					}
					else {
						//size?
						double size = Util.gigaBytes(list[i]);
						if (size >= minGigaBytes) {

							//age in days?
							int age = Util.ageOfFileInDays(list[i]);
							if (age >= minDaysOld) {
								candidatesForUpload.put(list[i].getCanonicalPath().substring(1), list[i].getCanonicalFile());
								if (debug) p("   Adding "+list[i].getCanonicalPath());
								
								//look for index file
								File index = findIndex(list[i]);
								if (index != null) {
									candidatesForUpload.put(index.getCanonicalPath().substring(1), index.getCanonicalFile());
									if (debug) p("   Adding "+ index.getCanonicalPath());
								}
								else if (debug) p("   No index file for "+list[i]);
							}
							else if (debug) p("   Too recent "+age+" days");
						}
						else if (debug) p("   Too small "+Util.formatNumber(size, 3)+" gb");
					}

				}
				else if (fileName.contains(Placeholder.PLACEHOLDER_EXTENSION) && symlink == false) {
					placeholderFiles.add(list[i].getCanonicalFile());
					if (debug) p("  Placeholder "+ list[i].getCanonicalPath());
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
					case 'k': deleteUploaded = true; break;
					case 'x': debug = true; break;
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
		if (localDir == null || localDir.canRead() == false || localDir.isFile()) p("\nError: please provide a directory to sync with S3\n");
		//if (baseS3Url == null) p("\nError: please provide a root S3 URL containing a bucket in which to sync with your local directory\n");
		//if (awsCredentials == null) p("\nError: please provide a path to your aws credentials file\n");

		localDir = localDir.getCanonicalFile();

		if (dryRun) deleteUploaded = false;
		if (debug) verbose = true;
		
		printOptions();
	}	

	private void printOptions() {
		p("Options:");
		p("  -d Local directory  : "+ localDir);
		p("  -b S3 Bucket name   : "+ bucketName);
		p("  -e File extensions  : "+ Util.stringArrayToString(fileExtensions, ","));
		p("  -a Min file days    : "+ minDaysOld);
		p("  -g Min file GB      : "+ minGigaBytes);
		p("  -r Dry run          : "+ dryRun);
		p("  -v Verbose output   : "+ verbose);
		p("  -k Delete uploaded files : "+ deleteUploaded);
	}

	public static void printDocs(){
		p("\n" +
				"**************************************************************************************\n" +
				"**                                 GSync : July 2019                                **\n" +
				"**************************************************************************************\n" +
				"GSync pushes files with a particular extention that exceed a given size and age to \n" +
				"Amazon's S3 object store. Associated genomic index files are also moved. Once \n"+
				"correctly uploaded, GSync replaces the original file with a local txt placeholder file \n"+
				"containing information about the S3 object. Symbolic links are ignored.\n"+
				
				"\nWARNING! This app has the potential to destroy precious genomic data. TEST IT on a\n"+
				"pilot system before depolying in production. BACKUP your local files and ENABLE S3\n"+
				"Object Versioning before running.\n"+
				
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
				
				"\nRequired:\n"+
				"-d Path to a local directory to sync\n"+
				"-b Dediated S3 bucket name\n"+

				"\nOptional:\n" +
				"-e File extensions to consider, comma delimited, no spaces, case sensitive. Defaults\n"+
				"     to '.bam,.cram,.gz,.zip'\n" +
				"-a Minimum days old for archiving, defaults to 60\n"+
				"-g Minimum gigabyte size for archiving, defaults to 5\n"+
				"-r Perform a real run, defaults to just listing the actions that would be taken\n"+
				"-k Delete local files that were sucessfully uploaded, defaults to just printing\n"+
				"     'rm -r xxx' statements. Use with caution!\n"+
				"-v Verbose output. Use -x for very verbose output.\n"+

				"\nExample: java -Xmx20G -jar pathTo/USeq/Apps/GSync -d /Repo/ -b hcibioinfo_gsync_repo \n"+
				"     -v -a 90 -g 1 \n\n"+

				"**************************************************************************************\n");

	}

	public void setLocalDir(File localDir) {
		this.localDir = localDir;
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


	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public ArrayList<Placeholder> getFailingPlaceholders() {
		return failingPlaceholders;
	}
}
