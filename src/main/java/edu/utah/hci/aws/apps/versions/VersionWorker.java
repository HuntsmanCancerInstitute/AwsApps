package edu.utah.hci.aws.apps.versions;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import edu.utah.hci.aws.util.Util;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectVersionsResponse;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

public class VersionWorker implements Runnable {

	//internal fields
	private String threadName = null;
	private VersionManager vm = null;
	private String bucketName = null;
	private int minDaysOld = 30;
	private String[] suffixes = null;
	private String[] prefixes = null;
	private boolean dryRun = true;
	private boolean verbose = true;
	private LinkedHashMap<String, AwsKeyJobWorker> keyObj = new LinkedHashMap<String, AwsKeyJobWorker>();
	private S3Client s3 = null;
	private static final long[] secToWaitForRetrys = {3, 6, 30, 120, 600, 1800};
	private boolean failed = false;
	private boolean complete = false;
	
	//counters
	private int numberResponsesReceived = 0;
	private long bytesDeleted = 0;
	private long numberKeysScanned = 0;
	private long numberObjectsScanned = 0;
	private long numberObjectsDeleted = 0;
	private long numberMarksScanned = 0;
	private long numberMarksDeleted = 0;


	//constructor
	public VersionWorker(VersionManager vm, String threadName) {
		this.vm = vm;
		this.threadName = threadName;
		bucketName = vm.getBucketName();
		minDaysOld = vm.getMinDaysOld();
		suffixes = vm.getSuffixes();
		prefixes = vm.getPrefixes();
		dryRun = vm.isDryRun();
		verbose = vm.isVerbose();
		
		s3 = S3Client.builder()
				.credentialsProvider(vm.getCred())
				.region(Region.of(vm.getRegion()))
				.build();
	}
	
	public void run() {
		while (true) {
			
			ListObjectVersionsResponse response = vm.fetchNextJob();
			if (response != null) processResonse(response);
			else {
				//all done?
				if (vm.isLastJobAdded()) {
					s3.close();
					complete = true;
					return;
				} else
					try {
						//wait a second and check again
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
						failed = true;
					}
			}	
		}
		
	}

	private void processResonse(ListObjectVersionsResponse response) {
		try {
			numberResponsesReceived++;
			keyObj.clear();
			loadVersions(response);
			loadMarks(response);
			processKeys();
		} catch (Exception e) {
			vm.el("\nERROR with processing response from VersionWorker thread:\n"+e.getMessage()+"\n"+e.toString());
			failed = true;
			if (s3 != null) s3.close();
		}
	}


	private void loadVersions(ListObjectVersionsResponse response) {
		for (ObjectVersion version : response.versions()) {
			numberObjectsScanned++;
			String key = version.key();
			AwsKeyJobWorker awskey = keyObj.get(key);
			if (awskey == null) {
				awskey = new AwsKeyJobWorker();
				keyObj.put(key, awskey);
			}
			awskey.addVersion(version);
		}
	}


	private void loadMarks(ListObjectVersionsResponse response) {
		for (DeleteMarkerEntry marker : response.deleteMarkers()) {
			numberMarksScanned++;
			AwsKeyJobWorker key = keyObj.get(marker.key());
			if (key == null) {
				key = new AwsKeyJobWorker();
				keyObj.put(marker.key(), key);
			}
			key.addMarks(marker);
		}
	}

	private void processKeys() throws Exception {
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
			AwsKeyJobWorker ak = keyObj.get(key);
			if (message == null) ak.trimObjects(minDaysOld, this, key);
			else if (verbose) ak.writeAll(message);
		}
	}

	public void deleteObject(ObjectVersion ov, String message) throws Exception {
		numberObjectsDeleted++;
		bytesDeleted+= ov.size();
		if (verbose) Util.pl(AwsKeyJobWorker.objectToString(ov, message));

		//really delete it?
		if (dryRun == false) {
			String key = ov.key();
			String vId = ov.versionId();
			Exception e = null;
			for (int i=0; i< secToWaitForRetrys.length; i++) {
				e = deleteIt(key, vId);
				if (e == null) return;
				long msToWait = secToWaitForRetrys[i]* 1000l;
				vm.el("\nAWS object deletion issue in thread "+threadName+", waiting "+secToWaitForRetrys[i]+"sec then trying again.");
				Thread.sleep(msToWait);
			}
			throw e;
		}
	}

	public Exception deleteIt(String key, String versionId) {
		try {
			s3.deleteObject(DeleteObjectRequest.builder()
					.bucket(bucketName)
					.key(key)
					.versionId(versionId)
					.build());
			return null;
		}
		catch (Exception e) {
			return e;
		}
	}

	public void deleteMarkers(ArrayList<DeleteMarkerEntry> markers, String message) throws Exception {
		if (markers != null) {
			for (DeleteMarkerEntry m : markers) deleteMarker(m, message);
		}

	}

	private void deleteMarker(DeleteMarkerEntry m, String message) throws Exception {
		numberMarksDeleted++;
		if (verbose) Util.pl(AwsKeyJobWorker.markerToString(m, message));

		//really delete it?
		if (dryRun == false) {
			String key = m.key();
			String vId = m.versionId();
			Exception e = null;
			for (int i=0; i< secToWaitForRetrys.length; i++) {
				e = deleteIt(key, vId);
				if (e == null) return;
				long msToWait = secToWaitForRetrys[i]* 1000l;
				vm.el("\nAWS marker deletion issue in thread "+threadName+", waiting "+secToWaitForRetrys[i]+"sec then trying again.");
				Thread.sleep(msToWait);
			}
			throw e;
		}
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

	public long getBytesDeleted() {
		return bytesDeleted;
	}

	public boolean isVerbose() {
		return verbose;
	}

	public S3Client getS3() {
		return s3;
	}

	public String getThreadName() {
		return threadName;
	}

	public boolean isFailed() {
		return failed;
	}

	public boolean isComplete() {
		return complete;
	}

	public int getNumberResponsesReceived() {
		return numberResponsesReceived;
	}

}
