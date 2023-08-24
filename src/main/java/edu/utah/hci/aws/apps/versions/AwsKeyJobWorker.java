package edu.utah.hci.aws.apps.versions;

import java.util.ArrayList;

import edu.utah.hci.aws.util.Util;
import software.amazon.awssdk.services.s3.model.DeleteMarkerEntry;
import software.amazon.awssdk.services.s3.model.ObjectVersion;

public class AwsKeyJobWorker {
	
	ArrayList<ObjectVersion> objects = null;
	ArrayList<DeleteMarkerEntry> markers = null;
	
	
	public void addVersion(ObjectVersion ov) {
		if (objects == null) objects = new ArrayList<ObjectVersion>();
		objects.add(ov);
	}

	public void addMarks(DeleteMarkerEntry ov) {
		if (markers == null) markers = new ArrayList<DeleteMarkerEntry>();
		markers.add(ov);
	}

	public void trimObjects(int minDaysOld, VersionWorker vm, String key) throws Exception {
		int num = 0;
		if (objects != null) num = objects.size();
		
		//one
		if (num == 1) {
			ObjectVersion ov = objects.get(0);
			//isLatest true then delete any markers and leave the ov alone
			if (ov.isLatest()) {
				if (vm.isVerbose()) Util.pl(AwsKeyJobWorker.objectToString(ov, "KEEP_IsLatest"));
				vm.deleteMarkers(markers, "DELETE_NoVersions");
				return;
			}
			
			//ok it isn't the latest, and it's just one, is it expired?
			double age = Util.daysOld(ov.lastModified());			
			if (age < minDaysOld) {
				if (vm.isVerbose()) {
					Util.pl(AwsKeyJobWorker.objectToString(ov, "KEEP_Young"));
					writeMarkers("KEEP_VersionsPresent");
				}
				return;
			}
			
			//delete it and all of the markers
			vm.deleteObject(ov, "DELETE_Expired");
			vm.deleteMarkers(markers, "DELETE_NoVersions");
		}
		
		//more than one
		else if (num > 1) {
			//pull latest out of AL
			ObjectVersion latest = extractLatest();
			if (latest != null && vm.isVerbose()) Util.pl(AwsKeyJobWorker.objectToString(latest, "KEEP_IsLatest"));
			
			//walk remainder, these are all deleted objects or copied over objects
			int numRem = objects.size();
			int numDel = 0;
			for (int i=0; i<numRem; i++) {
				ObjectVersion ov = objects.get(i);
				double age = Util.daysOld(ov.lastModified());
				if (age >= minDaysOld) {
					vm.deleteObject(ov, "DELETE_Expired");
					numDel++;
				}
				else if (vm.isVerbose()) Util.pl(AwsKeyJobWorker.objectToString(ov, "KEEP_Young"));
			}
			//is the num deleted == num hidden? if so delete all of the markers
			if (numDel == numRem) vm.deleteMarkers(markers, "DELETE_NoVersions");
			else writeMarkers("KEEP_VersionsPresent");
			
		}
		
		//must be zero, just check and delete any markers
		else {
			vm.deleteMarkers(markers, "DELETE_NoVersions");
		}
		
		
	}

	private ObjectVersion extractLatest() {
		int num = objects.size();
		for (int i=0; i<num; i++) {
			ObjectVersion ov = objects.get(i);
			if (ov.isLatest()) return objects.remove(i);
		}
		return null;
	}


	public void writeAll(String message) {
		//output objects
		if (objects != null) {
			int num = objects.size();
			for (int i=0; i<num; i++) Util.pl(objectToString(objects.get(i), message));
		}
		//output marks
		if (markers != null) {
			int num = markers.size();
			for (int i=0; i<num; i++) Util.pl(markerToString(markers.get(i), message));
		}
	}
	
	public void writeMarkers(String message) {
		//output marks
		if (markers != null) {
			int num = markers.size();
			for (int i=0; i<num; i++) Util.pl(markerToString(markers.get(i), message));
		}
	}
	
	/*KeyName Type Message VersionID isLatest DaysOld GBSize*/
	public static String markerToString(DeleteMarkerEntry ov, String message) { 
		StringBuilder sb = new StringBuilder();
		sb.append(ov.key()); sb.append("\t");
		sb.append("Mrk"); sb.append("\t");
		sb.append(message); sb.append("\t");
		sb.append(ov.versionId()); sb.append("\t");
		sb.append(ov.isLatest()); sb.append("\t");
		sb.append(Util.formatNumber(Util.daysOld(ov.lastModified()), 3)); sb.append("\tNA");
		return sb.toString();
	}

	/*KeyName Type Message VersionID isLatest DaysOld GBSize*/
	public static String objectToString(ObjectVersion ov, String message) {
		StringBuilder sb = new StringBuilder();
		sb.append(ov.key()); sb.append("\t");
		sb.append("Obj"); sb.append("\t");
		sb.append(message); sb.append("\t");
		sb.append(ov.versionId()); sb.append("\t");
		sb.append(ov.isLatest()); sb.append("\t");
		sb.append(Util.formatNumber(Util.daysOld(ov.lastModified()), 3)); sb.append("\t");
		sb.append(Util.formatNumber(Util.gigaBytes(ov.size()), 3));
		return sb.toString();
	}


}
