# AwsApps
Genomic data focused toolkit for working with AWS services (e.g. S3 and EC2). Includes exhaustive JUnit testing for each app.
<pre>
**************************************************************************************
**                                  GSync : August 2019                             **
**************************************************************************************
GSync pushes files with a particular extension that exceed a given size and age to 
Amazon's S3 object store. Associated genomic index files are also moved. Once 
correctly uploaded, GSync replaces the original file with a local txt placeholder file 
containing information about the S3 object. Files are restored or deleted by modifying
the name of the placeholder file. Symbolic links are ignored.

WARNING! This app has the potential to destroy precious genomic data. TEST IT on a
pilot system before deploying in production. BACKUP your local files and ENABLE S3
Object Versioning before running.  This app is provided with no guarantee of proper
function.

To use the app:
1) Create a new S3 bucket dedicated solely to this purpose. Use it for nothing else.
2) Enable S3 Object Versioning on the bucket to prevent accidental deletion and
   create an AbortIncompleteMultipartUpload lifecycle rule to delete partial uploads.
3) Create a ~/.aws/credentials file with your access, secret, and region info, chmod
   600 the file and keep it private. Use a txt editor or the aws cli configure
   command, see https://aws.amazon.com/cli   Example ~/.aws/credentials file:
   [default]
   aws_access_key_id = AKIARHBDRGYUIBR33RCJK6A
   aws_secret_access_key = BgDV2UHZv/T5ENs395867ueESMPGV65HZMpUQ
   region = us-west-2
4) Execute GSync to upload large old files to S3 and replace them with a placeholder
   file named xxx.S3.txt
5) To download and restore an archived file, rename the placeholder
   xxx.S3.txt.restore and run GSync.
6) To delete an S3 archived file, it's placeholder, and any local files, rename the 
   placeholder xxx.S3.txt.delete and run GSync.
   Versioned copies on S3 will not be deleted. Use the S3 Console to do so.
7) Placeholder files may be moved, see -u

Required:
-d Path to a local directory to sync. This becomes the base key in S3, e.g. 
     BucketName/LocalDirName/...
-b Dediated S3 bucket name

Optional:
-e File extensions to consider, comma delimited, no spaces, case sensitive. Defaults
     to '.bam,.cram,.gz,.zip'
-a Minimum days old for archiving, defaults to 60
-g Minimum gigabyte size for archiving, defaults to 5
-r Perform a real run, defaults to just listing the actions that would be taken.
-k Delete local files that were successfully  uploaded.
-u Update S3 Object keys to match current placeholder paths, slow for large files.
-v Verbose output

Example: java -Xmx20G -jar pathTo/USeq/Apps/GSync -d Repo/ -b hcibioinfo_gsync_repo 
     -v -a 90 -g 1 

**************************************************************************************
</pre>
