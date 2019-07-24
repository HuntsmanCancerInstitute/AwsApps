# AwsApps
Genomic data focused toolkit for working with AWS services (e.g. S3 and EC2) including exhaustive JUnit testing for each app.
<pre>
**************************************************************************************
**                                 GSync : July 2019                                **
**************************************************************************************
GSync pushes files with a particular extention that exceed a given size and age to 
Amazon's S3 object store. Associated genomic index files are also moved. Once 
correctly uploaded, GSync replaces the original file with a local txt placeholder file 
containing information about the S3 object. Symbolic links are ignored.

WARNING! This app has the potential to destroy precious genomic data. TEST IT on a
pilot system before depolying in production. BACKUP your local files and ENABLE S3
Object Versioning before running.

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

Required:
-d Path to a local directory to sync
-b Dediated S3 bucket name

Optional:
-e File extensions to consider, comma delimited, no spaces, case sensitive. Defaults
     to '.bam,.cram,.gz,.zip'
-a Minimum days old for archiving, defaults to 60
-g Minimum gigabyte size for archiving, defaults to 5
-r Perform a real run, defaults to just listing the actions that would be taken
-k Delete local files that were sucessfully uploaded, defaults to just printing
     'rm -r xxx' statements. Use with caution!
-v Verbose output

Example: java -Xmx20G -jar pathTo/USeq/Apps/GSync -d /Repo/ -b hcibioinfo_gsync_repo 
     -v -a 90 -g 1 

**************************************************************************************
</pre>
