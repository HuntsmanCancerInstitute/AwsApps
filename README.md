# AwsApps
Genomic data focused toolkit for working with AWS services (e.g. S3 and EC2). Includes exhaustive JUnit testing for each app.
<pre>
MacBook-Pro-89:~ u0028003$ java -jar -Xmx1G ~/Code/AwsApps/target/GSync_0.4.jar 

**************************************************************************************
**                                   GSync : Feb 2020                               **
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
2) Enable S3 Object Locking and Versioning on the bucket to assist in preventing 
   accidental object overwriting. Add lifecycle rules to
   AbortIncompleteMultipartUpload and move objects to Deep Glacier.
3) It is a good policy when working on AWS S3 to limit your ability to accidentally
   delete buckets and objects. To do so, create and assign yourself to an AWS Group 
   called AllExceptS3Delete with a custom permission policy that denies s3:Delete*:
   {"Version": "2012-10-17", "Statement": [
      {"Effect": "Allow", "Action": "*", "Resource": "*"},
      {"Effect": "Deny", "Action": "s3:Delete*", "Resource": "*"} ]}
   For standard upload and download gsyncs, assign yourself to the AllExceptS3Delete
   group. When you need to delete objects or buckets, switch to the Admin group, then
   switch back. Accidental overwrites are OK since object versioning is enabled.
   To add another layer of protection, apply object legal locks via the aws cli.
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
   Before executing, switch the GSync/AWS user to the Admin group.
7) Placeholder files may be moved, see -u

Required:
-d One or more local directories with the same parent to sync. This parent dir
     becomes the base key in S3, e.g. BucketName/Parent/.... Comma delimited, no
     spaces, see the example.
-b Dedicated S3 bucket name

Optional:
-f File extensions to consider, comma delimited, no spaces, case sensitive. Defaults
     to '.bam,.cram,.gz,.zip'
-a Minimum days old for archiving, defaults to 120
-g Minimum gigabyte size for archiving, defaults to 5
-r Perform a real run, defaults to just listing the actions that would be taken.
-k Delete local files that were successfully uploaded.
-u Update S3 Object keys to match current placeholder paths.
-c Recreate deleted placeholder files using info from orphaned S3 Objects.
-v Verbose output.
-e Email addresses to send gsync messages, comma delimited, no spaces.
-s Smtp host, defaults to hci-mail.hci.utah.edu
-x Execute every 6 hrs until complete, defaults to just once, good for downloading
    latent glacier objects.

Example: java -Xmx20G -jar pathTo/GSync_X.X.jar -r -u -k -b hcibioinfo_gsync_repo 
     -v -a 90 -g 1 -d -d /Repo/DNA,/Repo/RNA,/Repo/Fastq -e obama@real.gov

**************************************************************************************
</pre>
