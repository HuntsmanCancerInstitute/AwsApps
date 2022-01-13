# AwsApps
Genomic data focused toolkit for working with AWS services (e.g. S3 and EC2). Includes exhaustive JUnit testing for each app. See [Misc/WorkingWithTheAWSJobRunner.pdf](https://github.com/HuntsmanCancerInstitute/AwsApps/blob/master/Misc/WorkingWithTheAWSJobRunner.pdf) for details.
<pre>
u0028003$ java -jar -Xmx1G ~/Code/AwsApps/target/GSync_0.6.jar 

**************************************************************************************
**                                   GSync : June 2020                              **
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
   group. When you need to delete or update objects, switch to the Admin group, then
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
-q Quiet verbose output.
-e Email addresses to send gsync messages, comma delimited, no spaces.
-s Smtp host, defaults to hci-mail.hci.utah.edu
-x Execute every 6 hrs until complete, defaults to just once, good for downloading
    latent glacier objects.

Example: java -Xmx20G -jar pathTo/GSync_X.X.jar -r -u -k -b hcibioinfo_gsync_repo 
     -q -a 90 -g 1 -d -d /Repo/DNA,/Repo/RNA,/Repo/Fastq -e obama@real.gov

**************************************************************************************




u0028003$ java -jar -Xmx1G ~/Code/AwsApps/target/JobRunner_0.2.jar 

****************************************************************************************************************************
**                                              AWS Job Runner : December 2021                                            **
****************************************************************************************************************************
JR is an app for running bash scripts on AWS EC2 nodes. It downloads and uncompressed your resource bundle and looks for
xxx.sh_JR_START files in your S3 Jobs directories. For each, it copies over the directory contents, executes the
associated xxx.sh script, and transfers back the results.  This is repeated until no unrun jobs are found. Launch many
EC2 JR nodes, each running an instance of the JR, to process hundreds of jobs in parallel. Use spot requests and
hibernation to reduce costs.

To use:
1) Install and configure the aws cli on your local workstation, see https://aws.amazon.com/cli/
2) Upload your aws credentials file into a private bucket on aws, e.g.
     aws s3 cp ~/.aws/credentials s3://my-jr/aws.cred.txt
3) Generate a secure 24hr timed URL for the credentials file, e.g.
     aws --region us-west-2  s3 presign s3://my-jr/aws.cred.txt  --expires-in 259200
4) Upload a zip archive containing resources needed to run your jobs into S3, e.g.
     aws s3 cp ~/TNRunnerResourceBundle.zip s3://my-jr/TNRunnerResourceBundle.zip
     This will be copied into the /JRDir/ directory and then unzipped.
5) Upload script and job files into a 'Jobs' directory on S3, e.g.
     aws s3 cp ~/JRJobs/A/ s3://my-jr/Jobs/A/ --recursive
6) Optional, upload bash script files ending with JR_INIT.sh and or JR_TERM.sh. These are executed by JR before and after
     running the main bash script.  Use these to copy in sample specific resources, e.g. fastq/ cram/ bam files, and to run
     post job clean up.
7) Upload a file named XXX_JR_START to let the JobRunner know the bash script named XXX is ready to run, e.g.
     aws s3 cp s3://my-jr/emptyFile s3://my-jr/Jobs/A/dnaAlignQC.sh_JR_START
8) Launch the JobRunner.jar on one or more JR configured EC2 nodes. See https://ri-confluence.hci.utah.edu/x/gYCgBw

Job Runner Options:
-c URL to your secure timed config credentials file.
-r S3URI to your zipped resource bundle.
-j S3URI to your root Jobs directory containing folders with job scripts to execute.
-l S3URI to your Log folder for node logs.

Default Options:
-d Directory on the local worker node, full path, in which resources and job files will be processed, defaults to /JRDir/
-a Aws credentials directory, defaults to ~/.aws/
-t Terminate the EC2 node upon job completion. Defaults to looking for jobs for the min2Wait.
-w Minutes to wait when jobs are not found before termination, defaults to 10.
-x Replace S3 job directories with processed analysis, defaults to syncing local with S3. WARNING, if selected, don't place
     any files in these S3 jobs directories that cannot be replaced. JR will delete them.
-v Verbose debugging output.

Example: java -jar -Xmx1G JobRunner.jar
     -r s3://my-jr/TNRunnerResourceBundle.zip
     -j s3://my-jr/Jobs/
     -l s3://my-jr/NodeLogs/
     -c 'https://my-jr.s3.us-west-2.amazonaws.com/aws.cred.txt?X-Amz-Algorithm=AWS4-HMXXX...'

****************************************************************************************************************************



u0028003$ java -jar ~/Code/AwsApps/target/VersionManager_0.1.jar 

**************************************************************************************
**                             AWS S3 Version Manager : January 2022                **
**************************************************************************************
Bucket versioning in S3 protects objects from being deleted or overwritten by hiding
the original when 'deleting' or over writing an existing object. Use this tool to 
delete these hidden S3 objects and any deletion marks from your buckets. Use the
options to select particular redundant objects to delete in a dry run, review the
actions, and rerun it with the -r option to actually delete them. This app will not
delete any isLatest=true object.

WARNING! This app has the potential to destroy precious data. TEST IT on a
pilot system before deploying in production. Although extensively unit tested, this
app is provided with no guarantee of proper function.

To use the app:
1) Enable S3 Object versioning on your bucket.
2) Install and configure the aws cli with your region, access and secret keys. See
   https://aws.amazon.com/cli
3) Use cli commands like 'aws s3 rm s3://myBucket/myObj.txt' or the AWS web Console to
   'delete' particular objects. Then run this app to actually delete them.

Required Parameters:
-b Versioned S3 bucket name
-l Bucket region location

Optional Parameters:
-r Perform a real run, defaults to a dry run where no objects are deleted
-c Credentials profile name, defaults to 'default'
-a Minimum age, in days, of object to delete, defaults to 30
-s Object key suffixes to delete, comma delimited, no spaces
-p Object key prefixes to delete, comma delimited, no spaces
-q Quiet output.

Example: java -Xmx10G -jar pathTo/VersionManager_X.X.jar -b mybucket-vm-test 
     -s .cram,.bam,.gz,.zip -a 7 -c MiloLab -l us-west-2

**************************************************************************************
</pre>
