#!/bin/bash
set -e

# david.nix@hci.utah.edu, 24 April 2024

echo
echo "This helper script uses the AWS CLI to list the contents of each bucket in the account associated with a particular credentials key set. Use these local bucket indexes to search for particular files with grep."
echo
echo "Please enter which profile to use from your ~/.aws/credentials file?"
echo -n "   -> "
read profile
echo "Listing buckets in "$profile
echo
echo Tip: delete particular bucketName.list.txt files to relist, otherwise they will be skipped.
echo

rm -f COMPLETE

for x in $(aws --profile $profile s3 ls | cut -d' ' -f3)
do 
out=$x'.list.txt'
if [ ! -f $out ]
then
    echo $out ' not found, listing...'
    aws s3 ls s3://$x --recursive --profile $profile > $out &
else
    echo '   '$out ' found, skipping.'
fi
done

echo
echo Waiting for all of the aws jobs to complete.

while [ $(ps -u | grep 'aws s3 ls s3://' | wc -l) -ne 1  ] ;
do
      sleep 1
      echo -n '.'
done
echo; echo
echo COMPLETE!
touch COMPLETE



