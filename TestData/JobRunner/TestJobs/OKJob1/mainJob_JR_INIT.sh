echo "Downloading a file and checking it's integrity..."
aws s3 cp s3://hcibioinfo-jobrunner-test/JRTestDataBundle.zip ./TestDownload.zip
unzip -t TestDownload.zip

