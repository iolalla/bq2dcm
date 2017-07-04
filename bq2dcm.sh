#!/bin/sh
# Modify this to include your DataStorage path
outpath="gs://dcm_sample"
# This is the date format to include in the file names.
when=`date +%Y%m%d_%H%M%S`
file='bq2dcm-'$when'.dat'
echo 'File to store the query results: ' $file
# This is the query to be used to gather the results from BQ, feel free to adapt
# it to yout needs, the only trick this one does is to avoid duplicates in the 
# UserID
query="SELECT 'List_ID' as ListID, User_ID as uid, NOW() as timestamp \
FROM ( \
  SELECT *,User_ID AS index, ROW_NUMBER() OVER (PARTITION BY index) AS pos, \
  FROM [cloud-se-es:dcm.p_activity_411205] where User_ID != '0' \
) WHERE pos = 1"
# And hete goes the command, no absolute paths to make it easier to use.
bq --format=csv  query  $query > $file
echo 'Bigquery executed'
gsutil cp $file $outpath
echo 'Copied '$file' to '$outpath
if [ -e $file ]
then 
    rm $file
    echo 'File '$file' removed.'
fi
