#!/bin/sh
# TODO:Modify this to include your DataStorage path pointing to a PLU BUCKET
# This will like something like this: dcm-plt-data-c18f5735ec6014300b17dcefd64bc4gc9c5ac7eb
outpath="gs://YOUR_PATH_TO_PLU_BUCKET"
# This is the date format to include in the file names.
when=`date +%Y%m%d_%H%M%S`
# TODO: update with you accountid and floodlightconfigurationid 
# This is a .dat file, but the data included is a CSV format
# accountid_floodlightConfigurationid_yyyymmdd_uploadnumber
# accountid == DCM Network ID
# floodlightConfigurationid == Look into the DCM console Advertiser ID that owns the audiences
file='accountid_floodlightConfigurationid_'$when'.dat'
echo 'File to store the query results: ' $file
# TODO: Update the query to YOUR_PROJECT, YOUR_DATASET.YOUR_TABLE
# This is the query to be used to gather the results from BQ, feel free to adapt
# it to yout needs, the only trick this one does is to avoid duplicates in the 
# UserID
# timestamp field is optional
# delete field is optional, can take values true or false [true, false] to remove
#     a user from an audience list.
# 
# Every file header must include list_id and either cookie_encrypted or cookie_decimal. 
# All other columns in the file are optional.
# File Headers are case sensitive and should only be lower case to avoid errors.
query="SELECT 'List_ID' as list_id, User_ID as cookie_encrypted, NOW() as timestamp , 'false' as [delete]\
FROM ( \
  SELECT *,User_ID AS index, ROW_NUMBER() OVER (PARTITION BY index) AS pos, \
  FROM [YOUR_PROJECT:YOUR_DATASET.YOUR_TABLE] where User_ID != '0' \
) WHERE pos = 1"
# And hete goes the command, no absolute paths to make it easier to use.
bq --format=csv  query  $query > $file
echo 'Bigquery executed'
sed -i '1d' $file
echo 'Removed nasty first line'
gsutil cp $file $outpath
echo 'Copied '$file' to '$outpath
if [ -e $file ]
then 
    rm $file
    echo 'File '$file' removed.'
fi
# Once this is setup you can se a crontab
