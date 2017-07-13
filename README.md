The objective of this project is to simplify the process to move data from 
DobleClick DataTransfer (aka BQ) to Storage and to DC Manager.

The project includes examples for a shell script and a go appengine app.

In order to run the go appengine, you will need go 1.8 and appengine flex,
to install dependencies please run, from the src dir:
$ go get ./...

You will need a Bigquery project with the dataTransfer activated, and from there
you will extract your data to a Cloud Storage bucket.