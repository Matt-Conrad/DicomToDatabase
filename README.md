# DICOM Directory to PostgreSQL DB

Have you ever gotten a DICOM dataset from the internet in the form of a folder with a bunch of DCM files in it? I did recently and wanted to get an idea of the data that was housed in all of these DCM files, so I wrote this small tool that will extract all of the the DICOM elements you're interested in from all of the DCM files in the directory and it will put the data into a PostgreSQL DB table so you can indirectly query the DICOM header data. 
