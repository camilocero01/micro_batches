import gdown
from zipfile import ZipFile

#download file in url in google drive
file_id = "1ejZpGTvZa81ZGD7IRWjObFeVuYbsSvuB"
prefix = 'https://drive.google.com/uc?/export=download&id='
gdown.download(prefix+file_id)

# importing the zipfile module
# loading the temp.zip and creating a zip object
with ZipFile("C:\\Users\Asus\\PycharmProjects\\micro_batches\\extract_files\\dataPruebaDataEngineer PRUEBA ING DATOS.zip", 'r') as zObject:

	# Extracting all the members of the zip
	# into a specific location.
	zObject.extractall(
		path="C:\\Users\\Asus\\PycharmProjects\\micro_batches\\extract_files")
