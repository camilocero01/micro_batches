import gdown
from zipfile import ZipFile

#descargamos el archivo desde una URL en google drive
file_id = "1ejZpGTvZa81ZGD7IRWjObFeVuYbsSvuB"
prefix = 'https://drive.google.com/uc?/export=download&id='
gdown.download(prefix+file_id)

# leemos el archivo .ZIP y creamos un objeto ZIP
with ZipFile("C:\\Users\Asus\\PycharmProjects\\micro_batches\\extract_files\\dataPruebaDataEngineer PRUEBA ING DATOS.zip", 'r') as zObject:

	# Extraemos todos los archivos del ZIP a una ruta determinada.
	zObject.extractall(
		path="C:\\Users\\Asus\\PycharmProjects\\micro_batches\\extract_files")