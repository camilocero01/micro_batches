Buenos días Pragma:

Para el desarrollo de la prueba técnica se tienen en cuenta los siguientes elementos:

-Para almacenar la información, se usa GITHUB con el fin de controlar las versiones del desarrollo
    https://github.com/camilocero01/micro_batches/tree/main

-Se entregan dos carpeta:
-- una llamada extract_files, la cual contiene un script que se conecta a la URL en Google drive, los copia en la URL de desarrollo y los descomprime. El objetivo es que se pueda ejecutar N veces en búsqueda de nuevos archivos.
    https://github.com/camilocero01/micro_batches/tree/main/extract_files

-- una segunda llamada transform, la cual contiene el script que hace los cálculos y almacena los registros en Base de datos
    https://github.com/camilocero01/micro_batches/tree/main/transform

-Los script se desarrollan en Python 3.9

-La tabla final se almacena en la base de datos que trae por defecto Python, SQLITE 3 y puede ser descargada en
    https://github.com/camilocero01/micro_batches/blob/main/transform/output.db

-Para calcular el valor promedio, se descarta del calculo	 los registros que no tienen precio

El log con el resultado de la prueba puede ser consultado en
https://github.com/camilocero01/micro_batches/blob/main/transform/log.txt
