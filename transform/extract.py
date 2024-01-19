import os
import csv
import sqlite3
import datetime

#inicializamos las variables de calculo global
global_count = 0
global_sum_price = 0
global_max = 0
global_min = 0

#creamos una funcion para ejecutar sentencias en sqlite
def insert_data(sql,*args):
    conn = sqlite3.connect('output.db')
    c = conn.cursor()
    c.execute(sql, *args)
    conn.commit()
    conn.close()

#abrimos un archivo de escritura para llevar el log
#el log final puede ser consultado en GITHUB https://github.com/camilocero01/micro_batches/blob/main/transform/log.txt
file = open("log.txt", "w",encoding="utf-8")

#si no existe la tabla, la crea
#   timestamp fecha del registro
#   price precio de registro
#   user_id id del usuario del registro
#   create_time se adiciona este campo de AUDITORIA para almacenar la fecha de procesamiento
#   source_file_name se adiciona este campo de AUDITORIA para hacer seguimiento del archivo fuente"""

create_table = ("""CREATE TABLE IF NOT EXISTS final_table (
                    timestamp date,
                    price integuer,
                    user_id integuer,
                    create_time datetime,
                    source_file_name string
                ) """)
insert_data(create_table,[])

#busco en la carpeta de extracción los archivos encontrados y guardo su nombre en un array
file_src = "C:\\Users\\Asus\\PycharmProjects\\micro_batches\\extract_files\\dataPruebaDataEngineer"

#leo todos los archivos encontrados en la ruta file_src
for x in os.listdir(file_src):

    # extraigo el nombre del archivo
    file_name = os.path.basename(x)

    # abrimos una conexión para las consultas
    # el objetivo es validar si el archivo ya se cargo previamente en la base de datos
    # si existe, solo dejo el log donde indico que no se proceso, si no existe, entonces lo proceso y lo inserto a la BD
    file_exist = 0
    conn = sqlite3.connect('output.db')
    c = conn.cursor()
    c.execute(
        "select   min(1) from final_table where source_file_name = '"+file_name+"'")
    rows = c.fetchall()
    for row in rows:
        file_exist = row[0]
    conn.commit()
    conn.close()

    #si el nombre del archivo existe, entonces no se carga
    if file_exist != 1:

        #usamos file.write para guardar en el log
        file.write("Archivo Nuevo a cargar ->"+file_name+"\n")

        #inicializamos las variables de calulo individual por cada archivo
        local_count = 0
        local_sum_price = 0
        local_max = 0
        local_min = 0

        #cargo a una lista las filas y  campos del archivo recorrido
        with open(file_src + "\\" + file_name) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')

            #recorro cada registro y lo guardo en la base de datos
            line_count = 0
            val = []

            #ciclo for para leer cada campo de cada registro
            for row in csv_reader:

                #llevo los datos a un array
                val.append((row[0], row[1], row[2], datetime.date.today()))

                # quito primera linea con nombre de campos
                if line_count >= 1:

                    #inserto el registro
                    sql = "INSERT INTO final_table VALUES (?, ?, ?, DateTime('now'),'"+file_name+"')"
                    insert_data(sql,row)

                    #actualizo variables globales y locales
                    #se parte de la siguiente premisa, para los calculos no se tienen en cuenta registros sin precio
                    if row[1] != '':

                        global_count += 1
                        global_sum_price = global_sum_price + int(row[1])
                        # global_count == 1 para inicializar la variable con el primer registro
                        if int(row[1]) > global_max or global_count == 1:  global_max = int(row[1])
                        if int(row[1]) < global_min or global_count == 1:  global_min = int(row[1])

                        local_count += 1
                        local_sum_price = local_sum_price + int(row[1])
                        # global_count == 1 para inicializar la variable con el primer registro
                        if int(row[1]) > local_max or local_count == 1:  local_max = int(row[1])
                        if int(row[1]) < local_min or local_count == 1:  local_min = int(row[1])

                        #guardo el log para el calculo de cada registro
                        file.write(f'conteo de registros {local_count} sumatoria precio {local_sum_price} precio máximo {local_max} precio mínimo {local_min} media {local_sum_price / local_count}\n')

                line_count += 1

        file.write(f'Lineas tenidas en cuenta para el cálculo del archivo{file_name}= {line_count-1} lines.\n')
        file.write(f'********** datos adicionando el archivo {file_name}: conteo de registros {global_count} sumatoria precio {global_sum_price} precio máximo {global_max} precio mínimo {global_min} media {global_sum_price / global_count}\n\n\n\n')
    else:
        #si el archivo ya fue cargado previamente en BD, entonces dejo el log de que no se cargo
        file.write("No cargo Archivo ->" + file_name + " se validó y existe en base de datos\n")

file.write(f'Total archivos cargados en BD={str(len(os.listdir(file_src)))} \n nombre de los archivos= {os.listdir(file_src)}\n\n\n')


#con fines de comparación, se hacen dos consultas a la base de datos final con el fin de dejar evidencia en el LOG
#como en la tabla final se puede identificar el archivo fuente, se hacen dos consultas, uno teniendo en cuenta el archivo validation.csv y otra sin tenerlo en cuente

#query sin tener en cuenta el archivo validation.csv
conn = sqlite3.connect('output.db')
c = conn.cursor()
c.execute("select   count(1) ,sum(price), max(price), min(price),sum(price)/count(1) from final_table where price != '' and source_file_name != 'validation.csv'")
rows = c.fetchall()
for row in rows:
    file.write(f'Validación en SQL, los datos SIN el archivo validation.csv es: conteo de registros {row[0]} sumatoria precio {row[1]} precio máximo {row[2]} precio mínimo {row[3]} media {row[1]/row[0]}\n\n')

#query teniendo en cuenta el archivo validation.csv
c.execute("select   count(1) ,sum(price), max(price), min(price),sum(price)/count(1) from final_table where price != '' ")
rows = c.fetchall()
for row in rows:
    file.write(f'Validación en SQL, los datos CON el archivo validation.csv es: conteo de registros {row[0]} sumatoria precio {row[1]} precio máximo {row[2]} precio mínimo {row[3]} media {row[1]/row[0]}\n\n')
conn.commit()
conn.close()

#cerramos el log
file.close()

