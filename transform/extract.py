import os
import csv
import sqlite3
import datetime

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
file = open("log.txt", "w")


drop = ("""drop table if exists final_table""")
insert_data(drop, [])

#si no existe la tabla, la crea
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

    #extraigo el nombre del archivo
    file_name = os.path.basename(x)
    file.write("Archivo a cargar ->"+file_name+"\n")

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
        for row in csv_reader:
            val.append((row[0], row[1], row[2], datetime.date.today()))
            # quito primera linea con nombre de campos
            if line_count >= 1:

                #print(f'campo1 \t{row[0]} campo2 {(row[1])} campo3 {row[2]}. linea {line_count}')
                sql = "INSERT INTO final_table VALUES (?, ?, ?, DateTime('now'),'"+file_name+"')"
                insert_data(sql,row)
                file.write

                #actualizo variables globales
                if row[1] != '':
                    global_count += 1
                    global_sum_price = global_sum_price + int(row[1])
                    # line_count == 1 para inicializar la variable con el primer registro
                    if int(row[1]) > global_max or global_count == 1:  global_max = int(row[1])
                    if int(row[1]) < global_min or global_count == 1:  global_min = int(row[1])

                    local_count += 1
                    local_sum_price = local_sum_price + int(row[1])
                    # line_count == 1 para inicializar la variable con el primer registro
                    if int(row[1]) > local_max or local_count == 1:  local_max = int(row[1])
                    if int(row[1]) < local_min or local_count == 1:  local_min = int(row[1])

                    file.write(f'conteo de registros {local_count} sumatoria precio {local_sum_price} precio máximo {local_max} precio mínimo {local_min} media {local_sum_price / local_count}\n')

            line_count += 1


        #sql = "INSERT INTO final_table VALUES (?, ?, ?)"
        #c.executemany(sql, val)
    file.write(f'Lineas insertadas para el archivo{file_name}= {line_count-1} lines.\n')
    file.write(f'********** datos con el archivo {file_name}: conteo de registros {global_count} sumatoria precio {global_sum_price} precio máximo {global_max} precio mínimo {global_min} media {global_sum_price / global_count}\n\n\n\n')

file.write(f'Total registros cargados={str(len(os.listdir(file_src)))} \n nombre de los archivos= {os.listdir(file_src)}\n')

#cerramos el log
file.close()

