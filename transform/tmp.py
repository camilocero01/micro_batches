import datetime
import os
import csv
import sqlite3

conn = sqlite3.connect('output.db')
c = conn.cursor()

c.execute("""drop table final_table""")

c.execute("""CREATE TABLE IF NOT EXISTS final_table (
                    timestamp date,
                    price integuer,
                    user_id integuer,
                    create_time datetime
                ) """)


#busco en la carpeta de extracción los archivos encontrados y guardo su nombre en un array
file_src = "C:\\Users\\Asus\\PycharmProjects\\micro_batches\\extract_files\\dataPruebaDataEngineer"

#leo todos los archivos encontrados en la ruta file_src
for x in os.listdir(file_src):

    #extraigo el nombre del archivo
    file_name = os.path.basename(x)

    #cargo a una lista las filas y  campos del archivo recorrido
    with open(file_src + "\\" + file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')

        #recorro cada registro y lo guardo en la base de datos
        line_count = 0
        val = []
        for row in csv_reader:
            val.append(row[0], row[1], row[2])
            # quito primera linea con nombre de campos
            if line_count >= 1:
                print(f'campo1 \t{row[0]} campo2 {row[1]} campo3 {row[2]}.')
                sql = "INSERT INTO final_table VALUES (?, ?, ?, ?)"
                c.execute(sql,row)
            line_count += 1
        #sql = "INSERT INTO final_table VALUES (?, ?, ?)"
        #c.executemany(sql, val)
    print(f'Processed {line_count} lines.')

conn.commit()
conn.close()
print(len(os.listdir(file_src)))

print(os.listdir(file_src))