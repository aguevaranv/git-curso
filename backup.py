#!/usr/bin/python
# -*- encoding: utf-8 -*-
 
######################################################################################################
##                                                                                                  ##
##         SCRIPT EN PYTHON PARA EL RESPALDO DE BASES DE DATOS POSTGRES Y SUBIDA A MEGA             ##
##                                                                                                  ##
##     Almacenará tres tipos de copia:                                                              ##
##     - Diarias: el sistema realizará una copia diaria por base de datos y eliminará aquellas      ##
##                que tengan más de N_DAYS_AGO_DIARY_BACKUP días de antiguedad                      ##
##     - Semanales: el sistema realizará una copia semanal cada viernes de cada base de datos y     ##
##                  eliminará aquellas que tengan más de N_WEEKS_AGO_BACKUP semanas de antiguedad   ##
##     - Mensuales: el sistema realizará una copia mensual los días 1 de cada mes y eliminará       ##
##                  aquellas que tengan más de N_MONTHS_AGO_BACKUP meses de antiguedad              ##
##                                                                                                  ##
##                                                                                                  ##
##     * para poder subir los ficheros a mega es necesario instalar el siguiente requerimiento      ##
##       https://github.com/richardasaurus/mega.py                                                  ##
##                                                                                                  ##
##       sudo pip install mega.py                                                                   ##
##                                                                                                  ##
##                                                                                                  ##
##      @bienvenidosaez                                                                             ##
##                                                                                                  ##
######################################################################################################
 
 
from time import gmtime, strftime
import subprocess
import os
import glob
import time
from mega import Mega                    # sudo pip install mega.py

HOST             = '10.128.37.70'           # hostname del servidor de postgres
DATABASE_LIST    = (
                        {
                            'name'      : '001',
                            'username'  : 'odoo',
                            'password'  : 'EOL2tgM1Fbcop73HPt5kHCQm9oc0wmjEgfk/CtGaKkA='
                        },
                    )
BACKUP_DIR       = '/var/dbbackup/'      # directorio para guardar las copias
MONTH_DAY        = '01'                  # día del mes para la copia mensual
WEEK_DAY         = '1'                   # día de la semana para las copias semanal [1(Monday), 7]
N_DAYS_AGO       = 2                     # nº de copias diarias a almacenar
N_WEEKS_AGO      = 4                     # nº de copias semanas que se almecenarán
N_MONTHS_AGO     = 12                    # nº de copias mensuales que se almacenarán
DUMPER           = """ pg_dump --no-privileges --no-owner --no-reconnect -h 10.128.37.70 -p 5434 -U %s -f %s -F c %s  """                  

# MEGA
MEGA             = True                     # True = sube los archivos generados a la cuenta MEGA indicada
MEGA_EMAIL       = 'amguevarac@hotmail.com'         # Email de MEGA
MEGA_PASSWORD    = 'Am11062009'               # Password de MEGA
MEGA_FOLDER      = '/backup'             # Directorio donde subir los backup en MEGA

def log(string):
    print (time.strftime('%Y-%m-%d-%H-%M-%S', time.gmtime()) + ': ' + str(string))

def diary_backup(database):
    global BACKUP_DIR
    global MEGA
    global MEGA_FOLDER
    global DUMPER

    # Seteamos el password como variable de entorno para que pg_dump la coja de ahí
    os.putenv('PGPASSWORD', database['password'])

    print("===== START diary backup for %s =====" % database['name'])
 
    # Iteramos sobre las copias diarias, este apartado se ejecutará en cada ejecución del fichero
    glob_list = glob.glob(BACKUP_DIR + database['name'] + '_diary_backup*' + '.pgdump')
    for file in glob_list:
        file_info = os.stat(file)
        if file_info.st_ctime < x_days_ago:
            log("Delete diary backup: %s" % file)
            os.unlink(file)
        else:
            log("Keep diary backup: %s" % file)
      
    thetime = str(strftime("%Y-%m-%d")) 
    file_name = database['name'] + '_diary_backup_' + thetime + ".sql.pgdump"
    command = DUMPER % (database['username'],  BACKUP_DIR + file_name, database['name'])
    log(command)
    subprocess.call(command, shell=True)
    if MEGA:
        mega_upload_file(BACKUP_DIR + file_name, MEGA_FOLDER)
    print("===== END diary backup for %s =====\n" % database['name'])

def week_backup(database):
    global BACKUP_DIR
    global MEGA
    global MEGA_FOLDER
    global DUMPER

    print("\n===== START week backup for %s =====" % database['name'])
    glob_list = glob.glob(BACKUP_DIR + database['name'] + '_week_backup*' + '.pgdump')
    for file in glob_list:
        file_info = os.stat(file)
        if file_info.st_ctime < x_montsh_ago:
            log("Delete week backup: %s" % file)
            os.unlink(file)
        else:
            log("Keep week backup: %s" % file)
      
    thetime = str(strftime("%Y-%m-week-%U")) 
    file_name = database['name'] + '_week_backup_' + thetime + ".sql.pgdump"
    command = DUMPER % (database['username'],  BACKUP_DIR + file_name, database['name'])
    log(command)
    subprocess.call(command, shell=True)
    if MEGA:
        mega_upload_file(BACKUP_DIR + file_name, MEGA_FOLDER)
    print("===== END week backup for %s =====\n" % database['name'])

def month_backup(database):
    global BACKUP_DIR
    global MEGA
    global MEGA_FOLDER
    global DUMPER

    print("===== START month backup for %s =====" % database['name'])
    # Iteramos sobre las copias mensuales
    glob_list = glob.glob(BACKUP_DIR + database['name'] + '_month_backup*' + '.pgdump')
    for file in glob_list:
        file_info = os.stat(file)
        if file_info.st_ctime < x_montsh_ago:
            log("Delete month backup: %s" % file)
            os.unlink(file)
        else:
            log("Keep month backup: %s" % file)
      
    thetime = str(strftime("%Y-%m")) 
    file_name = database['name'] + '_month_backup_' + thetime + ".sql.pgdump"
    command = DUMPER % (database['username'],  BACKUP_DIR + file_name, database['name'])
    log(command)
    subprocess.call(command, shell=True)
    if MEGA:
        mega_upload_file(BACKUP_DIR + file_name, MEGA_FOLDER)
    print("===== END month backup for %s =====\n" % database['name'])

def mega_connect():
    global MEGA_EMAIL
    global MEGA_PASSWORD

    mega = Mega()
    m = mega.login(MEGA_EMAIL, MEGA_PASSWORD)
    return m

def mega_upload_file(file_to_upload, destination_path):
    m = mega_connect()
    file_info = os.stat(file_to_upload)
    total_space = m.get_storage_space()['total']
    total_used  = m.get_storage_space()['used']
    total_free = total_space - total_used
    if total_free > file_info.st_size:
        folder = m.find(destination_path)
        return m.upload(file_to_upload, folder[0])
    else:
        return False

# Si el directorio no existe lo creamos
if not os.path.isdir(BACKUP_DIR):
  os.makedirs(BACKUP_DIR, 0o770)
 
x_days_ago   = time.time() - ( 60 * 60 * 24 * N_DAYS_AGO )
x_weeks_ago  = time.time() - ( 60 * 60 * 24 * N_WEEKS_AGO  * 7)
x_montsh_ago = time.time() - ( 60 * 60 * 24 * N_MONTHS_AGO * 30)
 
# Iteramos sobre las bases de datos de las listas
for database in DATABASE_LIST:
 
    #La copia diaria la ejecutamos siempre
    diary_backup(database)
 
    # Si estamos en el día del mes indicado en la variable MOTH_DAY hay que ejecutar la parte mensual del backup
    if MONTH_DAY == time.strftime('%d', time.gmtime()):
        month_backup(database)
 
    # Si estamos en el día de la semanaindicado en la variable WEEK_DAY hay que ejecutar la parte semanal del backup
    if WEEK_DAY == time.strftime('%u', time.gmtime()):
        week_backup(database)
