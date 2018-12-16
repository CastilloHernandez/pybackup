import sqlite3
import os
import hashlib
import time
import shutil
import argparse
import fnmatch
import re
import itertools
import logging

TIMESYMBOLS = {
	'customary'	 : ('s', 'm', 'h', 'D', 'w', 'M', 'Y'),
	'customary_ext' : ('sec', 'min', 'hour', 'day', 'week', 'month', 'year'),
}

def human2seconds(s):
	init = s
	prefix= {}
	prefix['s']=1
	prefix['m']=60
	prefix['h']=3600
	prefix['D']=86400
	prefix['w']=604800
	prefix['M']=2592000
	prefix['Y']=31104000
	num = ""
	while s and s[0:1].isdigit() or s[0:1] == '.':
		num += s[0]
		s = s[1:]
	num = float(num)
	letter = s.strip()
	for name, sset in TIMESYMBOLS.items():
		if letter in sset:
			break
	else:
		raise ValueError("can't interpret %r" % init)
	return int(num * prefix[letter])

SYMBOLS = {
    'customary'     : ('B', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y'),
    'customary_ext' : ('byte', 'kilo', 'mega', 'giga', 'tera', 'peta', 'exa', 'zetta', 'iotta'),
}

def human2bytes(s):
    init = s
    num = ""
    while s and s[0:1].isdigit() or s[0:1] == '.':
        num += s[0]
        s = s[1:]
    num = float(num)
    letter = s.strip()
    for name, sset in SYMBOLS.items():
        if letter in sset:
            break
    else:
        raise ValueError("can't interpret %r" % init)
    prefix = {sset[0]:1}
    for i, s in enumerate(sset[1:]):
        prefix[s] = 1 << (i+1)*10
    return int(num * prefix[letter])
	
def hashArchivo(ruta):
	hash_md5 = hashlib.md5()
	try:
		with open(ruta, "rb") as f:
			for chunk in iter(lambda: f.read(4096), b""):
				hash_md5.update(chunk)
	except:
		print 'error accediendo al archivo ' + ruta
	return hash_md5.hexdigest()

def removeIfEmpty(dir, raiz=0):
	for entry in os.listdir(dir):
		if os.path.isdir(os.path.join(dir, entry)):
			removeIfEmpty(os.path.join(dir, entry))
	if raiz == 0:
		if not os.listdir(dir):
			print 'borrando directorio vacio '+ dir
			os.rmdir(dir)	
	
def removeOldFiles(backupset, starttime):
	global db
	cursor = db.cursor()
	cursor.execute('SELECT backuppath FROM files WHERE backupset='+ str(backupset) +' AND backupdate<'+ str(starttime) +'')
	for row in cursor.fetchall():
		if os.path.isfile(row[0]):
			print 'archivo obsoleto '+ row[0]
			try:
				os.remove(row[0])
			except:
				print 'error al borrar '+ row[0]
	cursor.execute('DELETE FROM files WHERE backupset='+ str(backupset) +' AND backupdate<'+ str(starttime) +'')
	db.commit()
	
def removeEmpyFolders(origen, destino, backupset):
	dirDestino = destino + os.path.basename(os.path.split(origen)[0]) + str(backupset) + os.path.sep
	removeIfEmpty(dirDestino, 1)
	
def backupFolder(origen, destino, backupset):
	global db
	global opt
	print 'origen '+ origen
	logger.info('origen '+ origen)
	print 'destino '+ destino
	logger.info('destino '+ destino)
	cursor = db.cursor()
	maxsize= human2bytes(opt.maxsize)
	if opt.exclude:
		excludes = list(itertools.chain(*opt.exclude))
		rexcludes = r'|'.join([fnmatch.translate(x) for x in excludes]) or r'$.'
	for root, dirs, files in os.walk(origen):
		if opt.exclude:
			dirs[:] = [d for d in dirs if not re.match(rexcludes, d)]
			files[:] = [f for f in files if not re.match(rexcludes, f)]
		for file in files:
			archivoEncontrado = 0
			archivoModificado = 0
			idActual = 0
			rutaActual = str(os.path.join(root, file))
			try:
				tamanioActual = os.path.getsize(rutaActual)
			except:
				print 'error al acceder '+ rutaActual
				logger.error('error al acceder '+ rutaActual)
				continue
			if tamanioActual > maxsize:
				print 'archivo demasiado grande '+ rutaActual
				logger.info('archivo demasiado grande '+ rutaActual)
				continue
			hashActual = hashArchivo(rutaActual)
			cursor.execute('SELECT id, backuphash FROM files WHERE backupset='+ str(backupset) +' AND originalpath="'+ rutaActual +'"')
			for row in cursor.fetchall():
				archivoEncontrado = 1
				idActual = row[0]
				if not row[1] == hashActual:
					archivoModificado = 1
			dirDestino = os.path.normpath(str(root).replace(origen, destino + os.path.basename(os.path.split(origen)[0]) + str(backupset) + os.path.sep))
			rutaDestino = os.path.join(dirDestino, file)
			if archivoEncontrado:
				if archivoModificado:
					print 'archivo modificado '+ rutaActual +' a '+ rutaDestino
					try:
						shutil.copy(rutaActual, rutaDestino)
					except:
						print 'error al copiar '+ rutaActual +' a '+ rutaDestino
						logger.error('error al copiar '+ rutaActual +' a '+ rutaDestino)
					cursor.execute('UPDATE files SET backuphash=?, backupdate=? WHERE id=?', (hashActual, time.time(), idActual))
					db.commit()
				else:
					cursor.execute('UPDATE files SET backupdate=? WHERE id=?', (time.time(), idActual))
					db.commit()
			else:
				if not (os.path.isdir(dirDestino)):
					os.makedirs(dirDestino)
				print 'copiar archivo '+ rutaActual +' a '+ rutaDestino
				try:
					shutil.copy(rutaActual, rutaDestino)
					cursor.execute('INSERT INTO files(backupset, originalpath, originalname, backuppath, backupsize, backuphash, backupdate) VALUES(?, ?,?,?,?,?,?)', (backupset, rutaActual, file, rutaDestino, 0, hashActual, time.time() ))
					db.commit()
				except:
					print 'error al copiar '+ rutaActual +' a '+ rutaDestino
					logger.error('error al copiar '+ rutaActual +' a '+ rutaDestino)

parser = argparse.ArgumentParser(prog='pybackup')
parser.add_argument('-o', action="append", required=True)
parser.add_argument('-d', action="append", required=True)
parser.add_argument('-maxsize', default='100M')
parser.add_argument('-n', type=int, default=3)
parser.add_argument('-r', default='1M')
parser.add_argument('-exclude', action="append", nargs='*')

opt = parser.parse_args()				

starttime = time.time()
db = sqlite3.connect('pybackup.db')
db.text_factory = lambda x: unicode(x, "utf-8", "ignore")

cursor = db.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, backupset INTEGER, originalpath TEXT, originalname TEXT, backuppath TEXT, backupsize INTEGER, backuphash TEXT, backupdate INTEGER)')
db.commit()

for logdir in opt.d:
	if not logdir.endswith(os.path.sep):
		logdir = logdir + os.path.sep 
	if not (os.path.isdir(logdir)):
		os.makedirs(logdir)
		
logger = logging.getLogger('pybackup')
hdlr = logging.FileHandler(logdir + 'pybackup.log')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
logger.setLevel(logging.INFO)
backupset = 1 + int(time.time() / human2seconds(opt.r)) % opt.n
logger.info('iniciando backup')
logger.info('backup set '+ str(backupset))

for destino in opt.d:
	for origen in opt.o:
		if not origen.endswith(os.path.sep):
			origen = origen + os.path.sep
		if not destino.endswith(os.path.sep):
			destino = destino + os.path.sep
		backupFolder(origen, destino, backupset)

removeOldFiles(backupset, starttime)

for destino in opt.d:
	for origen in opt.o:
		if not origen.endswith(os.path.sep):
			origen = origen + os.path.sep
		if not destino.endswith(os.path.sep):
			destino = destino + os.path.sep
		
		removeEmpyFolders(origen, destino, backupset)

logger.info('backup terminado')