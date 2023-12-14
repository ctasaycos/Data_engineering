'''
Web scrapping:
This project had as objective take all the documents in pdfs and send this information to every sales analyst in the
company, they have to create prospect which will be send to potential customers. For privacy I obfuscated some information
of the company. 
To avoid scrap irrelevant documents we determined some keywords.
'''
#Import Libraries
import os
from os import chdir, getcwd, listdir, path
import PyPDF2
from time import strftime
import docx2pdf
import glob

Al  = ["ArcGIS", "Licencias","Drones","Visor_web","GPS","Licenciamiento","PIX4D"]
JJ  = ["Imágenes","Satelital", "Desarrollo","Ortoimagen","Mosaico","BIM","Imagenes","Satélite","Digital Globe","Quickbird","SPOT","Building","Modeling"]
Ll  = ["Sistemas", "Analisis","Análisis","Visualización","Plataforma","SuperGIS","SuperMAP","Catastro","Map Info","Map","Autodesk","Sistematización","Analyst","AutoCAD","Civil 3D","Revit","Inventor","Infraworks"]
SL  = ["GIS", "ENVI","Teledetección","Open","Source","Software libre","PCI Geomatics","Cartografia","Geomatics","SIG","IDL","Deep", "learning","Geográfica","Geomática","Georeferencial","Geoespacial","Geodiseño"]
AN  = ["Servicios", "Consultoria","Soporte","Integración","Integracion"]

#Transform pdf to text 
def check_path(prompt):
    ''' (str) -> str
    Verifies if the provided absolute path does exist.
    '''
    abs_path = input(prompt)
    while path.exists(abs_path) != True:
        print("\nEl campo no existe\n")
        abs_path = input(prompt)
    return abs_path
print("\n")
folder = check_path("Escriba la ruta: ")
list = []
directory = folder
for root, dirs, files in os.walk(directory):
    for filename in files:
        if filename.endswith('.pdf'):
            t = os.path.join(directory, filename)
            list.append(t)
for item in list:
    path = item
    head, tail = os.path.split(path)
    var = "/"
    tail = tail.replace(".pdf", ".txt")
    name = head + var + tail
    content = ""
    pdf = PyPDF2.PdfFileReader(path, "rb")
    for i in range(0, pdf.getNumPages()):
        try:
            content += pdf.getPage(i).extractText() + "\n"
        except:
            print(filename,"No se puede transformar por favor revisar")
    print(strftime("%H:%M:%S"), " pdf  -> txt ")
    with open(name, 'a',encoding='utf8') as out:
        out.write(content)

#Identify all text files
import glob
file_list = glob.glob('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss/*.txt')
#Identificar archivos vacios
vacios=[]
filesize=[]
for i in range(len(file_list)):
    filesize.append(os.path.getsize(file_list[i]))
    if filesize[i]<50:
        vacios.append(file_list[i])
    else:
    print(filesize[i],"Archivo con datos")
vacios = [vacios.replace('.txt', '.pdf') for vacios in vacios]

#Funcion para contar palabras dentro de los archivos txt
def wordcount(filename, listwords):
    try:
        file = open(filename, "r",encoding='utf8')
        read = file.readlines()
        file.close()
        for word in listwords:
            lower = word.lower()
            count = 0
            for sentance in read:
                line = sentance.split()
                for each in line:
                    line2 = each.lower()
                    line2 = line2.strip("@#$%")
                    if lower == line2:
                        count += 1
            if count > 0:
                print(count,",",filename)
    except FileExistsError:
        print("El archivo no esta aqui")

#Keywords
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_Al.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], Al)
sys.stdout = orig_stdout
f.close()

#Lees lista con el txt donde esta el nombre de los archivos y palabras clave
with open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_Al.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_Al = [lines.replace('.txt', '.pdf') for lines in lines]

#AN
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_AN.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], AN)
sys.stdout = orig_stdout
f.close()

#Lees lista con el txt donde esta el nombre de los archivos y palabras clave
with open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_AN.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_AN = [lines.replace('.txt', '.pdf') for lines in lines]

#JJ
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_JJ.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], JJ)
sys.stdout = orig_stdout
f.close()

#Read the list with keywords
with open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_JJ.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_JJ = [lines.replace('.txt', '.pdf') for lines in lines]

#Ll
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_Ll.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], Ll)
sys.stdout = orig_stdout
f.close()

#Lees lista con el txt donde esta el nombre de los archivos y palabras clave
with open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_Ll.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_Ll = [lines.replace('.txt', '.pdf') for lines in lines]

#SL
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_SL.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], SL)
sys.stdout = orig_stdout
f.close()

with open('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_SL.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_SL = [lines.replace('.txt', '.pdf') for lines in lines]


import pandas as pd
import numpy as np
from pandas import DataFrame
#file_list = [file_list.replace('.txt', '.pdf') for file_list in file_list]
#if len(file_list) == 0:
#    print("No hay datos")
#else:
#    df_file_list= DataFrame (file_list)
#    df_file_list=df_file_list[0].str.split(',',expand=True)
#    df_file_list.rename(columns={0: 'archivo'}, inplace=True)

if len(lines_Al) == 0:
    df_Al = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_Al','archivo'])
else:
    df_Al = DataFrame (lines_Al)
    df_Al=df_Al[0].str.split(',',expand=True)
    df_Al.rename(columns={0: 'cantidad_Al', 1: 'archivo'}, inplace=True)
    df_Al["cantidad_Al"] = df_Al.cantidad_Al.astype(float)
    df_Al=df_Al.groupby(['archivo'])['cantidad_Al'].agg('sum').reset_index()
    df_Al['Name'] = 'Al'

if len(lines_AN) == 0:
    df_AN = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_AN','archivo'])
else:
    df_AN = DataFrame (lines_AN)
    df_AN=df_AN[0].str.split(',',expand=True)
    df_AN.rename(columns={0: 'cantidad_AN', 1: 'archivo'}, inplace=True)
    df_AN["cantidad_AN"] = df_AN.cantidad_AN.astype(float)
    df_AN=df_AN.groupby(['archivo'])['cantidad_AN'].agg('sum').reset_index()
    df_AN['Name'] = 'AN'

if len(lines_JJ) == 0:
    df_JJ = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_Juan','archivo'])
else:
    df_JJ = DataFrame (lines_JJ)
    df_JJ=df_JJ[0].str.split(',',expand=True)
    df_JJ.rename(columns={0: 'cantidad_Juan', 1: 'archivo'}, inplace=True)
    df_JJ["cantidad_Juan"] = df_JJ.cantidad_Juan.astype(float)
    df_JJ=df_JJ.groupby(['archivo'])['cantidad_Juan'].agg('sum').reset_index()
    df_JJ['Name'] = 'JJ'

if len(lines_Ll) == 0:
    df_Ll = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_Ll','archivo'])
else:
    df_Ll = DataFrame (lines_Ll)
    df_Ll=df_Ll[0].str.split(',',expand=True)
    df_Ll.rename(columns={0: 'cantidad_Ll', 1: 'archivo'}, inplace=True)
    df_Ll["cantidad_Ll"] = df_Ll.cantidad_Ll.astype(float)
    df_Ll=df_Ll.groupby(['archivo'])['cantidad_Ll'].agg('sum').reset_index()
    df_Ll['Name'] = 'Ll'

if len(lines_SL) == 0:
    df_SL = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_SL','archivo'])
else:
    df_SL = DataFrame (lines_SL)
    df_SL=df_SL[0].str.split(',',expand=True)
    df_SL.rename(columns={0: 'cantidad_SL', 1: 'archivo'}, inplace=True)
    df_SL["cantidad_SL"] = df_SL.cantidad_SL.astype(float)
    df_SL = df_SL.groupby(['archivo'])['cantidad_SL'].agg('sum').reset_index()
    df_SL['Name'] = 'SL'

df_total=pd.concat([df_Al, df_AN,df_JJ,df_Ll,df_SL])
df_total = df_total.fillna(0)
df_total['total'] = df_total['cantidad_Al'] + df_total['cantidad_AN'] + df_total['cantidad_Juan'] + df_total['cantidad_Ll'] +df_total['cantidad_SL']
df_total = df_total.groupby(['archivo'])['total'].agg('max').reset_index()
#df_cruzar.columns.values
df_total['total'] = df_total['total'].astype(str)
df_total['archivo'] = df_total['archivo'].astype(str)
df_total['LLAVE']  = df_total['archivo'] + df_total['total']

df_cruzar = pd.concat([df_Al, df_AN,df_JJ,df_Ll,df_SL])
df_cruzar = df_cruzar.fillna(0)
df_cruzar['total'] = df_cruzar['cantidad_Al'] + df_cruzar['cantidad_AN'] + df_cruzar['cantidad_Juan'] + df_cruzar['cantidad_Ll'] +df_cruzar['cantidad_SL']
df_cruzar['total'] = df_cruzar['total'].astype(str)
df_cruzar['archivo'] = df_cruzar['archivo'].astype(str)
df_cruzar['LLAVE']  = df_cruzar['archivo'] + df_cruzar['total']

df_ordenado=df_total.merge(df_cruzar.rename({'LLAVE': 'LLAVE2'}, axis=1), left_on='LLAVE', right_on='LLAVE2', how='left')
df_ordenado_Al=df_ordenado.loc[df_ordenado['Name'] == "Al"]
df_ordenado_Al=df_ordenado_Al['archivo_x'].tolist()
df_ordenado_Al= [df_ordenado_Al.replace(' ', '') for df_ordenado_Al in df_ordenado_Al]
df_ordenado_Ll=df_ordenado.loc[df_ordenado['Name'] == "Ll"]
df_ordenado_Ll=df_ordenado_Ll['archivo_x'].tolist()
df_ordenado_Ll= [df_ordenado_Ll.replace(' ', '') for df_ordenado_Ll in df_ordenado_Ll]
df_ordenado_AN=df_ordenado.loc[df_ordenado['Name'] == "AN"]
df_ordenado_AN=df_ordenado_AN['archivo_x'].tolist()
df_ordenado_AN = [df_ordenado_AN.replace(' ', '') for df_ordenado_AN in df_ordenado_AN]
df_ordenado_JJ=df_ordenado.loc[df_ordenado['Name'] == "JJ"]
df_ordenado_JJ=df_ordenado_JJ['archivo_x'].tolist()
df_ordenado_JJ= [df_ordenado_JJ.replace(' ', '') for df_ordenado_JJ in df_ordenado_JJ]
df_ordenado_SL=df_ordenado.loc[df_ordenado['Name'] == "SL"]
df_ordenado_SL=df_ordenado_SL['archivo_x'].tolist()
df_ordenado_SL= [df_ordenado_SL.replace(' ', '') for df_ordenado_SL in df_ordenado_SL]



##############
path = "C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_Al"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_Al:
    shutil.copy(f, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_Al')



path = "C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_AN"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_AN:
    shutil.copy(f, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_AN')


path = "C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_JJ"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_JJ:
    shutil.copy(f, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_JJ')


path = "C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_Ll"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_Ll:
    shutil.copy(f, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_Ll')


path = "C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_SL"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_SL:
    shutil.copy(f, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_SL')


path = "C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_vacio"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in vacios:
    shutil.copy(f, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_vacio')