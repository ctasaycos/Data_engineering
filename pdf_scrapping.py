#Cargas librerias
import os
from os import chdir, getcwd, listdir, path
import PyPDF2
from time import strftime
import docx2pdf
import glob
#docx_list = [docx_list.replace('\\', '\') for docx_list in docx_list]
#"Información"
#Palabras clave por ejecutivo
Allison  = ["ArcGIS", "Licencias","Drones","Visor_web","GPS","Licenciamiento","PIX4D"]
Juan_Jose  = ["Imágenes","Satelital", "Desarrollo","Ortoimagen","Mosaico","BIM","Imagenes","Satélite","Digital Globe","Quickbird","SPOT","Building","Modeling"]
Luis  = ["Sistemas", "Analisis","Análisis","Visualización","Plataforma","SuperGIS","SuperMAP","Catastro","Map Info","Map","Autodesk","Sistematización","Analyst","AutoCAD","Civil 3D","Revit","Inventor","Infraworks"]
Silvia  = ["GIS", "ENVI","Teledetección","Open","Source","Software libre","PCI Geomatics","Cartografia","Geomatics","SIG","IDL","Deep", "learning","Geográfica","Geomática","Georeferencial","Geoespacial","Geodiseño"]
Andres  = ["Servicios", "Consultoria","Soporte","Integración","Integracion"]

#Transformas todos los archivos pdf a txt
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

#Identificas todos los archivos txt
import glob
file_list = glob.glob('D:\seace/*.txt')
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

# wordcount("/Users/carlostasaycosilva/Downloads/webscrapping/log.txt",["artículo"])

#Escribes todos los archivos txt con los palabras clave en una lista
#Allison
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('D:\seace\log_Allison.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], Allison)
sys.stdout = orig_stdout
f.close()

#Lees lista con el txt donde esta el nombre de los archivos y palabras clave
with open('D:\seace\log_Allison.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_Allison = [lines.replace('.txt', '.pdf') for lines in lines]

#Andres
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('D:\seace\log_Andres.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], Andres)
sys.stdout = orig_stdout
f.close()

#Lees lista con el txt donde esta el nombre de los archivos y palabras clave
with open('D:\seace\log_Andres.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_Andres = [lines.replace('.txt', '.pdf') for lines in lines]

#Juan_Jose
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('D:\seace\log_Juan_Jose.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], Juan_Jose)
sys.stdout = orig_stdout
f.close()

#Lees lista con el txt donde esta el nombre de los archivos y palabras clave
with open('D:\seace\log_Juan_Jose.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_Juan_Jose = [lines.replace('.txt', '.pdf') for lines in lines]

#Luis
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('D:\seace\log_Luis.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], Luis)
sys.stdout = orig_stdout
f.close()

#Lees lista con el txt donde esta el nombre de los archivos y palabras clave
with open('D:\seace\log_Luis.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_Luis = [lines.replace('.txt', '.pdf') for lines in lines]

#Silvia
import sys
i = 0
a = []
orig_stdout = sys.stdout
f = open('D:\seace\log_Silvia.txt', 'w')
sys.stdout = f
for i in range(len(file_list)):
    wordcount(file_list[i], Silvia)
sys.stdout = orig_stdout
f.close()

#Lees lista con el txt donde esta el nombre de los archivos y palabras clave
with open('D:\seace\log_Silvia.txt') as g:
    lines = g.readlines()
lines = [lines.replace('\n', '') for lines in lines]
lines_Silvia = [lines.replace('.txt', '.pdf') for lines in lines]


##########Ordenando los conteos
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

if len(lines_Allison) == 0:
    df_Allison = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_Allison','archivo'])
else:
    df_Allison = DataFrame (lines_Allison)
    df_Allison=df_Allison[0].str.split(',',expand=True)
    df_Allison.rename(columns={0: 'cantidad_Allison', 1: 'archivo'}, inplace=True)
    df_Allison["cantidad_Allison"] = df_Allison.cantidad_Allison.astype(float)
    df_Allison=df_Allison.groupby(['archivo'])['cantidad_Allison'].agg('sum').reset_index()
    df_Allison['Name'] = 'Allison'

if len(lines_Andres) == 0:
    df_Andres = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_Andres','archivo'])
else:
    df_Andres = DataFrame (lines_Andres)
    df_Andres=df_Andres[0].str.split(',',expand=True)
    df_Andres.rename(columns={0: 'cantidad_Andres', 1: 'archivo'}, inplace=True)
    df_Andres["cantidad_Andres"] = df_Andres.cantidad_Andres.astype(float)
    df_Andres=df_Andres.groupby(['archivo'])['cantidad_Andres'].agg('sum').reset_index()
    df_Andres['Name'] = 'Andres'

if len(lines_Juan_Jose) == 0:
    df_Juan_Jose = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_Juan','archivo'])
else:
    df_Juan_Jose = DataFrame (lines_Juan_Jose)
    df_Juan_Jose=df_Juan_Jose[0].str.split(',',expand=True)
    df_Juan_Jose.rename(columns={0: 'cantidad_Juan', 1: 'archivo'}, inplace=True)
    df_Juan_Jose["cantidad_Juan"] = df_Juan_Jose.cantidad_Juan.astype(float)
    df_Juan_Jose=df_Juan_Jose.groupby(['archivo'])['cantidad_Juan'].agg('sum').reset_index()
    df_Juan_Jose['Name'] = 'Juan_Jose'

if len(lines_Luis) == 0:
    df_Luis = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_Luis','archivo'])
else:
    df_Luis = DataFrame (lines_Luis)
    df_Luis=df_Luis[0].str.split(',',expand=True)
    df_Luis.rename(columns={0: 'cantidad_Luis', 1: 'archivo'}, inplace=True)
    df_Luis["cantidad_Luis"] = df_Luis.cantidad_Luis.astype(float)
    df_Luis=df_Luis.groupby(['archivo'])['cantidad_Luis'].agg('sum').reset_index()
    df_Luis['Name'] = 'Luis'

if len(lines_Silvia) == 0:
    df_Silvia = pd.DataFrame(np.array([None, None]).reshape(-1,2),  columns=['cantidad_Silvia','archivo'])
else:
    df_Silvia = DataFrame (lines_Silvia)
    df_Silvia=df_Silvia[0].str.split(',',expand=True)
    df_Silvia.rename(columns={0: 'cantidad_Silvia', 1: 'archivo'}, inplace=True)
    df_Silvia["cantidad_Silvia"] = df_Silvia.cantidad_Silvia.astype(float)
    df_Silvia = df_Silvia.groupby(['archivo'])['cantidad_Silvia'].agg('sum').reset_index()
    df_Silvia['Name'] = 'Silvia'

df_total=pd.concat([df_Allison, df_Andres,df_Juan_Jose,df_Luis,df_Silvia])
df_total = df_total.fillna(0)
df_total['total'] = df_total['cantidad_Allison'] + df_total['cantidad_Andres'] + df_total['cantidad_Juan'] + df_total['cantidad_Luis'] +df_total['cantidad_Silvia']
df_total = df_total.groupby(['archivo'])['total'].agg('max').reset_index()
#df_cruzar.columns.values
df_total['total'] = df_total['total'].astype(str)
df_total['archivo'] = df_total['archivo'].astype(str)
df_total['LLAVE']  = df_total['archivo'] + df_total['total']

df_cruzar = pd.concat([df_Allison, df_Andres,df_Juan_Jose,df_Luis,df_Silvia])
df_cruzar = df_cruzar.fillna(0)
df_cruzar['total'] = df_cruzar['cantidad_Allison'] + df_cruzar['cantidad_Andres'] + df_cruzar['cantidad_Juan'] + df_cruzar['cantidad_Luis'] +df_cruzar['cantidad_Silvia']
df_cruzar['total'] = df_cruzar['total'].astype(str)
df_cruzar['archivo'] = df_cruzar['archivo'].astype(str)
df_cruzar['LLAVE']  = df_cruzar['archivo'] + df_cruzar['total']

df_ordenado=df_total.merge(df_cruzar.rename({'LLAVE': 'LLAVE2'}, axis=1), left_on='LLAVE', right_on='LLAVE2', how='left')
df_ordenado_Allison=df_ordenado.loc[df_ordenado['Name'] == "Allison"]
df_ordenado_Allison=df_ordenado_Allison['archivo_x'].tolist()
df_ordenado_Allison= [df_ordenado_Allison.replace(' ', '') for df_ordenado_Allison in df_ordenado_Allison]
df_ordenado_Luis=df_ordenado.loc[df_ordenado['Name'] == "Luis"]
df_ordenado_Luis=df_ordenado_Luis['archivo_x'].tolist()
df_ordenado_Luis= [df_ordenado_Luis.replace(' ', '') for df_ordenado_Luis in df_ordenado_Luis]
df_ordenado_Andres=df_ordenado.loc[df_ordenado['Name'] == "Andres"]
df_ordenado_Andres=df_ordenado_Andres['archivo_x'].tolist()
df_ordenado_Andres = [df_ordenado_Andres.replace(' ', '') for df_ordenado_Andres in df_ordenado_Andres]
df_ordenado_Juan_Jose=df_ordenado.loc[df_ordenado['Name'] == "Juan_Jose"]
df_ordenado_Juan_Jose=df_ordenado_Juan_Jose['archivo_x'].tolist()
df_ordenado_Juan_Jose= [df_ordenado_Juan_Jose.replace(' ', '') for df_ordenado_Juan_Jose in df_ordenado_Juan_Jose]
df_ordenado_Silvia=df_ordenado.loc[df_ordenado['Name'] == "Silvia"]
df_ordenado_Silvia=df_ordenado_Silvia['archivo_x'].tolist()
df_ordenado_Silvia= [df_ordenado_Silvia.replace(' ', '') for df_ordenado_Silvia in df_ordenado_Silvia]


#df_file_list.merge(df_Allison.rename({'archivo': 'archivo2'}, axis=1), left_on='archivo', right_on='archivo2', how='left')
#df_total.to_excel("D:\seace\ile.xlsx")
#df_ordenado.to_excel("D:\seace\llison.xlsx")
##############
#Exportas en nueva carpeta los pdfs seleccionados
path = "D:\seace\export_Allison"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_Allison:
    shutil.copy(f, 'D:\seace\export_Allison')


#Exportas en nueva carpeta los pdfs seleccionados
path = "D:\seace\export_Andres"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_Andres:
    shutil.copy(f, 'D:\seace\export_Andres')

#Exportas en nueva carpeta los pdfs seleccionados
path = "D:\seace\export_Juan_Jose"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_Juan_Jose:
    shutil.copy(f, 'D:\seace\export_Juan_Jose')

#Exportas en nueva carpeta los pdfs seleccionados
path = "D:\seace\export_Luis"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_Luis:
    shutil.copy(f, 'D:\seace\export_Luis')

#Exportas en nueva carpeta los pdfs seleccionados
path = "D:\seace\export_Silvia"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in df_ordenado_Silvia:
    shutil.copy(f, 'D:\seace\export_Silvia')

#Exportas en nueva carpeta los pdfs seleccionados
path = "D:\seace\export_vacio"
try:
    os.makedirs(path)
except OSError:
    print("Creacion de la carpeta%s fallo" % path)
else:
    print("Carpeta creada exitosamente %s" % path)

import shutil, os

for f in vacios:
    shutil.copy(f, 'D:\seace\export_vacio')