import os
import PyPDF2
from time import strftime
import glob
import pandas as pd
import numpy as np
from pandas import DataFrame
import shutil

# Keywords for different categories and obfuscated keywords
Al = ["a", "b", "dd", "c", "d", "e", "f"]
JJ = ["a", "b", "dd", "c", "d", "e", "f"]
Ll = ["a", "b", "dd", "c", "d", "e", "f"]
SL = ["a", "b", "dd", "c", "d", "e", "f"]
AN = ["a", "b", "dd", "c", "d", "e", "f"]

# Function to check if the provided absolute path exists
def check_path(prompt):
    abs_path = input(prompt)
    while not os.path.exists(abs_path):
        print("\nEl campo no existe\n")
        abs_path = input(prompt)
    return abs_path

print("\n")
folder = check_path("Escriba la ruta: ")
file_list = []
directory = folder

# Convert PDFs to text files
for root, dirs, files in os.walk(directory):
    for filename in files:
        if filename.endswith('.pdf'):
            file_path = os.path.join(directory, filename)
            file_list.append(file_path)

for item in file_list:
    path = item
    head, tail = os.path.split(path)
    tail = tail.replace(".pdf", ".txt")
    name = os.path.join(head, tail)
    content = ""

    pdf = PyPDF2.PdfFileReader(path, "rb")
    for i in range(0, pdf.getNumPages()):
        try:
            content += pdf.getPage(i).extractText() + "\n"
        except:
            print(filename, "No se puede transformar por favor revisar")

    print(strftime("%H:%M:%S"), " pdf  -> txt ")

    with open(name, 'a', encoding='utf8') as out:
        out.write(content)

# Identify all text files
file_list = glob.glob('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss/*.txt')

# Identify empty files
vacios = [file.replace('.txt', '.pdf') for file in file_list if os.path.getsize(file) < 50]

# Function to count words within text files
def wordcount(filename, listwords):
    try:
        with open(filename, "r", encoding='utf8') as file:
            read = file.readlines()

        for word in listwords:
            lower = word.lower()
            count = 0

            for sentence in read:
                line = sentence.split()
                for each in line:
                    line2 = each.lower()
                    line2 = line2.strip("@#$%")
                    if lower == line2:
                        count += 1

            if count > 0:
                print(count, ",", filename)
    except FileNotFoundError:
        print("El archivo no está aquí")

# Count words for each keyword category
def count_and_export(keyword_list, log_filename, export_folder):
    with open(log_filename, 'w') as f:
        for i in range(len(file_list)):
            wordcount(file_list[i], keyword_list)

# Keywords count and export
count_and_export(Al, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_Al.txt', 'export_Al')
count_and_export(AN, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_AN.txt', 'export_AN')
count_and_export(JJ, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_JJ.txt', 'export_JJ')
count_and_export(Ll, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_Ll.txt', 'export_Ll')
count_and_export(SL, 'C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\log_SL.txt', 'export_SL')

# Combine dataframes
df_total = pd.concat([df.groupby(['archivo']).agg({'total': 'max'}).reset_index() for df in [df_Al, df_AN, df_JJ, df_Ll, df_SL]])

# Create folders and copy files based on categories
for df_category, export_folder_name in zip([df_ordenado_Al, df_ordenado_AN, df_ordenado_JJ, df_ordenado_Ll, df_ordenado_SL],
                                           ['export_Al', 'export_AN', 'export_JJ', 'export_Ll', 'export_SL']):
    export_path = os.path.join('C:\\Users\\ctasa\\OneDrive\\Documentos\\ss', export_folder_name)

    try:
        os.makedirs(export_path)
    except OSError:
        print("Creación de la carpeta %s falló" % export_path)
    else:
        print("Carpeta creada exitosamente %s" % export_path)

    for file_path in df_category:
        shutil.copy(file_path, export_path)

# Create folder for empty files
empty_folder_path = "C:\\Users\\ctasa\\OneDrive\\Documentos\\ss\\export_vacio"
try:
    os.makedirs(empty_folder_path)
except OSError:
    print("Creación de la carpeta %s falló" % empty_folder_path)
else:
    print("Carpeta creada exitosamente %s" % empty_folder_path)

for empty_file_path in vacios:
    shutil.copy(empty_file_path, empty_folder_path)
