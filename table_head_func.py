# Pfad zur CSV-Datei
file_path = '/home/kilian/Documents/Python_Project/modified_data_2.csv'

# Die erste Zeile der CSV-Datei einlesen
with open(file_path, 'r') as file:
    first_line = file.readline().strip()

print(first_line)
