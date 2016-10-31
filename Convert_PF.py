import csv
import os
import re
os.chdir(r'C:\Users\user\Desktop\PF')
inputfile = raw_input();
read = open(inputfile)
write = open('outputfile.dat','wb')
csvreader = csv.reader(read,delimiter='|')
csvwriter = csv.writer(write,delimiter='|')
row = []
temp = ''
for line in csvreader:
    row.append(line[0])
    row.append(line[1])
    temp = re.sub('[\D]','',line[2])
    #print temp
    if(len(temp)==13):
        row.append((temp[:5]+'0'+temp[5:]))
    else:
        row.append(temp)
    csvwriter.writerow(row)
    row[:] = []
write.close()
