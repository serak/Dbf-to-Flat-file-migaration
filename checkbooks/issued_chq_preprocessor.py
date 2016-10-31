__author__ = 'serak shiferaw'
import dbf
import csv
import os

#os.chdir(r'C:\Users\user\Desktop\NEKEM DBASE\CURACT')
chkfile = dbf.Table('CHECKFIL')
issuedcheck = open ('1111 - issuedcheck.txt','wb')
csvwriter = csv.writer(issuedcheck,delimiter='|')
chkfile.open()
record = []
for rec in chkfile:
    record.append(int(rec.acctno))
    record.append(rec.date_1)
    record.append(rec.start_1)
    record.append(rec.endchk_1)
    csvwriter.writerow(record)
    record[:] = []
    if rec.date_2:
        record.append(int(rec.acctno))
        record.append(rec.date_2)
        record.append(rec.start_2)
        record.append(rec.endchk_2)
        csvwriter.writerow(record)
        record[:] = []
    if rec.date_3:
        record.append(int(rec.acctno))
        record.append(rec.date_2)
        record.append(rec.start_2)
        record.append(rec.endchk_2)
        csvwriter.writerow(record)
        record[:] = []
    if rec.date_4:
        record.append(int(rec.acctno))
        record.append(rec.date_2)
        record.append(rec.start_2)
        record.append(rec.endchk_2)
        csvwriter.writerow(record)
        record[:] = []
    if rec.date_5:
        record.append(int(rec.acctno))
        record.append(rec.date_2)
        record.append(rec.start_2)
        record.append(rec.endchk_2)
        csvwriter.writerow(record)
        record[:] = []
           
issuedcheck.close()
