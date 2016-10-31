__author__ = 'serak shiferaw'
import dbf
import csv
import os

#os.chdir(r'C:\Users\user\Desktop\NEKEM DBASE\CURACT')
chkfile = dbf.Table('STOPFILE')
issuedcheck = open ('1111 - StoppedCheque.txt','wb')
csvwriter = csv.writer(issuedcheck,delimiter='|')
chkfile.open()
record = []
for rec in chkfile:
    record.append(int(rec.fld01)) # account no
    record.append(rec.fld02) # strat no
    record.append(rec.fld03) # end no
    record.append(rec.fld04) # date
    record.append(rec.fld05) # date
    record.append(rec.fld06) # Amount
    csvwriter.writerow(record)
    record[:] = []

issuedcheck.close()
