import dbf
import csv
import re
from dbf import DbfError
import os
skipstoppedcheck = False
branchcode = ''
stopcheckfile = ''
def process_stopcheck(branch_code):
    global branchcode
    global skipstoppedcheck
    branchcode = branch_code
    try:
        stopdbf = dbf.Table('STOPFILE.DBF')
        stopdbf.open()
    except DbfError:
        print '--'*5, 'STOP CHECK NOT FOUND ------------ SKIPPING ---------------------------------'
        skipstoppedcheck = True
    if not skipstoppedcheck:
        filelist = os.listdir(os.getcwd())
        for fil in filelist:
            if fil.startswith(branchcode) and fil.endswith('.dat'):
                stopcheckfile = fil
        oldtonew = open(stopcheckfile,'rb').readlines()
        stopfile = open (branchcode+'-StoppedCheques.csv','wb')
        stopwriter  = csv.writer(stopfile,delimiter='|')
        oldtonewrecord = []
        stopchecks = []
        for old2new in oldtonew:
            for rec in stopdbf:
                oldtonewrecord = old2new.split('|')
                if (int(rec.fld01) == int(oldtonewrecord[0])) and (oldtonewrecord[3] == 'CurrentAccount'):
                    stopchecks.append(oldtonewrecord[1])
                    stopchecks.append(rec.fld06)                         # Amount
                    stopchecks.append(re.sub('[\D]','',rec.fld02)) # From Stop Cheque Referrence
                    stopchecks.append('ETB')                             # Iso Currency Code
                    stopchecks.append(re.sub('[\D]','',rec.fld02)) # Stop Cheque Referrence
                    stopchecks.append(rec.fld04)                         # Stop Date
                    stopchecks.append(re.sub('[\D]','',rec.fld02)) # STOPPEDCHEQUEID
                    stopchecks.append('REQUEST BY CUSTOMER' if rec.fld06 > 0 else 'LOST CHECK')# Stop Reason
                    stopchecks.append(re.sub('[\D]','',rec.fld03)) # To Stop Cheque Referrence
                    stopchecks.append('superit')                         # User Id
                    print 'RANGE OF CHECK STOPPED FOR ACCOUNT.\t'+rec.fld01+ '\t',(int(re.sub('[\D]','',rec.fld03))-int(re.sub('[\D]','',rec.fld02)))
                    stopwriter.writerow(stopchecks)
                    stopchecks[:] = []
        stopfile.close()
