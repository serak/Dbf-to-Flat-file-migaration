__author__ = 'serak'
import csv
import re
import dbf
from dbf import DbfError


branch_code = ''
start_no = 0
migrationdate = '2015-03-31'

def runmigration(branchcode, firstcustno):
    global start_no
    global branch_code
    skipcurrent = False
    skiploan = False
    skipissuedcheck = False
    branch_code = branchcode
    seqno = 0
    start_no = firstcustno
    log = open("logfiles.txt", 'w')
    log.writelines('Account Number\t' + 'Full Name\t' + 'Balance\t' + 'Product Type\t' + 'Comment\n')
    tablesaving = dbf.Table("SAVFILE.DBF")  # Open Saving file (SAVFILE)
    try:
        tablecurent = dbf.Table("FILE03.DBF")  # Open Current file(FILE03)
    except DbfError:
        print "---------------Current File not Fount : measures taken : SKIP-----------------"
        skipcurrent = True
    try:
        tableloan = dbf.Table("LONFILE.DBF")  # Open Loan file(LONFILE)
    except DbfError:
        print "---------------Loan File not Found : measures taken : SKIP -------------------"
        skiploan = True
    # ignorelist = open('ignorelist.txt','r').read().splitlines()
    # jointaccount = open('Joint.txt','r').read().splitlines()
    partycommonfile = open(branch_code + "-PartyCommon.csv", 'wb')
    partydetailfile = open(branch_code + "-PartyPersonDetails.csv", 'wb')
    PartyEnterprisefile = open(branch_code + "-PartyEnterpriseDetails.csv", 'wb')
    oldtonewfile = open(branch_code + "-oldtonew.dat", 'wb')
    accountfile = open(branch_code + "-Account.csv", 'wb')
    csvwriterPC = csv.writer(partycommonfile, delimiter='|')
    csvwriterPD = csv.writer(partydetailfile, delimiter='|')
    csvwriterAC = csv.writer(accountfile, delimiter='|')
    csvwriterPE = csv.writer(PartyEnterprisefile, delimiter='|')
    # csvwriterJA = csv.writer(JointAccountfile,delimiter='|')
    csvwriterOLD2NEW = csv.writer(oldtonewfile, delimiter='|')
    rowpartyc = []
    rowpartyd = []
    rowaccount = []
    rowpartyE = []
    rowold2new = []
    # Start of Savings Migration
    tablesaving.open()
    balancesaving = 0.00
    frozenbalance = 0.00
    for rec in tablesaving:
        # ----------------------------------------------------Party Common for Savings    -----------------------------------------------------
        try:
            if rec.balance == None or rec.balance == 0:  # Skip migration if balance field is empty
                log.writelines(
                    str(rec.acount_no) + '\t' + rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.g_fa_name.strip() + '\t' + `rec.balance` + '\tSAVINGS\tAccount balance Null or 0.00\n')
                continue
        except ValueError:  # if balance cannot be Parced skip it.
            log.writelines(
                str(rec.acount_no) + '\t' + rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.g_fa_name.strip() + '\t' + '.' + '\tSAVINGS\tBalance not a Number\n')
            continue
        rowpartyc.append(`(seqno + int(start_no))`.zfill(7))
        if rec.acoun_type in ([1, 2, 3]):
            rowpartyc.append('1063')
        else:
            rowpartyc.append('1062')
        rowpartyc.append('Individual')
        rowpartyc.append('FULL')
        rowpartyc.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.g_fa_name.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()+' '+rec.and_or2.strip()+' '+rec.first_nam3.strip()+' '+rec.last_nam3.strip()).strip())
        rowpartyc.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.g_fa_name.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()+' '+rec.and_or2.strip()+' '+rec.first_nam3.strip()+' '+rec.last_nam3.strip()).strip())
        rowpartyc.append((rec.first_nam1.strip()+' '+rec.last_nam1.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()).strip()[:50]) #Short Name
        rowpartyc.append((rec.first_nam1.strip()+' '+rec.last_nam1.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()).strip()[:50]) #Equivalent Short Name
        rowpartyc.append('1970-01-01')
        rowpartyc.append('superit')
        rowpartyc.append('N')
        rowpartyc.append(rec.first_nam1.strip()[:3] + rec.last_nam1.strip()[:3])
        rowpartyc.append(branch_code.zfill(8))
        rowpartyc.append('BF')
        rowpartyc.append('BF')
        rowpartyc.append(('Woreda ' + str(rec.wereda).strip()) if str(rec.wereda).strip() else None)
        rowpartyc.append(('kebele ' + str(rec.kebele).strip()) if str(rec.kebele).strip() else None)
        rowpartyc.append(('House No ' + str(rec.house_no).strip()) if str(rec.house_no).strip() else None)
        rowpartyc.append(('Phone No ' + ('9' + str(rec.tel_no).strip()).zfill(10)) if str(rec.tel_no).strip() else None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append('000000')
        rowpartyc.append(rec.city.strip())
        rowpartyc.append('2014-10-31')
        rowpartyc.append('2014-10-31')
        rowpartyc.append((rec.first_nam1.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.and_or2.strip()+' '+rec.first_nam3.strip()).strip()[:49])
        rowpartyc.append(None)
        rowpartyc.append('POST')
        rowpartyc.append('Y')
        rowpartyc.append('N')
        rowpartyc.append('N')
        rowpartyc.append('Y')
        rowpartyc.append('ETH')
        rowpartyc.append('UNKNOWN')
        rowpartyc.append('N')
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)
        rowpartyc.append(None)


        # Party Detail for Savings     -----------------------------------------------------

        rowpartyd.append(`(seqno + int(start_no))`.zfill(7))
        rowpartyd.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.g_fa_name.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()+' '+rec.and_or2.strip()+' '+rec.first_nam3.strip()+' '+rec.last_nam3.strip()).strip())
        rowpartyd.append(rec.first_nam1.strip())
        rowpartyd.append(rec.last_nam1.strip() if rec.last_nam1.strip() else rec.first_nam1.strip())
        if rec.last_nam1.strip():
            rowpartyd.append(
                rec.g_fa_name.strip() if rec.g_fa_name.strip() else rec.last_nam1.strip())  # If Grand Father name is empty assign fname istead.
        else:
            rowpartyd.append(rec.first_nam1.strip())  # IF NEITHER GNAME NOR LASTNAME IS FOUND USE FIRST NAME
        rowpartyd.append(None)
        rowpartyd.append('Mr.')
        rowpartyd.append('National ID')
        rowpartyd.append(None)
        rowpartyd.append('1969-12-01')
        rowpartyd.append('N')
        rowpartyd.append('Officially Employed')
        rowpartyd.append('Single')
        rowpartyd.append('Male')
        rowpartyd.append('ETH')
        rowpartyd.append('ETH')
        rowpartyd.append('ETH')
        rowpartyd.append('UNKNOWN')
        rowpartyd.append('Resident')
        rowpartyd.append('0')
        rowpartyd.append(
            rec.last_nam1.strip() if rec.last_nam1.strip() else rec.first_nam1.strip())  # MOTHERS MAIDEN NAME ????
        rowpartyd.append(rec.last_nam1.strip() if rec.last_nam1.strip() else rec.first_nam1.strip())
        rowpartyd.append(None)
        rowpartyd.append(None)
        rowpartyd.append(None)
        rowpartyd.append(None)
        rowpartyd.append(None)
        rowpartyd.append('1970-01-01')
        rowpartyd.append('1970-01-01')
        rowpartyd.append('Cash')
        rowpartyd.append('Monthly')
        rowpartyd.append('ETB')
        rowpartyd.append('0.000000')
        rowpartyd.append('Officially Employed')
        rowpartyd.append(None)
        rowpartyd.append(None)
        rowpartyd.append('2020-01-01')
        rowpartyd.append(None)
        rowpartyd.append(None)
        rowpartyd.append('dummyAddr')
        rowpartyd.append(
            rec.g_fa_name.strip() if rec.g_fa_name.strip() else rec.last_nam1.strip())  # If Grand Father name is empty assign fname instead.


        # Account Table for Saving---------------------------------------------------------------------------


        rowaccount.append(`(seqno + int(start_no))`.zfill(7))
        rowold2new.append(str(rec.acount_no).strip())
        if rec.acoun_type == 1:
            rowaccount.append('01322' + `(seqno + int(start_no))`.zfill(7) + '00')
            rowold2new.append('01322' + `(seqno + int(start_no))`.zfill(7) + '00')
            rowaccount.append('01322DEFAULTETB')
        elif rec.acoun_type == 2:
            rowaccount.append('01321' + `(seqno + int(start_no))`.zfill(7) + '00')
            rowold2new.append('01321' + `(seqno + int(start_no))`.zfill(7) + '00')
            rowaccount.append('01321DEFAULTETB')
        elif rec.acoun_type == 3:
            rowaccount.append('01323' + `(seqno + int(start_no))`.zfill(7) + '00')
            rowold2new.append('01323' + `(seqno + int(start_no))`.zfill(7) + '00')
            rowaccount.append('01323DEFAULTETB')
        else:
            rowaccount.append('01320' + `(seqno + int(start_no))`.zfill(7) + '00')
            rowold2new.append('01320' + `(seqno + int(start_no))`.zfill(7) + '00')
            rowaccount.append('01320DEFAULTETB')
        rowaccount.append('Savings')
        rowaccount.append(branch_code.zfill(8))
        rowaccount.append('ETB')
        rowaccount.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.g_fa_name.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()+' '+rec.and_or2.strip()+' '+rec.first_nam3.strip()+' '+rec.last_nam3.strip()).strip())
        rowaccount.append('Activated')
        rowaccount.append('0')
        rowaccount.append(rec.balance)   #CLEARED BALANCE
        try:
            rowaccount.append(rec.balance + (rec.froz_amt if rec.froz_amt else 0))   #BOOKED BALANCE
            rowaccount.append(rec.froz_amt if rec.froz_amt else 0.00)   #BLOCKED BALANCE
        except ValueError:
            rowaccount.append(rec.balance + 0)
            rowaccount.append('0')
        rowaccount.append('0.000000')
        rowaccount.append(rec.last_i_dat if rec.last_i_dat else migrationdate)  # Last Interest Acrual Date
        rowaccount.append('0.000000')
        rowaccount.append('0.000000')
        rowaccount.append('0.000000')
        rowaccount.append('0.000000')
        rowaccount.append('0')  # Both Cr and Dr Limits not applicable
        rowaccount.append('0')  # Do Not Allow Excess
        rowaccount.append('2020-01-01')
        rowaccount.append('2020-01-01')
        rowaccount.append('N')  # IF AND_OR IS NOT EMPTY THE ACC. IS JOINT
        rowaccount.append('1970-01-01')
        rowaccount.append(None)
        rowaccount.append(rec.last_p_dat if rec.last_p_dat else migrationdate)  #
        rowaccount.append('1970-01-01')
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append('1970-01-01')
        rowaccount.append('Y' if (rec.count_stat == 9) else 'N') # Dormancy Status
        rowaccount.append('1970-01-01')
        rowaccount.append('1970-01-01')
        rowaccount.append(rec.last_p_dat if rec.last_p_dat else migrationdate)
        rowaccount.append('N')
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append('Monthly')
        rowaccount.append('1')
        rowaccount.append('0')
        rowaccount.append(None)
        rowaccount.append('ACCSTMTCONFIG1')
        rowaccount.append('BS20020')
        rowaccount.append(branch_code.zfill(8))
        rowaccount.append('BS20020')
        rowaccount.append(branch_code.zfill(8))
        rowaccount.append(None)
        rowaccount.append(branch_code.zfill(8))
        rowaccount.append('0111') # Credit Base Code
        rowaccount.append('5')    # Base interest   rate
        rowaccount.append('0')    # Margin interest rate
        rowaccount.append('0')
        rowaccount.append('360')
        rowaccount.append('2')
        rowaccount.append('5')
        rowaccount.append('0')
        rowaccount.append('0')
        rowaccount.append('Monthly')
        rowaccount.append('1')
        rowaccount.append('30')
        rowaccount.append(None)
        rowaccount.append('01320-1E')
        rowaccount.append(branch_code.zfill(8))
        rowaccount.append(None)
        rowaccount.append('Y')
        rowaccount.append('0')
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(branch_code.zfill(8))
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append(None)
        rowaccount.append('2015-01-01')
        rowaccount.append(branch_code.zfill(8))
        rowaccount.append('1970-01-01')
        rowaccount.append('1970-01-01')
        rowaccount.append(rec.min_bal)
        rowaccount.append(migrationdate)
        rowaccount.append(None)
        rowaccount.append('ETB')
        rowaccount.append(None)
        rowaccount.append('1969-01-01')
        rowaccount.append('1969-01-01')

        if rec.acoun_type in ([1, 2, 3]):
            rowpartyE.append(`(seqno + int(start_no))`.zfill(7))
            rowpartyE.append('Professional Business')
            rowpartyE.append('Others')
            rowpartyE.append('Established in this country (switching)')
            rowpartyE.append('ETH')
            rowpartyE.append('ETH')
            rowpartyE.append('ETB')
            rowpartyE.append('1969-12-01')
            rowpartyE.append('1969-12-01')
            rowpartyE.append('UNKNOWN')
            rowpartyE.append('0')
            rowpartyE.append('0')
            rowpartyE.append('0')
            rowpartyE.append('0')
            rowpartyE.append('0')
            rowpartyE.append('0')
            rowpartyE.append('Y')
            rowpartyE.append(None)
            rowpartyE.append(None)
            rowpartyE.append(None)
            rowpartyE.append('BOTH')
            rowpartyE.append('AFFINITY')
            rowpartyE.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.g_fa_name.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()+' '+rec.and_or2.strip()+' '+rec.first_nam3.strip()+' '+rec.last_nam3.strip()).strip()[:99])
            rowpartyE.append(None)
            rowpartyE.append(None)
            rowpartyE.append(None)
            rowpartyE.append(None)
            rowpartyE.append(None)
            csvwriterPE.writerow(rowpartyE)  # write PartyEnterPriseDetail Row

        # -------- OLD to new block ---------------------------------------------

        rowold2new.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.g_fa_name.strip()+' '+rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()+' '+rec.and_or2.strip()+' '+rec.first_nam3.strip()+' '+rec.last_nam3.strip()).strip())
        rowold2new.append('Savings')
        rowold2new.append(branch_code.zfill(8))

        csvwriterPC.writerow(rowpartyc)  # write single party common row
        csvwriterPD.writerow(rowpartyd)  # write single party Detail row
        csvwriterAC.writerow(rowaccount)
        csvwriterOLD2NEW.writerow(rowold2new)
        rowpartyc[:] = []  # Clear party common row for nextloop() data storage
        rowpartyd[:] = []  # Clear party detail row for nextloop() data storage
        rowaccount[:] = []
        rowold2new[:] = []
        rowpartyE[:] = []
        balancesaving += rec.balance
        try:
            frozenbalance+= rec.froz_amt if rec.froz_amt else 0
        except ValueError:
            frozenbalance+=0
        seqno += 1

        # End of Saving Migration

    print '--' * 40
    print "The No of S|A Accounts Migrated----------- > " + `seqno`
    print "The Last Migrated Party ID---------------- > " + `(seqno - 1) + int(start_no)`.zfill(7)
    print "Total Saving Booked Balance  ------------- > " + "{0:,.2f}".format(balancesaving+frozenbalance) + '\n'

    temp = seqno

    # Start of Current Migration ------------------------------------------------------------------CURRENT ----------------------------------------------------------
    if not skipcurrent:
        try:
            chkfile = dbf.Table('CHECKFIL.DBF')
            chkfile.open()
        except DbfError:
            skipissuedcheck = True
        if not skipissuedcheck:
            issuedcheques = open(branch_code+'-ChequeBookDetails.csv', 'wb')
            csvwriterissuedchq = csv.writer(issuedcheques, delimiter='|')
            checks = []
            tempaccno = ''
        balancecurrent = 0.00
        balanceod = 0.00
        tablecurent.open()
        joint = {'A':'AND','AO':'AND/OR','O':'OR','F':'FOR','':''}
        for rec in tablecurent:
            # -------------------------------------------------Party Common for Current   ---------------------------------------------------------
            try:
                if rec.lastbal == None or rec.lastbal == 0:  # Skip migration if balance field is empty
                    log.writelines(
                        str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + `rec.lastbal` + '\tCURRENT\tAccount balance Null or 0.00\n')
                    continue
            except ValueError:  # if balance cannot be parced skip it
                log.writelines(
                    str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + '.' + '\tCURRENT\tBalance not a Number\n')
                continue
            rowpartyc.append(`(seqno + int(start_no))`.zfill(7))
            if rec.cust_code in ([2, 3, 4, 5]):
                rowpartyc.append('1063')
            else:
                rowpartyc.append('1062')
            rowpartyc.append('Individual')
            rowpartyc.append('FULL')
            rowpartyc.append((rec.name1.strip() + ' ' + rec.name2.strip() + ' ' +rec.name3.strip()+' '+ joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+rec.jname2.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()+' '+rec.j2name2.strip()+' '+rec.j2name3.strip()).strip())
            rowpartyc.append((rec.name1.strip() + ' ' + rec.name2.strip() + ' ' +rec.name3.strip()+' '+ joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+rec.jname2.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()+' '+rec.j2name2.strip()+' '+rec.j2name3.strip()).strip())
            rowpartyc.append((rec.name1.strip()+' '+ rec.name2.strip() + ' '+joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()).strip()[:59])   # Short Name
            rowpartyc.append((rec.name1.strip()+' '+ rec.name2.strip() + ' '+joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()).strip()[:59])   # Equivalent short name
            rowpartyc.append('1970-01-01')
            rowpartyc.append('superit')
            rowpartyc.append('N')
            rowpartyc.append(rec.name1.strip()[:3] + rec.name2.strip()[:3])
            rowpartyc.append(branch_code.zfill(8))
            rowpartyc.append('BF')
            rowpartyc.append('BF')
            rowpartyc.append('Woreda ' + str(rec.woreda).strip())
            rowpartyc.append('kebele ' + str(rec.kebele).strip())
            rowpartyc.append('House No ' + str(rec.houseno).strip())
            rowpartyc.append('Phone No ' + ('9' + str(rec.tele).strip()).zfill(10))
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append('000000')
            rowpartyc.append(rec.town.strip())
            rowpartyc.append('2014-10-31')
            rowpartyc.append('2014-10-31')
            rowpartyc.append((rec.name1.strip()+' '+joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()).strip()[:50])
            rowpartyc.append(None)
            rowpartyc.append('POST')
            rowpartyc.append('Y')
            rowpartyc.append('N')
            rowpartyc.append('N')
            rowpartyc.append('Y')
            rowpartyc.append('ETH')
            rowpartyc.append('UNKNOWN')
            rowpartyc.append('N')
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)

            # Party Detail for Current    -----------------------------------------------------------

            rowpartyd.append(`(seqno + int(start_no))`.zfill(7))
            rowpartyd.append((rec.name1.strip() + ' ' + rec.name2.strip() + ' ' +rec.name3.strip()+' '+ joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+rec.jname2.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()+' '+rec.j2name2.strip()+' '+rec.j2name3.strip()).strip())
            rowpartyd.append(rec.name1.strip())
            rowpartyd.append(rec.name2.strip())
            if rec.name2.strip():
                rowpartyd.append(rec.name3.strip() if rec.name3.strip() else rec.name2.strip())  # if no GFname assign Last name
            else:
                rowpartyd.append(rec.name1.strip())
            rowpartyd.append(None)
            rowpartyd.append('Mr.')
            rowpartyd.append('National ID')
            rowpartyd.append(None)
            rowpartyd.append('1969-12-01')
            rowpartyd.append('N')
            rowpartyd.append('Officially Employed')
            rowpartyd.append('Single')
            rowpartyd.append('Male')
            rowpartyd.append('ETH')
            rowpartyd.append('ETH')
            rowpartyd.append('ETH')
            rowpartyd.append('UNKNOWN')
            rowpartyd.append('Resident')
            rowpartyd.append('0')
            rowpartyd.append(rec.name2.strip() if rec.name2.strip() else rec.name1.strip())  # MOTHERS MAIDEN NAME ????
            rowpartyd.append(rec.name2.strip() if rec.name2.strip() else rec.name1.strip())  # Fathers Name
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append('1970-01-01')
            rowpartyd.append('1970-01-01')
            rowpartyd.append('Cash')
            rowpartyd.append('Monthly')
            rowpartyd.append('ETB')
            rowpartyd.append('0.000000')
            rowpartyd.append('Officially Employed')
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append('2020-01-01')
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append('dummyAddr')
            rowpartyd.append(
                rec.name3.strip() if rec.name3.strip() else rec.name2.strip())  # if no GFname assign Last name

            # Accounts for Current -----------------------------------------------------------------------------
            rowold2new.append(str(rec.acctno).strip())
            rowaccount.append(`(seqno + int(start_no))`.zfill(7))
            if rec.od_code == 1:
                rowaccount.append('01304' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01304' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01304DEFAULTETB')
                rowaccount.append('CurrentAccount')
            elif rec.od_code == 2:
                if rec.od_cata == 1:
                    rowaccount.append('01160' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowold2new.append('01160' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowaccount.append('01160DEFAULTETB')
                    rowaccount.append('OverDrafts')
                elif rec.od_cata == 2:
                    rowaccount.append('01155' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowold2new.append('01155' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowaccount.append('01155DEFAULTETB')
                    rowaccount.append('OverDrafts')
                elif rec.od_cata == 3:
                    rowaccount.append('01151' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowold2new.append('01151' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowaccount.append('01151DEFAULTETB')
                    rowaccount.append('OverDrafts')
                elif rec.od_cata == 4:
                    rowaccount.append('01165' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowold2new.append('01165' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowaccount.append('01165DEFAULTETB')
                    rowaccount.append('OverDrafts')
                elif rec.od_cata == 5:
                    rowaccount.append('01147' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowold2new.append('01147' + `(seqno + int(start_no))`.zfill(7) + '00')
                    rowaccount.append('01147DEFAULTETB')
                    rowaccount.append('OverDrafts')
                else:
                    log.writelines(
                        str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + `rec.balance` + '\tCURRENT\tUnknown OD catagorie from Current\n')
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append('ETB')
            rowaccount.append((rec.name1.strip() + ' ' + rec.name2.strip() + ' ' +rec.name3.strip()+' '+ joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+rec.jname2.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()+' '+rec.j2name2.strip()+' '+rec.j2name3.strip()).strip())
            rowaccount.append('Activated')
            rowaccount.append('0')
            rowaccount.append(rec.lastbal)  # Cleared Balance
            rowaccount.append(rec.lastbal)  # Booked Balance
            rowaccount.append('0')          # Blocked Balance
            rowaccount.append('0.000000')
            rowaccount.append(rec.lastdate if rec.lastdate else migrationdate)   # Last Interest Acrual Date
            rowaccount.append('0.000000')                                        # Accrued Credit Interest
            rowaccount.append(rec.accintrst if rec.accintrst else '0.000000')    # Accrued Debit  Interest
            rowaccount.append('0.000000')                                        # Credit Limit
            try:
                rowaccount.append(rec.od_limit if rec.od_limit else '0.000000')      # Debit Limit if Present(OD)
            except ValueError:
                rowaccount.append('0.000000')   # Debit Limit if Parse Error
            rowaccount.append('0' if rec.od_code == 1 else 2)  # if OD do something else.(Limit Excessation)
            rowaccount.append('0' if rec.od_code == 1 else 1)  # if OD do something else.(Limit Indicator)
            rowaccount.append('2020-01-01')
            rowaccount.append('2020-01-01')
            rowaccount.append('N')  # JOINT
            rowaccount.append('1970-01-01')
            rowaccount.append(None)
            rowaccount.append(rec.lastdate if rec.lastdate else migrationdate)   # Last Transaction Date
            rowaccount.append('1970-01-01')
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append('1970-01-01')
            rowaccount.append('Y' if (rec.statuscode == 4) else 'N')  # Dormancy Status
            rowaccount.append('1970-01-01')
            rowaccount.append('1970-01-01')
            rowaccount.append(rec.lastdate if rec.lastdate else migrationdate)   # Last Active Transaction date
            rowaccount.append('N')
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append('Monthly')
            rowaccount.append('1')
            rowaccount.append('0')
            rowaccount.append(None)
            rowaccount.append('ACCSTMTCONFIG1')
            if rec.od_code == 1:  # if Current
                rowaccount.append('BS14504')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS20B05')
            elif rec.od_code == 2:  # if OD
                if rec.od_cata == 1:
                    rowaccount.append('BS14802')
                    rowaccount.append(branch_code.zfill(8))
                    rowaccount.append('BS20B05')
                elif rec.od_cata == 2:
                    rowaccount.append('BS14604')
                    rowaccount.append(branch_code.zfill(8))
                    rowaccount.append('BS20B05')
                elif rec.od_cata == 3:
                    rowaccount.append('BS14504')
                    rowaccount.append(branch_code.zfill(8))
                    rowaccount.append('BS20B05')
                elif rec.od_cata == 4:
                    rowaccount.append('BS14905')
                    rowaccount.append(branch_code.zfill(8))
                    rowaccount.append('BS20B05')
                elif rec.od_cata == 5:
                    rowaccount.append('BS14404')
                    rowaccount.append(branch_code.zfill(8))
                    rowaccount.append('BS20B05')
                else:
                    log.writelines(
                        str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + `rec.balance` + '\tCURRENT\tUnknown Nominal Code (current and OD)\n')

            rowaccount.append(branch_code.zfill(8))
            rowaccount.append(None)
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append(None)
            rowaccount.append('0')  # Base interest   rate
            rowaccount.append('0')  # Margin interest rate
            rowaccount.append('0')
            rowaccount.append('365')
            rowaccount.append('1')
            rowaccount.append('0')
            rowaccount.append('0' if rec.od_code == 1 else None)  # ignore Acrual Method If OD
            rowaccount.append('0')
            rowaccount.append('Monthly')
            rowaccount.append('1')
            rowaccount.append('30')
            rowaccount.append(None)
            rowaccount.append('01304-1E' if rec.od_code == 1 else None)  # ignore CR Int Nominal Code if OD
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append(None)
            rowaccount.append('N' if rec.od_code == 1 else None)  # ignore WHT Applicble if OD
            rowaccount.append('0')
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append(str(rec.intrate*100))  # Debit Interest Rate
            rowaccount.append('365')
            rowaccount.append('1')
            rowaccount.append('0')
            rowaccount.append('0')
            if rec.od_code == 1:
                rowaccount.append('01304-1I')
            elif rec.od_code == 2:
                if rec.od_cata == 1:
                    rowaccount.append('01160-1I')
                elif rec.od_cata == 2:
                    rowaccount.append('01155-1I')
                elif rec.od_cata == 3:
                    rowaccount.append('01151-1I')
                elif rec.od_cata == 4:
                    rowaccount.append('01165-1I')
                elif rec.od_cata == 5:
                    rowaccount.append('01147-1I')
                else:
                    log.writelines(
                        str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + `rec.balance` + '\tCURRENT\tUnknown DR int Nominal Code (current and OD)\n')
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append(None)
            rowaccount.append('M')
            rowaccount.append('1')
            rowaccount.append('30')
            rowaccount.append(None)
            rowaccount.append('0.000000')
            rowaccount.append(None)
            rowaccount.append('2015-01-01')
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append('1970-01-01')
            rowaccount.append('1970-01-01')
            rowaccount.append(rec.min_bal if rec.min_bal else '0')
            rowaccount.append(migrationdate)
            rowaccount.append(None)
            rowaccount.append('ETB')
            rowaccount.append(None)
            rowaccount.append('1969-01-01')
            rowaccount.append('1969-01-01')

            # ---------------------  Party EnterPriseDetail Table for Current---------------------------------------------------------------------------
            if rec.cust_code in ([2, 3, 4, 5]):
                rowpartyE.append(`(seqno + int(start_no))`.zfill(7))
                rowpartyE.append('Professional Business')
                rowpartyE.append('Others')
                rowpartyE.append('Established in this country (switching)')
                rowpartyE.append('ETH')
                rowpartyE.append('ETH')
                rowpartyE.append('ETB')
                rowpartyE.append('1969-12-01')
                rowpartyE.append('1969-12-01')
                rowpartyE.append('UNKNOWN')
                rowpartyE.append('0')
                rowpartyE.append('0')
                rowpartyE.append('0')
                rowpartyE.append('0')
                rowpartyE.append('0')
                rowpartyE.append('0')
                rowpartyE.append('Y')
                rowpartyE.append(None)
                rowpartyE.append(None)
                rowpartyE.append(None)
                rowpartyE.append('BOTH')
                rowpartyE.append('AFFINITY')
                rowpartyE.append((rec.name1.strip() + ' ' + rec.name2.strip() + ' ' +rec.name3.strip()+' '+ joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+rec.jname2.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()+' '+rec.j2name2.strip()+' '+rec.j2name3.strip()).strip()[:99])
                rowpartyE.append(None)
                rowpartyE.append(None)
                rowpartyE.append(None)
                rowpartyE.append(None)
                rowpartyE.append(None)
                csvwriterPE.writerow(rowpartyE)  # write PartyEnterPriseDetail Row

            # ---------------------------old to new --------------------------------

            rowold2new.append((rec.name1.strip() + ' ' + rec.name2.strip() + ' ' +rec.name3.strip()+' '+ joint[rec.andor.strip()]+' '+rec.jname1.strip()+' '+rec.jname2.strip()+' '+joint[rec.andor.strip()]+' '+rec.j2name1.strip()+' '+rec.j2name2.strip()+' '+rec.j2name3.strip()).strip())
            rowold2new.append('CurrentAccount')
            rowold2new.append(branch_code.zfill(8))

            # ------------------------------------- ISSUED CHEQUES ------------------
            if not skipissuedcheck:
                for chqrec in chkfile:
                    try:
                        if not chqrec.acctno.strip():
                            continue
                    except ValueError:
                        log.writelines(
                            str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + `rec.balance` + '\tCURRENT\tINVALID ACCOUNT NO.\n')
                        continue
                    if int(rec.acctno.strip()) == int(chqrec.acctno.strip()):
                        try:
                            if rec.od_code == 1:
                                tempaccno = (
                                    '01304' + `(seqno + int(start_no))`.zfill(7) + '00')  # check for OD or Current
                            elif rec.od_code == 2:
                                if rec.od_cata == 1:
                                    tempaccno = ('01160' + `(seqno + int(start_no))`.zfill(7) + '00')
                                elif rec.od_cata == 2:
                                    tempaccno = ('01155' + `(seqno + int(start_no))`.zfill(7) + '00')
                                elif rec.od_cata == 3:
                                    tempaccno = ('01151' + `(seqno + int(start_no))`.zfill(7) + '00')
                                elif rec.od_cata == 4:
                                    tempaccno = ('01165' + `(seqno + int(start_no))`.zfill(7) + '00')
                                elif rec.od_cata == 5:
                                    tempaccno = ('01147' + `(seqno + int(start_no))`.zfill(7) + '00')
                                else:
                                    log.writelines(
                                        str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + `rec.balance` + '\tCURRENT\tUnknown OD catagie from issuedcheque\n')
                            checks.append(tempaccno)
                            checks.append(chqrec.date_1)
                            checks.append(re.sub('[\D]', '', chqrec.start_1))
                            checks.append(re.sub('[\D]', '', chqrec.endchk_1))
                            checks.append('ACTIVATED')  # Cheque Book Issue Status
                            checks.append('superit')  # ActivatedBy
                            checks.append('superit')  # LastModifiedBy
                            checks.append('BRANCHADDRESS')  # delivery instruction
                            checks.append(chqrec.date_4)    # Activation date
                            checks.append(branch_code.zfill(8))  # Address ID
                            if (int(re.sub('[\D]', '', chqrec.endchk_1)) - int(
                                    re.sub('[\D]', '', chqrec.start_1)) not in [24, 49, 99]):
                                print 'INVALID CHECK RANGE FOR ACCOUNT\t' + rec.acctno+'\t',(
                                    int(re.sub('[\D]', '', chqrec.endchk_1)) - int(
                                        re.sub('[\D]', '', chqrec.start_1)))
                            csvwriterissuedchq.writerow(checks)
                            checks[:] = []
                        except ValueError:
                            print 'INVALID VALUE ERROR ---------------- SKIPPING CURRENT CECKBOOK  accno ---> ' + rec.acctno
                            log.writelines(
                                str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + chqrec.start_1 + ' ' + chqrec.endchk_1 + '\tINVALID CHEQUE col - 1\n')
                            checks[:] = []
                        if chqrec.date_2:
                            try:
                                checks.append(tempaccno)
                                checks.append(chqrec.date_2)
                                checks.append(re.sub('[\D]', '', chqrec.start_2))
                                checks.append(re.sub('[\D]', '', chqrec.endchk_2))
                                checks.append('ACTIVATED')  # Cheque Book Issue Status
                                checks.append('superit')  # ActivatedBy
                                checks.append('superit')  # LastModifiedBy
                                checks.append('BRANCHADDRESS')  # delivery instruction
                                checks.append(chqrec.date_4)    # Activation date
                                checks.append(branch_code.zfill(8))  # Address ID
                                if int(re.sub('[\D]', '', chqrec.endchk_2)) - int(
                                        re.sub('[\D]', '', chqrec.start_2)) not in [24, 49, 99]:
                                    print 'INVALID CHECK RANGE FOR ACCOUNT\t' + rec.acctno+'\t',(
                                        int(re.sub('[\D]', '', chqrec.endchk_2)) - int(
                                            re.sub('[\D]', '', chqrec.start_2)))
                                csvwriterissuedchq.writerow(checks)
                                checks[:] = []
                            except ValueError:
                                print 'INVALID VALUE ERROR ---------------- SKIPPING CURRENT CECKBOOK  accno ---> ' + rec.acctno
                                log.writelines(
                                    str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + chqrec.start_2 + ' ' + chqrec.endchk_2 + '\tINVALID CHEQUE col - 2\n')
                                checks[:] = []
                        if chqrec.date_3:
                            try:
                                checks.append(tempaccno)
                                checks.append(chqrec.date_3)
                                checks.append(re.sub('[\D]', '', chqrec.start_3))
                                checks.append(re.sub('[\D]', '', chqrec.endchk_3))
                                checks.append('ACTIVATED')  # Cheque Book Issue Status
                                checks.append('superit')  # ActivatedBy
                                checks.append('superit')  # LastModifiedBy
                                checks.append('BRANCHADDRESS')  # delivery instruction
                                checks.append(chqrec.date_4)    # Activation date
                                checks.append(branch_code.zfill(8))  # Address ID
                                if (int(re.sub('[\D]', '', chqrec.endchk_3)) - int(
                                        re.sub('[\D]+', '', chqrec.start_3)) not in [24, 49, 99]):
                                    print 'INVALID CHECK RANGE FOR ACCOUNT\t' + rec.acctno+'\t',(
                                        int(re.sub('[\D]', '', chqrec.endchk_3)) - int(
                                            re.sub('[\D]', '', chqrec.start_3)))
                                csvwriterissuedchq.writerow(checks)
                                checks[:] = []
                            except ValueError:
                                print 'INVALID VALUE ERROR ---------------- SKIPPING CURRENT CECKBOOK  accno ---> ' + rec.acctno
                                log.writelines(
                                    str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + chqrec.start_3 + ' ' + chqrec.endchk_3 + '\tINVALID CHEQUE col - 3\n')
                                checks[:] = []
                        if chqrec.date_4:
                            try:
                                checks.append(tempaccno)                           # AccountID
                                checks.append(chqrec.date_4)                       # DateIssued
                                checks.append(re.sub('[\D]', '', chqrec.start_4))  # FromChequeNumber
                                checks.append(re.sub('[\D]', '', chqrec.endchk_4)) # ToChequeNumber
                                checks.append('ACTIVATED')  # Cheque Book Issue Status
                                checks.append('superit')    # ActivatedBy
                                checks.append('superit')    # LastModifiedBy
                                checks.append('BRANCHADDRESS')  # delivery instruction
                                checks.append(chqrec.date_4)    # Activation date
                                checks.append(branch_code.zfill(8))  # Address ID
                                if (int(re.sub('[\D]', '', chqrec.endchk_4)) - int(
                                        re.sub('[\D]', '', chqrec.start_4)) not in [24, 49, 99]):
                                    print 'INVALID CHECK RANGE FOR ACCOUNT\t' + rec.acctno+'\t',(
                                        int(re.sub('[\D]', '', chqrec.endchk_4)) - int(
                                            re.sub('[\D]', '', chqrec.start_4)))
                                csvwriterissuedchq.writerow(checks)
                                checks[:] = []
                            except ValueError:
                                print 'INVALID VALUE ERROR ---------------- SKIPPING CURRENT CECKBOOK  accno ---> ' + rec.acctno
                                log.writelines(
                                    str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + chqrec.start_4 + ' ' + chqrec.endchk_4 + '\tINVALID CHEQUE col - 4\n')
                                checks[:] = []
                        if chqrec.date_5:
                            try:
                                checks.append(tempaccno)
                                checks.append(chqrec.date_5)
                                checks.append(re.sub('[\D]+', '', chqrec.start_5))
                                checks.append(re.sub('[\D]+', '', chqrec.endchk_5))
                                checks.append('ACTIVATED')  # Cheque Book Issue Status
                                checks.append('superit')  # ActivatedBy
                                checks.append('superit')  # LastModifiedBy
                                checks.append('BRANCHADDRESS')  # delivery instruction
                                checks.append(chqrec.date_4)    # Activation date
                                checks.append(branch_code.zfill(8))  # Address ID
                                if (int(re.sub('[\D]', '', chqrec.endchk_5)) - int(
                                        re.sub('[\D]', '', chqrec.start_5)) not in [24, 49, 99]):
                                    print 'INVALID CHECK RANGE FOR ACCOUNT\t' + rec.acctno+'\t',(
                                        int(re.sub('[\D]', '', chqrec.endchk_5)) - int(
                                            re.sub('[\D]', '', chqrec.start_5)))
                                csvwriterissuedchq.writerow(checks)
                                checks[:] = []
                            except ValueError:
                                print 'INVALID VALUE ERROR ---------------- SKIPPING CURRENT CECKBOOK  accno ---> ' + rec.acctno
                                log.writelines(
                                    str(rec.acctno) + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + chqrec.start_5 + ' ' + chqrec.endchk_5 + '\tINVALID CHEQUE col - 5\n')
                                checks[:] = []

            csvwriterPC.writerow(rowpartyc)  # write single party common row
            csvwriterPD.writerow(rowpartyd)  # write single party Detail row
            csvwriterAC.writerow(rowaccount)
            csvwriterOLD2NEW.writerow(rowold2new)

            rowpartyc[:] = []  # Clear party common row for nextloop() data storage
            rowpartyd[:] = []  # Clear party detail row for nextloop() data storage
            rowaccount[:] = []
            rowold2new[:] = []
            rowpartyE[:] = []
            if rec.od_code == 1 or rec.od_code == None:
                balancecurrent += rec.lastbal
            else:
                balanceod+=rec.lastbal
            seqno += 1

        print '--' * 40
        print 'The No of Current Accounts Migrated----------- > ' + `seqno - temp`
        print "The Last Migrated Party ID-------------------- > " + `(seqno - 1) + int(start_no)`.zfill(7)
        print "Total Current Balance  ----------------------- > " + "{0:,.2f}".format(balancecurrent)
        print "Total OverDraft Balance  --------------------- > " + "{0:,.2f}".format(balanceod) + '\n'

    # ----  End of Current Accounts   ------------------------

    # Start of Loan Migration  -----------------------------------------------------------------------
    if not skiploan:
        temp = seqno
        accountnotesfile = open(branch_code + "-AccountNote.csv", 'wb')
        csvwriterAN = csv.writer(accountnotesfile, delimiter='|')
        rowaccnote = []
        balanceagriculture = 0.00
        balancemanufacture = 0.00
        balanceDTS = 0.00
        balancebuildandconst = 0.00
        balancepersonal = 0.00
        balanceimport = 0.00
        balanceexport = 0.00
        balanceTransport = 0.00
        balancemerchandize = 0.00
        balanceco_finance = 0.00
        balanceALD = 0.00
        balanceNPL = 0.00
        balanceStaff = 0.00
        tableloan.open()
        for rec in tableloan:
            # --------------------------------------------------Party Common for Loan   ----------------------------------------------------
            try:
                if rec.balance == 0 or rec.balance == None:  # Skip migration if balance field is empty
                    log.writelines(
                        `rec.acount_no` + '\t' + rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.last_nam1.strip() + '\t' + `rec.balance` + '\tLOAN\tAccount balance Null or 0.00\n')
                    continue
            except ValueError:
                log.writelines(
                    `rec.acctno` + '\t' + rec.name1.strip() + ' ' + rec.name2.strip() + ' ' + rec.name3.strip() + '\t' + '.' + '\tLOAN\tBalance not a Number\n')
                continue
            rowpartyc.append(`(seqno + int(start_no))`.zfill(7))
            rowpartyc.append('1062')  # All loan party to be Individual
            rowpartyc.append('Individual')
            rowpartyc.append('FULL')
            rowpartyc.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()).strip())
            rowpartyc.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()).strip())
            rowpartyc.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()).strip()[:60])
            rowpartyc.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()).strip()[:60])
            rowpartyc.append('1970-01-01')
            rowpartyc.append('superit')
            rowpartyc.append('N')
            rowpartyc.append(rec.first_nam1.strip()[:3] + rec.last_nam1.strip()[:3])
            rowpartyc.append(branch_code.zfill(8))
            rowpartyc.append('BF')
            rowpartyc.append('BF')
            rowpartyc.append(('Woreda ' + str(rec.wereda).strip()) if str(rec.wereda).strip() else None)
            rowpartyc.append(('kebele ' + str(rec.kebele).strip()) if str(rec.kebele).strip() else None)
            rowpartyc.append(('House No ' + str(rec.house_no).strip()) if str(rec.house_no).strip() else None)
            rowpartyc.append(
                ('Phone No ' + ('9' + str(rec.tel_no).strip()).zfill(10)) if str(rec.tel_no).strip() else None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append('000000' if not str(rec.box).strip() else str(rec.box).strip())
            rowpartyc.append(rec.city.strip())
            rowpartyc.append('2014-10-31')
            rowpartyc.append('2016-10-31')
            rowpartyc.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()).strip()[:50])
            rowpartyc.append(None)
            rowpartyc.append('POST')
            rowpartyc.append('Y')
            rowpartyc.append('N')
            rowpartyc.append('N')
            rowpartyc.append('Y')
            rowpartyc.append('ETH')
            rowpartyc.append('UNKNOWN')
            rowpartyc.append('N')
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)
            rowpartyc.append(None)


            # Party Detail for Loan --------------------------------------------------------

            rowpartyd.append(`(seqno + int(start_no))`.zfill(7))
            rowpartyd.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()).strip())
            rowpartyd.append(rec.first_nam1.strip())
            rowpartyd.append(rec.last_nam1.strip() if rec.last_nam1.strip() else rec.first_nam1.strip())
            if rec.last_nam1.strip():
                rowpartyd.append(rec.last_nam1.strip())  # If Grand Father name is empty assign fname istead.
            else:
                rowpartyd.append(rec.first_nam1.strip())
            rowpartyd.append(None)
            rowpartyd.append('Mr.')
            rowpartyd.append('National ID')
            rowpartyd.append(None)
            rowpartyd.append('1969-12-01')
            rowpartyd.append('N')
            rowpartyd.append('Officially Employed')
            rowpartyd.append('Single')
            rowpartyd.append('Male')
            rowpartyd.append('ETH')
            rowpartyd.append('ETH')
            rowpartyd.append('ETH')
            rowpartyd.append('UNKNOWN')
            rowpartyd.append('Resident')
            rowpartyd.append('0')
            rowpartyd.append(rec.last_nam1.strip() if rec.last_nam1.strip() else rec.first_nam1.strip())  # MOTHERS MAIDEN NAME ????
            rowpartyd.append(rec.last_nam1.strip() if rec.last_nam1.strip() else rec.first_nam1.strip())
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append('1970-01-01')
            rowpartyd.append('1970-01-01')
            rowpartyd.append('Cash')
            rowpartyd.append('Monthly')
            rowpartyd.append('ETB')
            rowpartyd.append('0.000000')
            rowpartyd.append('Officially Employed')
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append('2020-01-01')
            rowpartyd.append(None)
            rowpartyd.append(None)
            rowpartyd.append('dummyAddr')
            rowpartyd.append(rec.last_nam1.strip())

            # Account Table for Loan---------------------------------------------------------------------------

            rowold2new.append(str(rec.acount_no).strip())  # old account no. to oldto new
            rowaccount.append(`(seqno + int(start_no))`.zfill(7))
            if rec.acoun_type == None or rec.acoun_type == 0:
                rowaccount.append('01142' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01142' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01142' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01142DEFAULTETB')
                rowaccount.append('MigratedLoans')
            elif rec.acoun_type == 1:
                rowaccount.append('01146' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01146' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01146' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01146DEFAULTETB')
                rowaccount.append('MigratedLoans')
            elif rec.acoun_type == 2:
                rowaccount.append('01150' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01150' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01150' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01150DEFAULTETB')
                rowaccount.append('MigratedLoans')
            elif rec.acoun_type == 3:
                rowaccount.append('01164' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01164' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01164' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01164DEFAULTETB')
                rowaccount.append('MigratedLoans')
            elif rec.acoun_type == 4:
                rowaccount.append('01168' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01168' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01168' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01168DEFAULTETB')
                rowaccount.append('MigratedLoans')
            elif rec.acoun_type == 5:
                rowaccount.append('01159' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01159' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01159' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01159DEFAULTETB')
                rowaccount.append('MigratedLoans')
            elif rec.acoun_type == 6:
                rowaccount.append('01154' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01154' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01154' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01154DEFAULTETB')
                rowaccount.append('MigratedLoans')
            elif rec.acoun_type == 7:
                rowaccount.append('01144' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01144' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01144' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01144DEFAULTETB')
                rowaccount.append('MigratedLoans')
            elif rec.acoun_type == 8:
                rowaccount.append('01166' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01166' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01166' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01166DEFAULTETB')
                rowaccount.append('MerchantLoans')
            elif rec.acoun_type == 10:
                rowaccount.append('01175' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01175' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01175' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01175DEFAULTETB')
                rowaccount.append('OverDrafts')
            elif rec.acoun_type == 11:
                rowaccount.append('01180' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01180' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01180' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01180DEFAULTETB')
                rowaccount.append('OverDrafts')  # NPL Migrated as OverDraft Instead of MigratedLoan Type
            elif rec.acoun_type == 12:
                rowaccount.append('01168' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowold2new.append('01168' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccnote.append('01168' + `(seqno + int(start_no))`.zfill(7) + '00')
                rowaccount.append('01168DEFAULTETB')
                rowaccount.append('MigratedLoans')
            else:
                log.writelines(
                    `rec.acount_no` + '\t' + rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.last_nam1.strip() + '\t' + `rec.balance` + '\tLOAN\tUnknown Subproduct from Migrated loans\n')
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append('ETB')
            rowaccount.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()).strip())
            rowaccount.append('Activated')
            rowaccount.append('0')
            rowaccount.append(0 - rec.balance)   #cleared Balance IN DEBIT
            rowaccount.append(0 - rec.balance)   #Booked Balance in DEBIT
            rowaccount.append('0')               #Blocked Balance
            rowaccount.append('0.000000')
            rowaccount.append(
                rec.last_i_dat if rec.last_i_dat else migrationdate)  # migration date if last interest date is not defined
            rowaccount.append('0.000000')  # Accrued credit interest.
            rowaccount.append('0.000000') # Debit Accrued Interest
            rowaccount.append('0.000000') # Credit Limit
            rowaccount.append(rec.amt_grant if rec.amt_grant else '0.0')   # Debit Limit
            rowaccount.append('2')  # Allow Excess with Supervisor Authorization
            rowaccount.append('1')  # Dr limit applicable, not Cr limit
            rowaccount.append('2020-01-01')
            rowaccount.append('2020-01-01')
            rowaccount.append('N')  # IF AND_OR IS NOT EMPTY THE ACC. IS JOINT
            rowaccount.append('1970-01-01')
            rowaccount.append(None)
            rowaccount.append(rec.last_p_dat if rec.last_p_dat else migrationdate)
            rowaccount.append('1970-01-01')
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append('1970-01-01')
            rowaccount.append('N')
            rowaccount.append('1970-01-01')
            rowaccount.append('1970-01-01')
            rowaccount.append(rec.last_p_dat if rec.last_p_dat else migrationdate)
            rowaccount.append('N')
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append('Monthly')
            rowaccount.append('1')
            rowaccount.append('30')
            rowaccount.append(None)
            rowaccount.append('ACCSTMTCONFIG1')
            if rec.acoun_type == None or rec.acoun_type == 0:
                rowaccount.append('BS14300')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14300')
            elif rec.acoun_type == 1:
                rowaccount.append('BS14400')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14400')
            elif rec.acoun_type == 2:
                rowaccount.append('BS14500')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14500')
            elif rec.acoun_type == 3:
                rowaccount.append('BS14901')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14901')
            elif rec.acoun_type == 4:
                rowaccount.append('BS14B01')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14B01')
            elif rec.acoun_type == 5:
                rowaccount.append('BS14801')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14801')
            elif rec.acoun_type == 6:
                rowaccount.append('BS14600')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14600')
            elif rec.acoun_type == 7:
                rowaccount.append('BS14A01')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14A01')
            elif rec.acoun_type == 8:
                rowaccount.append('BS14B02')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14B02')
            elif rec.acoun_type == 10:
                rowaccount.append('BS14Z01')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14Z01')
            elif rec.acoun_type == 11:
                rowaccount.append('BS14C01')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14C01')
            elif rec.acoun_type == 12:
                rowaccount.append('BS14B01')
                rowaccount.append(branch_code.zfill(8))
                rowaccount.append('BS14B01')
            else:
                log.writelines(
                    `rec.acount_no` + '\t' + rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.last_nam1.strip() + '\t' + `rec.balance` + '\tLOAN\tUnknown nominal code (migrated loan)n\n')
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append(None)
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append(None)
            rowaccount.append(None)   # Base Credit interest  rate
            rowaccount.append(None)   # Margin interest rate
            rowaccount.append(rec.last_i_dat if rec.last_i_dat else migrationdate)
            rowaccount.append('365')
            rowaccount.append('1')
            rowaccount.append('0')
            rowaccount.append('0')
            rowaccount.append('0')
            rowaccount.append('Monthly')
            rowaccount.append('1')
            rowaccount.append('30')
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append('0')
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append(str(rec.int_rate*100))  # DEBIT INTEREST RATE
            rowaccount.append('365')
            rowaccount.append('1')
            rowaccount.append('0')
            rowaccount.append('0')
            if rec.acoun_type == None or rec.acoun_type == 0:  # DR Int Nominal Code  01144-1I,-----01168-1I
                rowaccount.append('01142-1I')
            elif rec.acoun_type == 1:
                rowaccount.append('01146-1I')
            elif rec.acoun_type == 2:
                rowaccount.append('01150-1I')
            elif rec.acoun_type == 3:
                rowaccount.append('01164-1I')
            elif rec.acoun_type == 4:
                rowaccount.append('01168-1I')
            elif rec.acoun_type == 5:
                rowaccount.append('01159-1I')
            elif rec.acoun_type == 6:
                rowaccount.append('01154-1I')
            elif rec.acoun_type == 7:
                rowaccount.append('01144-1I')
            elif rec.acoun_type == 8:
                rowaccount.append('01166-1I')
            elif rec.acoun_type == 10:
                rowaccount.append('01175-1I')
            elif rec.acoun_type == 11:
                rowaccount.append('01180-1I')
            elif rec.acoun_type == 12:
                rowaccount.append('01168-1I')
            else:
                log.writelines(
                    `rec.acount_no` + '\t' + rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.last_nam1.strip() + '\t' + `rec.balance` + '\tLOAN\tUnknown nominal code (migrated loan)n\n')
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append('30')
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append(None)
            rowaccount.append('2015-01-01')
            rowaccount.append(branch_code.zfill(8))
            rowaccount.append('1970-01-01')
            rowaccount.append('1970-01-01')
            rowaccount.append(None)
            rowaccount.append(migrationdate)
            rowaccount.append(None)
            rowaccount.append('ETB')
            rowaccount.append(None)
            rowaccount.append('1969-01-01')
            rowaccount.append('1969-01-01')


            #------------------Account Note's for Loan Accrued Interest Holder -------------------------------------------------------
            rowaccnote.append(rec.accr_int if rec.accr_int else '0')
            # --------------------old to new loan ------------------------------------------

            rowold2new.append((rec.first_nam1.strip() + ' ' + rec.last_nam1.strip() + ' ' + rec.and_or.strip()+' '+rec.first_nam2.strip()+' '+rec.last_name2.strip()).strip())
            rowold2new.append('MigratedLoans')
            rowold2new.append(branch_code.zfill(8))

            csvwriterPC.writerow(rowpartyc)  # write single party common row
            csvwriterPD.writerow(rowpartyd)  # write single party Detail row
            csvwriterAC.writerow(rowaccount)
            csvwriterOLD2NEW.writerow(rowold2new)
            csvwriterAN.writerow(rowaccnote)  # write Account Notes ROW

            rowpartyc[:] = []  # Clear party common row for nextloop() data storage
            rowpartyd[:] = []  # Clear party detail row for nextloop() data storage
            rowaccount[:] = []
            rowold2new[:] = []
            rowaccnote[:] = []
            if rec.acoun_type == 0 or rec.acoun_type == None:
                balanceagriculture += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 1:
                balancemanufacture += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 2:
                balanceDTS += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 3:
                balancebuildandconst += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 4:
                balancepersonal += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 5:
                balanceimport += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 6:
                balanceexport += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 7:
                balanceTransport += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 8:
                balancemerchandize += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 9:
                balanceco_finance += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 10:
                balanceALD += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 11:
                balanceNPL += rec.balance+(rec.accr_int if rec.accr_int else 0)
            elif rec.acoun_type == 12:
                balanceStaff += rec.balance+(rec.accr_int if rec.accr_int else 0)
            else:
                print "Unknow Loan Catagorie---------------"
            seqno += 1

        print '--' * 40
        print "The No of Loan Accounts Migrated----------------------------- > " + `seqno - temp`
        print "The Last Migrated Party ID----------------------------------- > " + `(seqno - 1) + int(start_no)`.zfill(7)
        print "Total Agricultural Loan balance + Accrued Interest----------- > " + "{0:,.2f}".format(0 - balanceagriculture)
        print "Total Manufacture Loan balance + Accrued Interest------------ > " + "{0:,.2f}".format(0 - balancemanufacture)
        print "Total DTS Loan Balance + Accrued nterest--------------------- > " + "{0:,.2f}".format(0 - balanceDTS)
        print "Total Export Loan Balance + Accrued Interest----------------- > " + "{0:,.2f}".format(0 - balanceexport)
        print "Total Import Loan Balance + Accrued Interest----------------- > " + "{0:,.2f}".format(0 - balanceimport)
        print "Total Building & Const. Loan Balance + Accrued Interest------ > " + "{0:,.2f}".format(0 - balancebuildandconst)
        print "Total Personal Loan Balance + Accrued Interest--------------- > " + "{0:,.2f}".format(0 - balancepersonal)
        print "Total Transportation Loan Balance + Accrued Interest--------- > " + "{0:,.2f}".format(0 - balanceTransport)
        print "Total Merchandize Loan Balance + Accrued Interest------------ > " + "{0:,.2f}".format(0 - balancemerchandize)
        print "Total Co-finance Loan Balance + Accrued Interest------------- > " + "{0:,.2f}".format(0 - balanceco_finance)
        print "Total ALD Loan Balance + Accrued Interest-------------------- > " + "{0:,.2f}".format(0 - balanceALD)
        print "Total NPL Loan Balance + Accrued Interest-------------------- > " + "{0:,.2f}".format(0 - balanceNPL)
        print "Total Staff Loan Balance + Accrued Interest------------------ > " + "{0:,.2f}".format(0 - balanceStaff) + '\n'

        accountnotesfile.close() # Close Loan Account notes
        # End of LAON Migration
    start_no = seqno + int(start_no)  # set start customer no for the next branch
    log.close()  # flush and write log file
    partycommonfile.close()  # flush and write party common file
    partydetailfile.close()  # flush and write party detail file
    accountfile.close()  # flush accounts and write to file
    oldtonewfile.close()  # flush old2new and write to file
    PartyEnterprisefile.close()  # flush party enterprise detail
