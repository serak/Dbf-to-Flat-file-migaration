import os
import migrat
import stopcheque
import time
path = os.getcwd()
folders = os.listdir(path)
print "Please Enter The Starting  Customer Number  ----- > ",
firstcustno = raw_input()
for folder in folders:
    if not len(folder.split('.')) > 1:
        os.chdir(path+'\\'+folder)
        print '--'*5, folder.split('-')[1].strip(), '--'*5
        print 'CURRENT WORKING DIRECTY SET TO - > '+folder
        migrat.runmigration(folder.split('-')[0].strip(),firstcustno)
        firstcustno = migrat.start_no
        #time.sleep(2)
        #stopcheque.process_stopcheck(folder.split('-')[0].strip()) 
        print 'LAST CUSTOMER NUMBER FOR ', folder.split('-')[1].strip()+' BRANCH', str(int(firstcustno)-1)
