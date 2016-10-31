DBASE TO FLAT FILE MIGRATION SCRIPT SAMPLE MADE FOR AWASH INTERNATIONAL BANK

Dear All 

This scripts are made to convert DBASE (*.dbf) databases to the company datamigration tool called talend

previosly the company used to migrate data's using manual copy and paste method using excel program and exporting it to csv(comma separated value)
DISADVANTAGE OF THIS METHOD WAS
- Prone to error , eg. moving one cell could result it wrong balance migration of accounts and customer data
- Time Consuming,eg, it would take more than 3 days tom migrate a single branch data 

so i wrote a custom Python script to automate the process of conversion
the struction of the migration data looks like this

CUSTOMER
|
|-SAVINGS  -- (FILE1.DBF)
|-CURRENT  -- (FILE2.DBF)
|-LOAN     -- (FILE3.DBF)
ACCOUNTS
|
|-SAVINGS
|-|-SAVING SAVING    -- (FILE1.DBF)
|-|-NORMAL SAVING    -- (FILE1.DBF)
|-|-CORPORATE SAVING -- (FILE1.DBF) 
|-CURRENT            -- (FILE2.DBF)
|-|-CURRENT ACCOUNTS -- (FILE2.DBF)
|-|-OVERDRAFT ACCOUNTS -- (FILE2.DBF)
|-LOAN                 -- (FILE3.DBF)
|-|-AGRICULTURAL       -- (FILE3.DBF)
|-|-MANUFACTURING      -- (FILE3.DBF)
|-|-ETC...             -- (FILE3.DBF)

Methodology 
read the content of each dbf files, i.e in case of awash 3 files saving, current and loan dbase file and trasform data to the required datatype
then append each records to a multiple files like party, party_detail, party_enterprise and such based on their table relationship

do the same as for the account table

FINAL OUTCOME

migrate data from old dbase database to modern relational database,
i migrated 78 branches with lesser time and lesset effort than it used to migrate 1 branch, including creating a cutom account conversion software in JAVA , that is old account to new account convertor

Thanks For Reading

