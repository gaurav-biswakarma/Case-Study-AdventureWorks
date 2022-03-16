**Sqoop import command**

sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?useSSL=False --username root --password-file file:///home/saif/LFS/cohort_c9/envvar/sqoop.pwd --delete-target-dir --target-dir /user/saif/HFS/Input/adventureworks --query "select CustomerID,TerritoryID,AccountNumber,CustomerType,ModifiedDate,Demographics from customer_individual \ where \$CONDITIONS" --split-by CustomerID

**Hive manage table**

create table cust_indi(CustomerID int,TerritoryID int,AccountNumber string,CustomerType string,ModifiedDate timestamp,Demographics string) row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1") ;

**Load data in manage table**

load data inpath "/user/saif/HFS/Input/adventureworks/" into table cust_indi;

**Create new table and load data from manage table to this new table**

create table ext_cust as select customerid,territoryid,accountnumber,customertype,modifieddate,xpath(demographics,'IndividualSurvey/TotalPurchaseYTD/text()'),xpath(demographics,'IndividualSurvey/DateFirstPurchase/text()'),xpath(demographics,'IndividualSurvey/BirthDate/text()'),xpath(demographics,'IndividualSurvey/MaritalStatus/text()'),xpath(demographics,'IndividualSurvey/YearlyIncome/text()'),xpath(demographics,'IndividualSurvey/Gender/text()'),xpath(demographics,'IndividualSurvey/TotalChildren/text()'),xpath(demographics,'IndividualSurvey/NumberChildrenAtHome/text()'),xpath(demographics,'IndividualSurvey/Education/text()'),xpath(demographics,'IndividualSurvey/Occupation/text()'),xpath(demographics,'IndividualSurvey/HomeOwnerFlag/text()'),xpath(demographics,'IndividualSurvey/NumberCarsOwned/text()'),xpath(demographics,'IndividualSurvey/CommuteDistance/text()') from cust_indi;

**Sqoop import for credit card table**

sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?useSSL=False --username root --password-file file:///home/saif/LFS/cohort_c9/envvar/sqoop.pwd --delete-target-dir --target-dir /user/saif/HFS/Input/adventureworks1 --query "select CreditCardID,CardType,CardNumber,ExpMonth,ExpYear,ModifiedDate from creditcard \ where \$CONDITIONS" -m 1

Create manage table for credit card create table credit_hive(CreditCardID int,CardType string,CardNumber bigint,ExpMonth int,ExpYear int,ModifiedDate timestamp ) row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1");

**Load data in manage table for credit card table**

load data inpath "/user/saif/HFS/Input/adventureworks1/" into table credit_hive;
