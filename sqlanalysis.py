#JOins

create view customer_individual as select a.CustomerID,a.TerritoryID,a.AccountNumber,a.CustomerType,a.ModifiedDate,b.Demographics from customer a , 
individual b where a.CustomerID = b.CustomerID;

#Sqoop import command

sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?useSSL=False --username root 
            --password-file file:///home/saif/LFS/cohort_c9/envvar/sqoop.pwd --delete-target-dir --target-dir /user/saif/HFS/Input/adventureworks 
                --query "select CustomerID,TerritoryID,AccountNumber,CustomerType,ModifiedDate,Demographics from customer_individual \
where \$CONDITIONS" --split-by CustomerID

sqoop import --connect jdbc:mysql://localhost:3306/adventureworks?useSSL=False --username root 
            --password-file file:///home/saif/LFS/cohort_c9/envvar/sqoop.pwd --delete-target-dir --target-dir /user/saif/HFS/Input/adventureworks 
                --query "select CreditCardID,CardType,CardNumber,ExpMonth,ExpYear,ModifiedDate from creditcard \
where \$CONDITIONS" -m 1



#Hive manage table 

create table cust_indi(CustomerID int,TerritoryID int,AccountNumber string,CustomerType string,ModifiedDate timestamp,Demographics string) 
row format delimited fields terminated by ',' tblproperties("skip.header.line.count"="1") ;

#HIve table for creditcard

create table credit_hive(CreditCardID  int,CardType string,CardNumber bigint,ExpMonth int,ExpYear int,ModifiedDate timestamp ) 
row format delimited fields terminated by ','  tblproperties("skip.header.line.count"="1");


load data inpath "/user/saif/HFS/Input/adventureworks/" into table credit_hive;

#Load data in manage table

load data inpath "/user/saif/HFS/Input/adventureworks/" into table cust_indi;


#Load data in new table from manage table

create table ext_cust as select customerid,territoryid,accountnumber,customertype,modifieddate,xpath(demographics,'IndividualSurvey/TotalPurchaseYTD/text()'),
xpath(demographics,'IndividualSurvey/DateFirstPurchase/text()'),xpath(demographics,'IndividualSurvey/BirthDate/text()'),
xpath(demographics,'IndividualSurvey/MaritalStatus/text()'),xpath(demographics,'IndividualSurvey/YearlyIncome/text()'),
xpath(demographics,'IndividualSurvey/Gender/text()'),xpath(demographics,'IndividualSurvey/TotalChildren/text()'),
xpath(demographics,'IndividualSurvey/NumberChildrenAtHome/text()'),xpath(demographics,'IndividualSurvey/Education/text()'),
xpath(demographics,'IndividualSurvey/Occupation/text()'),xpath(demographics,'IndividualSurvey/HomeOwnerFlag/text()'),
xpath(demographics,'IndividualSurvey/NumberCarsOwned/text()'),xpath(demographics,'IndividualSurvey/CommuteDistance/text()') from cust_indi;


#Pyspark


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring
if __name__ == '__main__':
    spark = SparkSession.builder.appName("AppName").master("local[*]").config("hive.metastore.uris", "thrift://localhost:9083/")\
            .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse/") \
            .enableHiveSupport().getOrCreate()
    spark.sql("use adventureworks")
    df1= spark.sql("Select * from ext_cust")
    df2= df1.dropna()
    # df2.show()
    df3 = df2.select(df2.customerid,df2.territoryid,df2.accountnumber,df2.customertype,
        df2.modifieddate,df2._c5.getItem(0).alias("TotalPurchaseYTD"),df2._c6.getItem(0).alias("DateFirstPurchase"),
        df2._c7.getItem(0).alias("BirthDate"),df2._c8.getItem(0).alias("MaritalStatus"),df2._c9.getItem(0).alias("YearlyIncome"),
        df2._c10.getItem(0).alias("Gender"),df2._c11.getItem(0).alias("TotalChildren"),df2._c12.getItem(0).alias("NumberChildrenAtHome"),
        df2._c13.getItem(0).alias("Education"),df2._c14.getItem(0).alias("Occupation"),df2._c15.getItem(0).alias("HomeOwnerFlag"),
        df2._c16.getItem(0).alias("NumberCarsOwned"),df2._c17.getItem(0).alias("CommuteDistance"))
    # df3.show()
    # df3.printSchema()

    df4 = df3.select(df3.customerid,df3.territoryid,df3.accountnumber,df3.customertype,
        df3.modifieddate,df3.TotalPurchaseYTD.cast("float"), substring("DateFirstPurchase",1,10).alias('DateFirstPurchase').cast("date"),
                     substring("BirthDate",1,10).alias('BirthDate').cast("date"),df3.YearlyIncome,df3.Gender,df3.TotalChildren.cast("int"),
                     df3.NumberChildrenAtHome.cast("int"),df3.Education,df3.Occupation,df3.HomeOwnerFlag.cast("int"),
                     df3.NumberCarsOwned.cast("int"),df3.CommuteDistance)

    df4.show()
    # df4.printSchema()
    df4.write.mode("overwrite").format("parquet").saveAsTable("adventureworks.sqlproject")

