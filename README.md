# Case-Study-AdventureWorks

Buisness Problem: A company AdventureWorks wants to work on the database for its visiting customers and it is trying to analyse the year to date total revenue based on the database of retail sales data called adventure works. For this particular idea, company wants to use only customer, individual and credit card details. These datasets comprise of bulk and raw data that needs to be transformed and cleaned in order to bring out the data for analytics purpose and getting the insights out of it.

**Disclaimer for AdventureWorks Database**

The information contained in this website (AdventureWorks) is for general information purposes only. The information is provided by AdventureWorks and while we endeavour to keep the information up to date and correct, we make no representations or warranties of any kind, express or implied, about the completeness, accuracy, reliability, suitability or availability with respect to the website or the information, products, services, or related graphics contained on the website for any purpose. Any reliance you place on such information is therefore strictly at your own risk.

**Approach:**

1.	Data is present in MySQL database.

2.	Load the data from MySQL to HDFS using SQOOP.

3.	Create and load data to HIVE table.

4.	Read data from HIVE in Spark and perform data cleaning.

5.	Load the data again to hive and perform analytics.


**HDFS EcoSystem**

•	Sqoop
•	Hive
•	Spark
•	PySpark


**Individual Table Definition**

The Individual table is contained in the Sales schema.

![image](https://user-images.githubusercontent.com/100192347/158580184-b3c6e9d2-a67f-44a4-ae9b-ad9be403135a.png)

**Customer Table Definition**

The Customer table is contained in the Sales schema.

![image](https://user-images.githubusercontent.com/100192347/158580637-6b6af6c6-ad4a-4a02-851d-a073a1e3c188.png)

**CreditCard Table Definition**

The CreditCard table is contained in the Sales schema.

![image](https://user-images.githubusercontent.com/100192347/158580771-36221c03-6617-4e50-b5e4-122bb11d9ef8.png)


**Loading data from the database**

The first task is to load the AdventureWorks Database from SQL Server to the Hadoop platform. This database contains data of finance, sales, products and customers of the bicycle store. This shifting of data is done by using Apache Sqoop. It is an open source tool used to transfer bulk of data from structured data stores like relational databases such as Teradata, Netezza, Oracle, MySQL, Postgres, and HSQLDB. After transferring the data, queries are fired for insertion of tables in Hive. Hive is data warehouse which is a component of Hadoop that processes the data. It is used for summarizing the big data making it easy for querying and analyzing. It is provides a language similar to SQL called HiveQL or HQL for processing the data on it.

**Project Architecture**

![image](https://user-images.githubusercontent.com/100192347/159216383-aceeb585-2e5a-4e94-b44d-24dccf5cbac3.png)

