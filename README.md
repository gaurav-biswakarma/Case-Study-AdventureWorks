# Case-Study-AdventureWorks

AdventureWorks sells bicycles and bicycle parts directly to customers and distributors. The company currently has a single office in the Netherlands, and have been selling bicycles in the United States, Germany and Spain through a chain of distributors and through online sales on its website. The fulfilment of delivery is done by local distribution centers.

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


