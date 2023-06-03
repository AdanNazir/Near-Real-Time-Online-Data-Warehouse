Steps to Run Project:

Step 1: Using CreateDW.sql file:

-> Create the Star Schema using "createDW.sql" file

Step 2: Load MeshJoin.java file:

-> Load the "meshJoin.java" file into a java project. Before loading it, create a "new java project" in the file section on the top left corner and then create a new class, then
copy paste the code from "meshJoin.java" file into the new class or directly load into it.

Step 3: Importing the 2 provided Jar files in the zip folder

_> Add the provided files (javatuples-1.2.jar,mysql-connector-java-8.0.27.java) in Eclipse by changing the path.

-> Follow *Project->Properties->JavaBuildPath->Add External Jars

Step 4: Changing the username, password and schema name in the connections section

->Change the  schema,username and password in both connections (database,datawarehouse) .
	e.g. ("jdbc:mysql://localhost:3306/schema","UserName","password")

Step 5: Run File in eclipse

Step 6: Running provided Queries

-> Copy paste the queries from the provided "Queries.sql" file in the zip folder and copy paste them into the mysql work bench one by one.

Thanks-------