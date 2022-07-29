# Food_Export_Analytics_Engineering

This Project is Based on DBT(data build Tool) Here i am using a Food Export company data called northwind originaly built by microsoft as an teaching example databse. 

I am using DBT(data build tool) to transfom the data by pulling all the data into staging area data lake of big query.

The following solution is great for the company that wants to get a dataware house based on their on premesis database or transaction database data to be analysed and stored in data ware house 

Below is the solution architure that i have followed to build the project.

![image](https://user-images.githubusercontent.com/10596580/181746588-205136d1-d965-4fc2-bed3-637341d7612a.png)

 here i used bigquery for dataware house 

The data modelling is carried out through standard bus matrics spreadsheet where , the key buiness areas will be discussed with the stakeholders and build conceptual , logical and physycal database modelling with a incremental agile stages 

1. conceptual model of the data warehouse  involve identifying real world entities of the business and their relationships in a high level model
2. logical model of the data warehouse defines the key attributs of the entities and cardinatily between the entities 
3. physical model of the data warehouse decribe the whole datawarehouse architure and impementation and deployement.

the below bus matrics gives and detailed view on how the above concepts are carried out in build the data warehouse 

![image](https://user-images.githubusercontent.com/10596580/181745090-72481523-7abc-4a6a-85a4-4a900ca6bc13.png)

the main feature of dbt is jinja sql where you can write sql in programetic way , this helps transformation of raw data in easier way.

