import pandas as pd
import mysql.connector

# Replace the following variables with your MySQL database connection details
mysql_host = "127.0.0.1"
mysql_port = 3306
mysql_user = "root"
mysql_password = "root"
mysql_database = "movies"


# Read data from data.tsv using pandas
data_file = 'data.tsv'
df = pd.read_csv(data_file, sep='\t')
df = df.loc[df['startYear'] > "2000"]
df = df.loc[df['startYear'] != "\\N"]
df = df.loc[df['titleType']== "movie"]
df = df.head(1000)

#Connect to the MySQL database
try:
    connection = mysql.connector.connect(
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    cursor = connection.cursor()

    # Insert data into the 'movies' table
    for index, row in df.iterrows():
        # Replace the column names in the 'INSERT INTO' query with your actual column names
        sql = "INSERT INTO movies_movie (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        values = (
            row['tconst'],
            row['titleType'],
            row['primaryTitle'],
            row['originalTitle'],
            row['isAdult'],
            row['startYear'],
            row['endYear'],
            row['runtimeMinutes'],
            row['genres']
        )
        cursor.execute(sql, values)

    connection.commit()
    print("Data inserted successfully!")

except mysql.connector.Error as error:
    print("Error while connecting to MySQL or inserting data:", error)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed.")
