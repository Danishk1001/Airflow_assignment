import psycopg2


def create_insert_daily_weather_info():
    try:
        # setting up connection with postgresql
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port='5433')

        # conn.autocommit = True
        # creating a cursor
        cursor = conn.cursor()

        # creating new sql table
        sql = """CREATE TABLE weather(
                STATE VARCHAR(30),
                DESCRIPTION varchar(30),
                TEMPERATURE decimal,
                FEELS_LIKE_TEMPERATURE decimal,
                MIN_TEMP decimal,
                MAX_TEMP decimal,
                HUMIDITY numeric,
                CLOUDS numeric);"""

        cursor.execute(sql)

        # sql2 = '''COPY weather
        # FROM '/Users/danish01/Downloads/Problem_1_excel.csv'
        # DELIMITER ','
        # CSV HEADER;'''
        #
        # cursor.execute(sql2)

        # open the csv file using python standard file I/O
        # copy file into the table just created
        with open('Problem_1_excel.csv', 'r') as f:
            next(f)  # Skip the header row.
            # f , <database name>, Comma-Separated
            cursor.copy_from(f, 'weather', sep=',')

        # run query to show all the data present in the database
        sql3 = '''select * from weather;'''
        cursor.execute(sql3)
        for i in cursor.fetchall():
            print(i)

    except:
        print("Couldn't connect to the network.")

    finally:
        conn.commit()
        conn.close()

# import pandas
# import psycopg2
#
#
# def create_insert_daily_weather_info():
#     df = pandas.read_csv("Problem_1_excel.csv")
#     print(df)
#
#     create_table = """CREATE TABLE weather(
#         STATE VARCHAR(30),
#         DESCRIPTION varchar(30),
#         TEMPERATURE decimal,
#         FEELS_LIKE_TEMPERATURE decimal,
#         MIN_TEMP decimal,
#         MAX_TEMP decimal,
#         HUMIDITY numeric,
#         CLOUDS numeric)"""
#
#     try:
#         conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port='5433')
#         cursor = conn.cursor()
#
#         cursor.execute(create_table)
#
#         print("Table Created successfully")
#
#         insert_query = "Insert into weather (STATE, DESCRIPTION, TEMPERATURE, FEELS_LIKE_TEMPERATURE,MIN_TEMP, " \
#                        "MAX_TEMP,HUMIDITY,CLOUDS) values (%s,%s,%s,%s,%s,%s,%s,%s)"
#
#         for index, row in df.iterrows():
#             print("added..")
#             cursor.execute(insert_query, (
#                 row['State'], row['Description'], row['Temperature'], row['Feels_Like_Temperature']
#                 , row['Min_Temperature'], row['Max_Temperature'], row['Humidity'], row['Clouds']))
#
#         conn.commit()
#
#         print("values are Inserted successfully")
#
#     except:
#         print("Error in connection")
#     finally:
#         conn.close()
#         print("No issues")