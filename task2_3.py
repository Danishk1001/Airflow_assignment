import pandas
import psycopg2


def create_insert_daily_weather_info():
    # reading csv using pandas
    df = pandas.read_csv("Problem_1_excel.csv")
    print(df)

    # creating a table
    create_table = """CREATE TABLE weather(
        STATE VARCHAR(30),
        DESCRIPTION varchar(30),
        TEMPERATURE decimal,
        FEELS_LIKE_TEMPERATURE decimal,
        MIN_TEMP decimal,
        MAX_TEMP decimal,
        HUMIDITY numeric,
        CLOUDS numeric)"""

    try:
        # connecting to the database
        conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow", port='5433')
        cursor = conn.cursor()

        cursor.execute(create_table)

        print("Table Created successfully")

        insert_query = "Insert into weather (STATE, DESCRIPTION, TEMPERATURE, FEELS_LIKE_TEMPERATURE,MIN_TEMP, " \
                       "MAX_TEMP,HUMIDITY,CLOUDS) values (%s,%s,%s,%s,%s,%s,%s,%s)"

        # iterating each new row from csv and updating in database
        for index, row in df.iterrows():
            print("added..")
            cursor.execute(insert_query, (
                row['State'], row['Description'], row['Temperature'], row['Feels Like Temperature']
                , row['Min Temperature'], row['Max Temperature'], row['Humidity'], row['Clouds']))

        conn.commit()

        print("values are Inserted successfully")


    except:
        print("Error in connection")
    finally:
        conn.close()
        print("No issues")