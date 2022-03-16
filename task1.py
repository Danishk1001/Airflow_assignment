import requests
import pandas as pd


def write_into_csv():
    try:
        # extracting data from the apis
        url = "https://community-open-weather-map.p.rapidapi.com/weather"
        headers = {
            'x-rapidapi-host': "community-open-weather-map.p.rapidapi.com",
            'x-rapidapi-key': "8f57a3f704mshd2aff0177866e0cp125236jsn2d6b941c9952"
        }

        cities = ["Rajasthan", "Gujrat", "madhya pradesh", "uttar pradesh", "chandigarh", "punjab", "maharashtra",
                  "manipur", "west bengal", "delhi"]

        data_to_write = []

        for city in cities:
            querystring = {"q": city + ",india", "lat": "0", "lon": "0", "id": "2172797", "lang": "null",
                           "units": "metric", "mode": "JSON"}
            response = requests.request("GET", url, headers=headers, params=querystring)
            data = response.json()
            print(data)
            list_item = [data["name"], data["weather"][0]["description"], data["main"]["temp"],
                         data["main"]["feels_like"],
                         data["main"]["temp_min"], data["main"]["temp_max"], data["main"]["humidity"],
                         data["clouds"]["all"]]

            data_to_write.append(list_item)
    except:
        print("Couldn't extract data from api.")

    # converting data into table
    state = []
    description = []
    temp = []
    feels_like = []
    temp_min = []
    temp_max = []
    humidity = []
    clouds = []

    for row in data_to_write:
        state.append(row[0])
        description.append(row[1])
        temp.append(row[2])
        feels_like.append(row[3])
        temp_min.append(row[4])
        temp_max.append(row[5])
        humidity.append(row[6])
        clouds.append(row[7])

    # header = ['State', 'Description', 'Temperature', 'Feels_Like_Temperature', 'Min_Temperature', 'Max_Temperature',
    #           'Humidity', 'Clouds']

    #  State, Description, Temperature, Feels Like Temperature, Min Temperature, Max Temperature, Humidity, Clouds.
    # using pandas dataframe converting data into dataframe
    df = pd.DataFrame(
        {'State': state, 'Description': description, 'Temperature': temp, 'Feels Like Temperature': feels_like,
         'Min Temerature': temp_min, 'Max Temperature': temp_max, 'Humidity': humidity, 'Clouds': clouds})

    print(df)

    # writing the dataframe into a csv file
    result = df.to_csv('Problem_1_excel.csv', index=False, mode='a')
