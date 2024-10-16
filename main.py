import pandas as pd
import mysql.connector
from mysql.connector import Error

def import_csv_to_mysql(csv_file, table_name, connection_params):
    connection = None
    cursor = None

    try:
        connection = mysql.connector.connect(**connection_params)

        if connection.is_connected():
            print("Connected to MySQL database")

            data = pd.read_csv(csv_file)

            data = data.rename(columns={
                'AppID': 'app_id',
                'Name': 'name',
                'Release date': 'release_date',
                'Estimated owners': 'estimated_owners',
                'Peak CCU': 'peak_ccu',
                'Required age': 'required_age',
                'Price': 'price',
                'DiscountDLC count': 'dlc_count',
                'About the game': 'detailed_description',
                'Supported languages': 'supported_languages',
                'Full audio languages': 'full_audio_languages',
                'Header image': 'header_image',
                'Website': 'website',
                'Support url': 'support_url',
                'Support email': 'support_email',
                'Windows': 'support_windows',
                'Mac': 'support_mac',
                'Linux': 'support_linux',
                'Metacritic score': 'metacritic_score',
                'Metacritic url': 'metacritic_url',
                'User score': 'user_score',
                'Positive': 'positive',
                'Negative': 'negative',
                'Score rank': 'score_rank',
                'Achievements': 'achievements',
                'Recommendations': 'recommendations',
                'Notes': 'notes',
                'Average playtime forever': 'average_playtime_forever',
                'Average playtime two weeks': 'average_playtime_2weeks',
                'Median playtime forever': 'median_playtime_forever',
                'Median playtime two weeks': 'median_playtime_2weeks'
            })

            print("DataFrame columns:", data.columns)

            cursor = connection.cursor()

            for _, row in data.iterrows():
                insert_game_query = """
               INSERT INTO games (
                    app_id, name, release_date, estimated_owners, peak_ccu, 
                    required_age, price, dlc_count, detailed_description, 
                    supported_languages, full_audio_languages, reviews, header_image, 
                    website, support_url, support_email, support_windows, support_mac, 
                    support_linux, metacritic_score, metacritic_url, user_score, 
                    positive, negative, score_rank, achievements, recommendations, 
                    notes, average_playtime_forever, average_playtime_2weeks, 
                    median_playtime_forever, median_playtime_2weeks, developers, 
                    publishers, categories, genres, tags, screenshots, movies
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);

                """

                values = (
                    row['app_id'],
                    row['name'],
                    row['release_date'],
                    row['estimated_owners'],
                    row['peak_ccu'],
                    row['required_age'],
                    row['price'],
                    row['dlc_count'],
                    row['detailed_description'],
                    row['supported_languages'],
                    row['full_audio_languages'],
                    row.get('Reviews', ''),
                    row['header_image'],
                    row['website'],
                    row['support_url'] if not pd.isna(row['support_url']) else None,
                    row['support_email'] if not pd.isna(row['support_email']) else None,
                    row['support_windows'],
                    row['support_mac'],
                    row['support_linux'],
                    row['metacritic_score'] if not pd.isna(row['metacritic_score']) else None,
                    row['metacritic_url'] if not pd.isna(row['metacritic_url']) else None,
                    row['user_score'] if not pd.isna(row['user_score']) else None,
                    row['positive'],
                    row['negative'],
                    row['score_rank'],
                    row['achievements'],
                    row['recommendations'],
                    row.get('notes', None),
                    row['average_playtime_forever'],
                    row['average_playtime_2weeks'],
                    row['median_playtime_forever'],
                    row['median_playtime_2weeks']
                )

                for idx, val in enumerate(values):
                    print(f"Value at index {idx}: {val} (type: {type(val)})")

                columns = [
                    'app_id', 'name', 'release_date', 'estimated_owners', 'peak_ccu',
                    'required_age', 'price', 'dlc_count', 'detailed_description',
                    'supported_languages', 'full_audio_languages', 'reviews', 'header_image',
                    'website', 'support_url', 'support_email', 'support_windows', 'support_mac',
                    'support_linux', 'metacritic_score', 'metacritic_url', 'user_score',
                    'positive', 'negative', 'score_rank', 'achievements', 'recommendations',
                    'notes', 'average_playtime_forever', 'average_playtime_2weeks',
                    'median_playtime_forever', 'median_playtime_2weeks'
                ]

                for idx, col in enumerate(columns):
                    print(f"Column at index {idx}: {col}")

                cursor.execute(insert_game_query, values)

            connection.commit()
            print("Data imported successfully")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

connection_params = {
    'host': 'localhost',
    'database': 'gamesdb',
    'user': 'root',
    'password': 'Wh3r3wvulf@2004!'
}

csv_file = r"C:\Users\klunk\Downloads\games.csv"
table_name = 'games'

import_csv_to_mysql(csv_file, table_name, connection_params)
