from facebook_scraper import get_posts
from facebook_scraper import get_profile
from datetime import datetime
import mysql.connector
import requests

# Declare & initialize constants
databaseName = "facebook_scraper_data"
citiesInDistrict = ['palo alto', 'stanford', 'los altos', 'woodside', 'mountain view', 'los altos hills', 'campbell',
                    'saratoga', 'los gatos', 'scotts valley', 'menlo park', 'redwood city', 'san jose', 'santa cruz']

database = mysql.connector.connect(
    host="localhost",
    user="root",
    password="T-R294$fxy"
)

# Create database and tables
cursor = database.cursor()
cursor.execute("SHOW DATABASES")

# Attempt to create database, pass if already exists
try:
    cursor.execute(f"CREATE DATABASE {databaseName}")
except mysql.connector.errors.DatabaseError:
    pass

# Now that the database definitely exists, get it
database = mysql.connector.connect(
    host="localhost",
    user="root",
    password="T-R294$fxy",
    database=databaseName
)
cursor = database.cursor()

# Create posts table, pass if already exists
try:
    cursor.execute("CREATE TABLE posts ("
                   "post_id BIGINT NOT NULL,"
                   "fetched_time DATETIME,"
                   "post_url text,"
                   "posting_time DATETIME,"
                   "post_text text,"
                   "attached_url text,"
                   "comments int,"
                   "likes int,"
                   "shares int,"
                   "PRIMARY KEY (post_id))")
except mysql.connector.errors.ProgrammingError:
    pass

# Create comments table, pass if already exists
try:
    cursor.execute("CREATE TABLE comments ("
                   "comment_id BIGINT NOT NULL,"
                   "post_id BIGINT NOT NULL,"
                   "fetched_time DATETIME,"
                   "comment_url text,"
                   "commenter text,"
                   "commenter_id BIGINT,"
                   "commenting_time DATETIME,"
                   "comment_text text,"
                   "replies int,"
                   "PRIMARY KEY (comment_id))")
except mysql.connector.errors.ProgrammingError:
    pass

# Create profiles table, pass if already exists
try:
    cursor.execute("CREATE TABLE profiles ("
                   "profile_id BIGINT NOT NULL,"
                   "fetched_time DATETIME,"
                   "profile_url text,"
                   "name text,"
                   "about text,"
                   "education text,"
                   "favorite_quotes text,"
                   "location text,"
                   "in_district BOOLEAN,"
                   "work text,"
                   "other_info text,"
                   "PRIMARY KEY (profile_id))")
except mysql.connector.errors.ProgrammingError:
    pass

# Get posts
posts = get_posts('repannaeshoo', pages=1, options={"progress": True, "comments": True},
                  credentials=("josephmayes97@gmail.com", "nHi5&UcFzk6i"))
for post in posts:
    print(post)
    # Add data to posts table
    cursor.execute(
        "REPLACE INTO posts (post_id, fetched_time, post_url, posting_time, post_text, attached_url, comments, "
        "likes, shares) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (post['post_id'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), post['post_url'], post['time'],
         post['post_text'], post['link'], post['comments'], post['likes'], post['shares'])
    )
    database.commit()

    # Make sure there are comments on the post
    if len(post['comments_full']) == 0:
        print("No comments on post")
    else:
        for comment in post['comments_full']:
            print(comment)

            # Get the number of replies
            if 'replies' in comment:
                repliesCount = len(comment['replies'])
            else:
                repliesCount = 0

            # Insert data in comments table
            cursor.execute(
                "REPLACE INTO comments (comment_id, post_id, fetched_time, comment_url, commenter, commenter_id, "
                "commenting_time, comment_text, replies) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (comment['comment_id'], post['post_id'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                 comment['comment_url'], comment['commenter_name'], comment['commenter_id'], comment['comment_time'],
                 comment['comment_text'], repliesCount)
            )
            database.commit()

            try:
                # Get profile of commenter
                profile = get_profile(comment['commenter_id'])
                print(profile)
            except Exception as e:
                print(e)

            # Get current city and compare against cities in the district
            placesLived = profile.get('Places Lived')
            if placesLived is not None:
                currentLocation = placesLived[:profile.get('Places Lived').index(",")]
                currentLocation = currentLocation.lower()
                if currentLocation in citiesInDistrict:
                    inDistrict = True
                else:
                    inDistrict = False
            else:
                currentLocation = None
                inDistrict = False

            # Process URL
            if profile.get('Contact Info') is not None and profile.get('Contact Info').get('Facebook') is not None:
                profileURL = profile.get('Contact Info').get('Facebook')
            else:
                profileURL = None

            # Check for error getting profile
            if profile.get('Name') == "You Can't Use This Feature Right Now":
                name = "Error"
                cursor.execute(f"SELECT * FROM profiles WHERE profile_id = {comment['commenter_id']}")
                entries = cursor.fetchall()
                print("Fetchall: " + str(entries) + " Length: " + str(len(entries)))
                if len(entries) == 0:
                    # Insert profile data in profiles table
                    cursor.execute(
                        "INSERT INTO profiles (profile_id, fetched_time, profile_url, name, about, education, favorite_quotes, "
                        "location, in_district, work, other_info) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (comment['commenter_id'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), profileURL, name, profile.get('About'),
                         profile.get('Education'), profile.get('Favorite Quotes'),
                         currentLocation, inDistrict, profile.get('Work'), profile.get('Basic Info'))
                    )
                    database.commit()
            else:
                name = profile.get('Name')
                # Insert profile data in profiles table
                cursor.execute(
                    "REPLACE INTO profiles (profile_id, fetched_time, profile_url, name, about, education, favorite_quotes, "
                    "location, in_district, work, other_info) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (comment['commenter_id'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), profileURL, name, profile.get('About'),
                     profile.get('Education'), profile.get('Favorite Quotes'),
                     currentLocation, inDistrict, profile.get('Work'), profile.get('Basic Info'))
                )
                database.commit()
