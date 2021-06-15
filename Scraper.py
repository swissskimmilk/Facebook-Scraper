from facebook_scraper import get_posts
from facebook_scraper import get_profile
import mysql.connector

# Declare & initialize constants
databaseName = "facebook_scraper_data"

database = mysql.connector.connect(
    host="localhost",
    user="root",
    password="PASSWORD GOES HERE"
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

# Create posts table
cursor = database.cursor()
cursor.execute("CREATE TABLE posts ("
               "post_id int NOT NULL AUTO_INCREMENT,"
               "fb_post_id BIGINT,"
               "url text,"
               "text text,"
               "time DATETIME,"
               "comments int,"
               "likes int,"
               "shares int,"
               "PRIMARY KEY (post_id))")

# Create reactions table

# Create comments table

# Create profiles table

# for post in get_posts('repannaeshoo', pages=1, options={"comments": True, "reactors": True}):
#    print(post)
