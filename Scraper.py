# Known issues:
# When process_profile is called, it should update the existing profile with new information. Currently, it only adds profileID and username
# Remove commas
# Remove links
# Notes:
# If you start getting HTTP errors, make sure you are using a VPN and change your VPN server. Proton VPN works and is free
# If names start printing as "You Can't Use This Feature Right Now" try switching profiles and VPN server
# Interacting with Eshoo's posts using the accounts we scrape with will produce errors

from facebook_scraper import get_posts
from facebook_scraper import get_profile
import facebook_scraper
from datetime import datetime

import mysql.connector

import time

from random import seed
from random import random

processProfiles = False

# Declare & initialize constants
databaseName = "facebook_scraper_data"
host = "localhost"
user = "root"
password="T-R294$fxy"
citiesInDistrict = ['palo alto', 'stanford', 'los altos', 'woodside', 'mountain view', 'los altos hills', 'campbell',
                    'saratoga', 'los gatos', 'scotts valley', 'menlo park', 'redwood city', 'san jose', 'santa cruz',
                    'cupertino', 'santa clara', 'felton', 'la honda']
# For generating random numbers
seed(1)

# Connect to database
database = mysql.connector.connect(
    host=host,
    user=user,
    password=password
)

# Get cursor which is used to interact with the database
cursor = database.cursor()

# Attempt to create database, pass if already exists
try:
    cursor.execute(f"CREATE DATABASE {databaseName}")
except mysql.connector.errors.DatabaseError:
    pass

# Now that the database definitely exists, get it
database = mysql.connector.connect(
    host=host,
    user=user,
    password=password,
    database=databaseName
)
# Cursor needs to be acquired again
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

# Create replies table, pass if already exists
try:
    cursor.execute("CREATE TABLE replies ("
                   "comment_id BIGINT NOT NULL,"
                   "parent_comment_id BIGINT NOT NULL,"
                   "fetched_time DATETIME,"
                   "comment_url text,"
                   "commenter text,"
                   "commenter_id BIGINT,"
                   "commenting_time DATETIME,"
                   "comment_text text,"
                   "PRIMARY KEY (comment_id))")
except mysql.connector.errors.ProgrammingError:
    pass

# Create reactions table, pass if already exists
try:
    cursor.execute("CREATE TABLE reactions ("
                   "post_id BIGINT NOT NULL,"
                   "fetched_time DATETIME,"
                   "reactor text,"
                   "profile_id BIGINT,"
                   "username text,"
                   "type text)")
except mysql.connector.errors.ProgrammingError:
    pass

# Create profiles table, pass if already exists
try:
    cursor.execute("CREATE TABLE profiles ("
                   "profile_id BIGINT,"
                   "fetched_time DATETIME,"
                   "username VARCHAR(50),"
                   "name text,"
                   "about text,"
                   "education text,"
                   "favorite_quotes text,"
                   "location text,"
                   "in_district BOOLEAN,"
                   "work text,"
                   "other_info text)")
except mysql.connector.errors.ProgrammingError:
    pass

def username_from_profile(profile):
    # Prevents error by checking if the key 'Contact Info' exists first
    if profile.get('Contact Info') is not None and profile.get('Contact Info').get('Facebook') is not None:
        username = profile.get('Contact Info').get('Facebook')
        # Removes the '/' at the beginning of the string
        username = username[1:]
    else:
        username = None
    return username

# Used to process variables for SQL queries where the value might be null. Also formats strings correctly.
# Use Parameterized SQL Queries instead whenever possible
def process_value(value):
    if value is None:
        return "is NULL"
    else:
        value = str(value)
        value = value.replace(',', "\\,")
        value = value.replace("'", "\\'")
        value = value.replace('"', '\\"')
        return "= \'" + value + "\'"

# Processes profiles and updates the profiles table
def process_profile(profileID, username):

    if not processProfiles:
        return

    # Check if entries already exists, to prevent lockout
    if profileID is not None:
        cursor.execute("SELECT * FROM profiles WHERE profile_id = %s", (profileID,))
        entriesByID = cursor.fetchall()
        print(entriesByID)
        if len(entriesByID) > 1:
            raise Exception("Database error: multiple profiles with same id")
        if len(entriesByID) > 0 and entriesByID[0][3] != "Error":
            print("Skipping " + profileID)
            return
    else:
        entriesByID = None
    if username is not None:
        cursor.execute("SELECT * FROM profiles WHERE username = %s", (username,))
        entriesByUsername = cursor.fetchall()
        print(entriesByUsername)
        if len(entriesByUsername) > 1:
            raise Exception("Database error: multiple profiles with same username")
        if len(entriesByUsername) > 0 and entriesByUsername[0][3] != "Error":
            print("Skipping " + username)
            return

    # Error checking
    if profileID is None and username is None:
        raise Exception("profileID and username can't both be none")

    # Debugging, remove later
    print("Profile ID: " + str(profileID) + ". Username: " + str(username))

    # Tries both profileID and username to get profile
    if username is not None:
        profile = get_profile(username)
        print(profile)
    else:
        profile = get_profile(profileID)
        print(profile)

    # Get current city and compare against cities in the district
    placesLived = profile.get('Places Lived')
    if placesLived is not None:
        placesLived = placesLived.lower()
        # Catches exceptions that occur when someone doesn't have their location listed how it normally is
        try:
            if "," in placesLived:
                currentLocation = placesLived[:placesLived.index(",")]
                if "current city" in currentLocation:
                    currentLocation = currentLocation[:currentLocation.index("current city")]
            elif "current city" in placesLived:
                currentLocation = placesLived[:placesLived.index("current city")]
            else:
                currentLocation = profile.get('Places Lived')
        except:
            currentLocation = placesLived

        # Checks location again array of cities in the district, defined at the top of this file
        if currentLocation in citiesInDistrict:
            inDistrict = True
        else:
            inDistrict = False
    else:
        currentLocation = None
        inDistrict = False

    # Gets username using profileID
    if username is None:
        username = username_from_profile(profile)

    # Checks if entry already exists
    cursor.execute("SELECT * FROM profiles WHERE username = %s", (username,))
    entriesByUsername = cursor.fetchall()
    print("Entries by username " + str(entriesByUsername))

    # Check for error getting profile
    if profile.get('Name') == "You Can't Use This Feature Right Now":
        name = "Error"
        if not entriesByID and not entriesByUsername:
            # Insert entry so that we have a profileID in the table to use later
            cursor.execute(
                "INSERT INTO profiles (profile_id, fetched_time, username) VALUES (%s, %s, %s)",
                (profileID, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), username)
            )
            database.commit()
    else:
        # Get duplicate rows in case keys don't match
        cursor.execute(
            "SELECT * FROM profiles where (name %s) and (about %s) and (education %s) and (favorite_quotes %s) and (location %s) and (work %s) and (other_info %s)"
            % (process_value(profile.get('Name')), process_value(profile.get('About')),
               process_value(profile.get('Education')),
               process_value(profile.get('Favorite Quotes')), process_value(currentLocation),
               process_value(profile.get('Work')), process_value(profile.get('Basic Info')))
        )
        duplicates = cursor.fetchall()
        print("Duplicates" + str(duplicates))

        # Determine if any duplicates exist but creating a boolean
        containsDuplicate = len(entriesByID) != 0 or len(entriesByUsername) != 0 or len(duplicates) != 0
        print("Contains duplicate: " + str(containsDuplicate))

        if not containsDuplicate:

            # Insert profile data in profiles table if no entry currently exists
            cursor.execute(
                "INSERT INTO profiles (profile_id, fetched_time, username, name, about, education, favorite_quotes, "
                "location, in_district, work, other_info) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (profileID, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), username, profile.get('Name'),
                 profile.get('About'),
                 profile.get('Education'), profile.get('Favorite Quotes'),
                 currentLocation, inDistrict, profile.get('Work'), profile.get('Basic Info'))
            )
            database.commit()
        else:
            if profileID is not None and username is not None:

                # Update profile data if we have both profileID and username
                cursor.execute(
                    "UPDATE profiles SET profile_id %s, username %s WHERE profile_id %s or username %s or "
                    "((name %s) and (about %s) and (education %s) and (favorite_quotes %s) and (location %s) and (work %s) and (other_info %s))"
                    % (process_value(profileID), process_value(username), process_value(profileID),
                       process_value(username), process_value(profile.get('Name')), process_value(profile.get('About')),
                       process_value(profile.get('Education')),
                       process_value(profile.get('Favorite Quotes')), process_value(currentLocation),
                       process_value(profile.get('Work')), process_value(profile.get('Basic Info')))
                )
                database.commit()
            elif profileID is not None and username is None:

                # Update profile data when we only know profileID
                cursor.execute(
                    "UPDATE profiles SET profile_id %s WHERE profile_id %s or username %s or "
                    "((name %s) and (about %s) and (education %s) and (favorite_quotes %s) and (location %s) and (work %s) and (other_info %s))"
                    % (process_value(profileID), process_value(profileID),
                       process_value(username), process_value(profile.get('Name')), process_value(profile.get('About')),
                       process_value(profile.get('Education')),
                       process_value(profile.get('Favorite Quotes')), process_value(currentLocation),
                       process_value(profile.get('Work')), process_value(profile.get('Basic Info')))
                )
                database.commit()
            elif profileID is None and username is not None:

                # Update profile data when we only username
                cursor.execute(
                    "UPDATE profiles SET username %s WHERE profile_id %s or username %s or "
                    "((name %s) and (about %s) and (education %s) and (favorite_quotes %s) and (location %s) and (work %s) and (other_info %s))"
                    % (process_value(username), process_value(profileID),
                       process_value(username), process_value(profile.get('Name')), process_value(profile.get('About')),
                       process_value(profile.get('Education')),
                       process_value(profile.get('Favorite Quotes')), process_value(currentLocation),
                       process_value(profile.get('Work')), process_value(profile.get('Basic Info')))
                )
                database.commit()

    # Helps delay Facebook lockout. Generates random number from 5-10 (10 exclusive)
    timeout = (random() * 5) + 5
    print("Sleeping for " + str(timeout) + " seconds")
    time.sleep(timeout)


# Get posts
posts = get_posts('repannaeshoo', pages=1, timeout=20, options={"progress": True, "comments": True, "reactors": True},
                  )

# Process everything the function got
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

    # Get reactions to the post
    reactions = post.get('reactors')
    if reactions is not None:
        print("Reactions count: " + str(len(reactions)))
        for reaction in reactions:

            # Get username or ID from the URL
            url = reaction['link']
            print("Reactor url " + url)
            try:
                # Will go to the 'expect' if not in the string
                url.index("profile.php")
                # Gets substring after "id=" and up to "&fref"
                profileID = url[url.index("id=") + 3: url.index("&fref")]
                username = None
            except:
                # The first 21 character are 'https://' etc. Gets substring after that and up to "?fref"
                username = url[21:url.index("?fref")]
                # Sometimes a '/' exists at the end
                if username[-1] == "/":
                    username = username[:-1]
                profileID = None

            #  Get username using profileID
            if profileID is not None:
                profile = get_profile(profileID)
                username = username_from_profile(profile)

            # Check if entry already exists
            cursor.execute(f"SELECT * FROM reactions where post_id = {post['post_id']} and username = '{username}'")
            entries = cursor.fetchall()
            if len(entries) == 0:
                # Insert data into
                cursor.execute(
                    "INSERT INTO reactions (post_id, fetched_time, reactor, profile_id, username, type) VALUES (%s, %s, %s, %s, %s, %s)",
                    (post['post_id'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'), reaction['name'], profileID,
                     username, reaction['type'])
                )
                database.commit()
                process_profile(profileID, username)

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

            # Process comment text
            commentText = comment['comment_text']
            print("Raw text: " + commentText)
            commentText = commentText.replace(",", "")

            while commentText.find("http") != -1:
                spaceIndex = commentText[commentText.find("http"):].find(" ")
                print("space index: " + str(spaceIndex))
                if spaceIndex == - 1:
                    commentText = commentText[:commentText.find("http")]
                else:
                    commentText = commentText[:commentText.find("http")] + commentText[spaceIndex:]

            print("Processed text: " + commentText)

            # Insert data in comments table
            cursor.execute(
                "REPLACE INTO comments (comment_id, post_id, fetched_time, comment_url, commenter, commenter_id, "
                "commenting_time, comment_text, replies) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (comment['comment_id'], post['post_id'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                 comment['comment_url'], comment['commenter_name'], comment['commenter_id'], comment['comment_time'],
                 commentText, repliesCount)
            )
            database.commit()
            process_profile(comment['commenter_id'], None)

            # Get all the replies
            replies = comment.get('replies')
            if replies is not None:
                for reply in replies:
                    print(reply)

                    # Process reply text
                    replyText = reply['comment_text']
                    print("Raw text: " + replyText)
                    replyText = replyText.replace(",", "")

                    while replyText.find("http") != -1:
                        print("Processing text: " + replyText)
                        spaceIndex = replyText[replyText.find("http"):].find(" ")
                        print("space index: " + str(spaceIndex))
                        if spaceIndex == - 1:
                            replyText = replyText[:replyText.find("http")]
                        else:
                            replyText = replyText[:replyText.find("http")] + replyText[spaceIndex:]

                    print("Processed text: " + replyText)

                    # Insert data in replies table
                    cursor.execute(
                        "REPLACE INTO replies (comment_id, parent_comment_id, fetched_time, comment_url, commenter, commenter_id, "
                        "commenting_time, comment_text) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                        (reply['comment_id'], comment['comment_id'], datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                         reply['comment_url'], reply['commenter_name'], reply['commenter_id'], reply['comment_time'],
                         replyText)
                    )
                    database.commit()
                    process_profile(reply['commenter_id'], None)
