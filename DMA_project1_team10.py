import mysql.connector
import csv
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# TODO: REPLACE THE VALUE OF VARIABLE team (EX. TEAM 1 --> team = 1)
team = 10


# Requirement1: create schema ( name: DMA_team## )
def requirement1(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    print('Creating schema...')
    
    # TODO: WRITE CODE HERE
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS DMA_team{str(team)};')
    cursor.execute(f'USE DMA_team{str(team)};')
    # TODO: WRITE CODE HERE
    cursor.close()


# Requierement2: create table
def requirement2(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    print('Creating tables...')
    
    # TODO: WRITE CODE HERE
    cursor.execute(f'USE DMA_team{str(team)};')
    cursor.execute('CREATE TABLE IF NOT EXISTS Category (\
                   category_id INT(11) PRIMARY KEY, \
                   name VARCHAR(255));')
    cursor.execute('CREATE TABLE IF NOT EXISTS Collection (\
                   user_id VARCHAR(255), \
                   restaurant_id VARCHAR(255), \
                   PRIMARY KEY (user_id, restaurant_id));')
    cursor.execute('CREATE TABLE IF NOT EXISTS Follow (\
                   follower_id VARCHAR(255), \
                   followee_id VARCHAR(255), \
                   PRIMARY KEY (follower_id, followee_id));')
    cursor.execute('CREATE TABLE IF NOT EXISTS Location (\
                   location_id INT(11) PRIMARY KEY, \
                   name VARCHAR(255));')
    cursor.execute('CREATE TABLE IF NOT EXISTS Menu (\
                   menu_name VARCHAR(255), \
                   price_min DECIMAL(11), \
                   price_max DECIMAL(11), \
                   restaurant VARCHAR(255), \
                   PRIMARY KEY (menu_name, restaurant));')
    cursor.execute('CREATE TABLE IF NOT EXISTS Post_Menu (\
                   post_id INT(11), \
                   menu_name VARCHAR(255), \
                   restaurant VARCHAR(255), \
                   PRIMARY KEY (post_id, menu_name, restaurant));')
    cursor.execute('CREATE TABLE IF NOT EXISTS Post (\
                   post_id INT(11) PRIMARY KEY, \
                   blog_title VARCHAR(255), \
                   blog_URL VARCHAR(255), \
                   post_date DATETIME, \
                   restaurant VARCHAR(255));')
    cursor.execute('CREATE TABLE IF NOT EXISTS Restaurant (\
                   restaurant_id VARCHAR(255) PRIMARY KEY, \
                   restaurant_name VARCHAR(255), \
                   lunch_price_min DECIMAL(11), \
                   lunch_price_max DECIMAL(11), \
                   dinner_price_min DECIMAL(11), \
                   dinner_price_max DECIMAL(11), \
                   location INT(11), \
                   category INT(11));')
    cursor.execute('CREATE TABLE IF NOT EXISTS Review (\
                   review_id INT(11) PRIMARY KEY, \
                   review_content LONGTEXT, \
                   reg_date DATETIME, \
                   user_id VARCHAR(255), \
                   total_score INT(11), \
                   taste_score INT(11), \
                   service_score INT(11), \
                   mood_score INT(11), \
                   restaurant VARCHAR(255));')
    cursor.execute('CREATE TABLE IF NOT EXISTS User (\
                   user_id VARCHAR(255) PRIMARY KEY, \
                   user_name VARCHAR(255), \
                   region VARCHAR(255));')
    # TODO: WRITE CODE HERE
    cursor.close()


# Requirement3: insert data
def requirement3(host, user, password, directory):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    print('Inserting data...')

    files = ['Category.csv', 'Collection.csv', 'Follow.csv', 'Location.csv', 'Menu.csv', 'Post_Menu.csv', 'Post.csv' 'Restaurant.csv' 'Review.csv', 'User.csv']
    
    # TODO: WRITE CODE HERE
    # if string directory does not contain BASE_DIR, add BASE_DIR to directory
    if BASE_DIR not in directory:
        directory = os.path.join(BASE_DIR, directory)

    cursor.execute(f'USE DMA_team{str(team)};')
    with open(directory + 'Category.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Category (name, category_id) VALUES (%s, %s);', row)
    with open(directory + 'Collection.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Collection (user_id, restaurant_id) VALUES (%s, %s);', row)
    with open(directory + 'Follow.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Follow (followee_id, follower_id) VALUES (%s, %s);', row)
    with open(directory + 'Location.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Location (name, location_id) VALUES (%s, %s);', row)
    with open(directory + 'Menu.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            price_min = row[1] if len(row[1]) else None  # Assign a default value of 0.0 if 'price_min' is empty
            price_max = row[2] if len(row[2]) else None  # Assign a default value of 0.0 if 'price_max' is empty
            cursor.execute('INSERT INTO Menu (menu_name, price_min, price_max, restaurant) VALUES (%s, %s, %s, %s);', (row[0], price_min, price_max, row[3]))
    with open(directory + 'Post_Menu.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Post_Menu (post_id, menu_name, restaurant) VALUES (%s, %s, %s);', row)
    with open(directory + 'Post.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Post (blog_title, blog_URL, post_date, restaurant, post_id) VALUES (%s, %s, %s, %s, %s);', row)
    with open(directory + 'Restaurant.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Restaurant (restaurant_id, restaurant_name, lunch_price_min, lunch_price_max, dinner_price_min, dinner_price_max, location, category) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);', row)
    with open(directory + 'Review.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Review (review_id, review_content, reg_date, user_id, total_score, taste_score, service_score, mood_score, restaurant) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);', row)
    with open(directory + 'User.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO User (user_id, user_name, region) VALUES (%s, %s, %s);', row)

    # TODO: WRITE CODE HERE
    cursor.close()


# Requirement4: add constraint (foreign key)
def requirement4(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    print('Adding constraints...')
    
    # TODO: WRITE CODE HERE
    cursor.execute(f'USE DMA_team{str(team)};')
    # Foreign key constraints
    cursor.execute('ALTER TABLE Collection ADD FOREIGN KEY (user_id) REFERENCES User(user_id);')
    cursor.execute('ALTER TABLE Collection ADD FOREIGN KEY (restaurant_id) REFERENCES Restaurant(restaurant_id);')
    cursor.execute('ALTER TABLE Follow ADD FOREIGN KEY (follower_id) REFERENCES User(user_id);')
    cursor.execute('ALTER TABLE Follow ADD FOREIGN KEY (followee_id) REFERENCES User(user_id);')
    cursor.execute('ALTER TABLE Menu ADD FOREIGN KEY (restaurant) REFERENCES Restaurant(restaurant_id);')
    cursor.execute('ALTER TABLE Post_Menu ADD FOREIGN KEY (post_id) REFERENCES Post(post_id);')
    cursor.execute('ALTER TABLE Post_Menu ADD FOREIGN KEY (menu_name) REFERENCES Menu(menu_name);')
    cursor.execute('ALTER TABLE Post_Menu ADD FOREIGN KEY (restaurant) REFERENCES Restaurant(restaurant_id);')
    cursor.execute('ALTER TABLE Post ADD FOREIGN KEY (restaurant) REFERENCES Restaurant(restaurant_id);')
    cursor.execute('ALTER TABLE Restaurant ADD FOREIGN KEY (location) REFERENCES Location(location_id);')
    cursor.execute('ALTER TABLE Restaurant ADD FOREIGN KEY (category) REFERENCES Category(category_id);')
    cursor.execute('ALTER TABLE Review ADD FOREIGN KEY (user_id) REFERENCES User(user_id);')
    cursor.execute('ALTER TABLE Review ADD FOREIGN KEY (restaurant) REFERENCES Restaurant(restaurant_id);')
    # TODO: WRITE CODE HERE
    cursor.close()


# TODO: REPLACE THE VALUES OF FOLLOWING VARIABLES
host = 'localhost'
user = 'root'
password = ''
directory_in = 'dataset/'


requirement1(host=host, user=user, password=password)
# requirement2(host=host, user=user, password=password)
# requirement3(host=host, user=user, password=password, directory=directory_in)
requirement4(host=host, user=user, password=password)
print('Done!')