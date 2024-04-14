import mysql.connector
import csv

team = 10

# Requirement1: create schema ( name: DMA_team## )
def requirement1(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    print('Creating schema...')
    
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS DMA_team{str(team)};')
    cursor.execute(f'USE DMA_team{str(team)};')

    cursor.close()


# Requierement2: create table
def requirement2(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    print('Creating tables...')
    
    cursor.execute(f'USE DMA_team{str(team)};')

    cursor.execute('CREATE TABLE IF NOT EXISTS Category (\
                   category_id INT(11) PRIMARY KEY, \
                   name VARCHAR(255) NOT NULL, \
                   num_restaurants INT(11) DEFAULT 0);')
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Collection (\
                   user_id VARCHAR(255), \
                   restaurant_id VARCHAR(255), \
                   PRIMARY KEY (user_id, restaurant_id));')\
                   
    cursor.execute('CREATE TABLE IF NOT EXISTS Follow (\
                   follower_id VARCHAR(255), \
                   followee_id VARCHAR(255), \
                   PRIMARY KEY (follower_id, followee_id));')
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Location (\
                   location_id INT(11) PRIMARY KEY, \
                   name VARCHAR(255) NOT NULL, \
                   num_restaurants INT(11) DEFAULT 0);')
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Menu (\
                   menu_name VARCHAR(255), \
                   price_min INT(11), \
                   price_max INT(11), \
                   restaurant VARCHAR(255), \
                   PRIMARY KEY (menu_name, restaurant));')
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Posted_On (\
                   post_id INT(11), \
                   menu_name VARCHAR(255), \
                   restaurant VARCHAR(255), \
                   PRIMARY KEY (post_id, menu_name, restaurant));')
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Post (\
                   post_id INT(11) PRIMARY KEY, \
                   blog_title VARCHAR(255) NOT NULL, \
                   blog_URL VARCHAR(255) NOT NULL, \
                   post_date DATETIME NOT NULL, \
                   restaurant VARCHAR(255) NOT NULL);')
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Restaurant (\
                   restaurant_id VARCHAR(255) PRIMARY KEY, \
                   restaurant_name VARCHAR(255) NOT NULL, \
                   lunch_price_min INT(11), \
                   lunch_price_max INT(11), \
                   dinner_price_min INT(11), \
                   dinner_price_max INT(11), \
                   location INT(11) NOT NULL, \
                   category INT(11) NOT NULL, \
                   mean_review_score DECIMAL(11,1) DEFAULT 0.0, \
                   num_reviews INT(11) DEFAULT 0, \
                   num_collection INT(11) DEFAULT 0);')
    
    cursor.execute('CREATE TABLE IF NOT EXISTS Review (\
                   review_id INT(11) PRIMARY KEY, \
                   review_content LONGTEXT, \
                   reg_date DATETIME, \
                   user_id VARCHAR(255) NOT NULL, \
                   total_score DECIMAL(11,1) NOT NULL, \
                   taste_score DECIMAL(11,1) DEFAULT 0.0, \
                   service_score DECIMAL(11,1) DEFAULT 0.0, \
                   mood_score DECIMAL(11,1) DEFAULT 0.0, \
                   restaurant VARCHAR(255) NOT NULL);')
    
    cursor.execute('CREATE TABLE IF NOT EXISTS User (\
                   user_id VARCHAR(255) PRIMARY KEY, \
                   user_name VARCHAR(255) NOT NULL, \
                   region VARCHAR(255), \
                   num_reviews INT(11) DEFAULT 0, \
                   num_followers INT(11) DEFAULT 0, \
                   num_followees INT(11) DEFAULT 0, \
                   num_collections INT(11) DEFAULT 0, \
                   mean_review_score DECIMAL(11,1) DEFAULT 0.0);')

    # Triggers for derived attributes
    cursor.execute('CREATE TRIGGER IF NOT EXISTS update_num_restaurants_category \
                   AFTER INSERT ON Restaurant \
                   FOR EACH ROW \
                   BEGIN \
                       UPDATE Category \
                       SET num_restaurants = num_restaurants + 1 \
                       WHERE category_id = NEW.category; \
                   END')
    
    cursor.execute('CREATE TRIGGER IF NOT EXISTS update_num_restaurants_location \
                   AFTER INSERT ON Restaurant \
                   FOR EACH ROW \
                   BEGIN \
                       UPDATE Location \
                       SET num_restaurants = num_restaurants + 1 \
                       WHERE location_id = NEW.location; \
                   END')
    
    cursor.execute('CREATE TRIGGER IF NOT EXISTS update_num_reviews_restaurant \
                   AFTER INSERT ON Review \
                   FOR EACH ROW \
                   BEGIN \
                       UPDATE Restaurant \
                       SET num_reviews = num_reviews + 1, \
                           mean_review_score = (mean_review_score * CAST(num_reviews AS DECIMAL(11,1)) + NEW.total_score) / (num_reviews + 1) \
                       WHERE restaurant_id = NEW.restaurant; \
                   END')
    
    cursor.execute('CREATE TRIGGER IF NOT EXISTS update_num_reviews_user \
                     AFTER INSERT ON Review \
                        FOR EACH ROW \
                        BEGIN \
                            UPDATE User \
                            SET num_reviews = num_reviews + 1, \
                                mean_review_score = (mean_review_score * CAST(num_reviews AS DECIMAL(11,1)) + NEW.total_score) / (num_reviews + 1) \
                            WHERE user_id = NEW.user_id; \
                        END')
    
    cursor.execute('CREATE TRIGGER IF NOT EXISTS update_num_collections_restaurant \
                   AFTER INSERT ON Collection \
                   FOR EACH ROW \
                   BEGIN \
                       UPDATE Restaurant \
                       SET num_collection = num_collection + 1 \
                       WHERE restaurant_id = NEW.restaurant_id; \
                   END')
    
    cursor.execute('CREATE TRIGGER IF NOT EXISTS update_num_collections_user \
                   AFTER INSERT ON Collection \
                   FOR EACH ROW \
                   BEGIN \
                       UPDATE User \
                       SET num_collections = num_collections + 1 \
                       WHERE user_id = NEW.user_id; \
                   END')
    
    cursor.execute('CREATE TRIGGER IF NOT EXISTS Update_num_followers_user \
                   AFTER INSERT ON Follow \
                   FOR EACH ROW \
                   BEGIN \
                       UPDATE User \
                       SET num_followers = num_followers + 1 \
                       WHERE user_id = NEW.follower_id; \
                   END')
    
    cursor.execute('CREATE TRIGGER IF NOT EXISTS Update_num_followees_user \
                     AFTER INSERT ON Follow \
                        FOR EACH ROW \
                        BEGIN \
                            UPDATE User \
                            SET num_followees = num_followees + 1 \
                            WHERE user_id = NEW.followee_id; \
                        END')

    cursor.close()


# Requirement3: insert data
def requirement3(host, user, password, directory):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    print('Inserting data...')

    cursor.execute(f'USE DMA_team{str(team)};')
    with open(directory + 'Category.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Category (name, category_id) VALUES (%s, %s);', row)
    with open(directory + 'Location.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Location (name, location_id) VALUES (%s, %s);', row)
    with open(directory + 'User.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO User (user_id, user_name, region) VALUES (%s, %s, %s);', row)
    with open(directory + 'Restaurant.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Restaurant (restaurant_id, restaurant_name, lunch_price_min, lunch_price_max, dinner_price_min, dinner_price_max, location, category) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);', row)
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
    with open(directory + 'Review.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            total_score = row[4] if len(row[4]) else None
            taste_score = row[5] if len(row[5]) else None
            service_score = row[6] if len(row[6]) else None
            mood_score = row[7] if len(row[7]) else None
            if taste_score is None:
                taste_score = 0
            if service_score is None:
                service_score = 0
            if mood_score is None:
                mood_score = 0
            if total_score is None:
                total_score = (float(taste_score) + float(service_score) + float(mood_score)) / 3
            cursor.execute('INSERT INTO Review (review_id, review_content, reg_date, user_id, total_score, taste_score, service_score, mood_score, restaurant) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);', (row[0], row[1], row[2], row[3], total_score, taste_score, service_score, mood_score, row[8]))
    with open(directory + 'Menu.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            price_min = row[1] if len(row[1]) else None
            price_max = row[2] if len(row[2]) else None
            cursor.execute('INSERT INTO Menu (menu_name, price_min, price_max, restaurant) VALUES (%s, %s, %s, %s);', (row[0], price_min, price_max, row[3]))
    
    with open(directory + 'Post.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Post (blog_title, blog_URL, post_date, restaurant, post_id) VALUES (%s, %s, %s, %s, %s);', row)
    with open(directory + 'Post_Menu.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute('INSERT INTO Posted_On (post_id, menu_name, restaurant) VALUES (%s, %s, %s);', row)

    # Save all changes
    cnx.commit()

    cursor.close()


# Requirement4: add constraint (foreign key)
def requirement4(host, user, password):
    cnx = mysql.connector.connect(host=host, user=user, password=password)
    cursor = cnx.cursor()
    cursor.execute('SET GLOBAL innodb_buffer_pool_size=2*1024*1024*1024;')
    print('Adding constraints...')
    
    cursor.execute(f'USE DMA_team{str(team)};')
    # Foreign key constraints
    cursor.execute('ALTER TABLE Collection ADD FOREIGN KEY (user_id) REFERENCES User(user_id);')
    cursor.execute('ALTER TABLE Collection ADD FOREIGN KEY (restaurant_id) REFERENCES Restaurant(restaurant_id);')
    cursor.execute('ALTER TABLE Follow ADD FOREIGN KEY (follower_id) REFERENCES User(user_id);')
    cursor.execute('ALTER TABLE Follow ADD FOREIGN KEY (followee_id) REFERENCES User(user_id);')
    cursor.execute('ALTER TABLE Menu ADD FOREIGN KEY (restaurant) REFERENCES Restaurant(restaurant_id);')
    cursor.execute('ALTER TABLE Posted_On ADD FOREIGN KEY (post_id) REFERENCES Post(post_id);')
    cursor.execute('ALTER TABLE Posted_On ADD FOREIGN KEY (menu_name) REFERENCES Menu(menu_name);')
    cursor.execute('ALTER TABLE Posted_On ADD FOREIGN KEY (restaurant) REFERENCES Restaurant(restaurant_id);')
    cursor.execute('ALTER TABLE Post ADD FOREIGN KEY (restaurant) REFERENCES Restaurant(restaurant_id);')
    cursor.execute('ALTER TABLE Restaurant ADD FOREIGN KEY (location) REFERENCES Location(location_id);')
    cursor.execute('ALTER TABLE Restaurant ADD FOREIGN KEY (category) REFERENCES Category(category_id);')
    cursor.execute('ALTER TABLE Review ADD FOREIGN KEY (user_id) REFERENCES User(user_id);')
    cursor.execute('ALTER TABLE Review ADD FOREIGN KEY (restaurant) REFERENCES Restaurant(restaurant_id);')

    cursor.close()


host = 'localhost'
user = 'root'
pwpath = 'password.txt'

try:
    with open(pwpath, 'r') as f:
        password = f.read().strip()
except FileNotFoundError:
    password = ''

directory_in = 'dataset/'


requirement1(host=host, user=user, password=password)
requirement2(host=host, user=user, password=password)
requirement3(host=host, user=user, password=password, directory=directory_in)
requirement4(host=host, user=user, password=password)
print('Done!')