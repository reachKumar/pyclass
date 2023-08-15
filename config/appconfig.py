# Code goes from here
# importing config parser
import configparser

# creating the object of configparser
config_data = configparser.ConfigParser()

# reading data
config_data.read("pyautomation.ini")

# app configuration data
database = config_data["database"]

# admin data
user = config_data["user"]

# admin data
smpt = config_data["smpt"]

print("database data")
for database_data in database:
   print(f"{database_data} = {database.get(database_data)}")

print("\nuser data")
for user_data in user:
   print(f"{user_data} = {user.get(user_data)}")

print("\nSMTP data")
for smpt_data in smpt:
   print(f"{smpt_data} = {smpt.get(smpt_data)}")