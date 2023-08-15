# Code goes from here
# importing config parser
import configparser

# creating object of configparser
config = configparser.ConfigParser()

# creating a section
config.add_section("database")

# adding key-value pairs
config.set("database", "host", "local host")
config.set("database", "admin", "local admin")
config.set("database", "password", "qwerty@123")
config.set("database", "port no", "2253")
config.set("database", "database", "SQL")
config.set("database", "version", "1.1.0")

# creating another section
config.add_section("user")

# adding key-value pairs
config.set("user", "name", "test_name")
config.set("user", "e-mail", "test@example.com")
config.set("user", "id", "4759422")

# creating another section -- SMTP
config.add_section("smpt")

# adding key-value pairs SMTP
config.set("smpt", "SMTPServerName", "test_name")
config.set("smpt", "Port", "test@example.com")
config.set("smpt", "FromEmail", "4759422")
config.set("smpt", "Password", "4759422")
config.set("smpt", "ToEmail", "4759422")

with open("pyautomation.ini", 'w') as pyautomation:
   config.write(pyautomation)
   
with open("pyautomation.config", 'w') as pyautomation:
   config.write(pyautomation)