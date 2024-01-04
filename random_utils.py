import datetime
import random
import string


# Function to generate random birthday
def generate_random_birthday():
    start_date = datetime.date(1970, 1, 1)
    end_date = datetime.date(2000, 12, 31)
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + datetime.timedelta(days=random_days)


# Function to generate random phone number
def generate_random_phone():
    return "010" + str(random.randint(10000000, 99999999))


# Function to generate random email
def generate_random_email():
    username_length = random.randint(5, 10)
    username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=username_length))

    domain = "@example.com"
    return username + domain
