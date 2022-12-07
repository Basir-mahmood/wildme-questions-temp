

# I/O assumption
# For this problem, it is assumed that `client IP`,the `cookie ID`,  and the `username` are all string datatype.
# The per hour is assumed to be the last hour duration.
# The following code is written in function format, as it was told in the description to write it in the function format.
# Another more feasible approach would be to write a modular based code, where db functionalities should be in a separate class.
# Moreover, utils will be in a separate file, for better code readability.

# Database Assumption
# It is assumed that the OLTP database is built such as Postgres. And the schema for the database LoginRateLimiterRecoder with three tables wll be as follows,
# TABLE client_ip
# | date_timestamp < datetime object > | client_ip < VARCHAR (100) >
# TABLE cookie_id
# | date_timestamp < datetime object > | cookie_id < VARCHAR (16)> |
# TABLE username
# | date_timestamp < datetime object > | username < VARCHAR (100) > |


# As states is assumed to be persisted
import random
import time
import datetime
from typing import Dict, List
from multiprocessing import Process

global lastDbDataDeleted
lastDbDataDeleted = datetime.datetime(2000, 1, 1)


# It is assumed that this method makes connection with the databse, and will not
#            be needed to check and make the connection again.
# For example with postgres through psycopg2 # conn = psycopg.connect(db_url)


def call_db_data_delete():
    # one_hour_before = datetime.datetime.now() - datetime.timedelta(minutes=60)
    # for table_name in ["client_ip", "cookie_id", "username"]:
    #     conn.run("DELETE FROM table_name WHERE date_timestamp < one_hour_before")
    pass


def validate_data_from_db(time_interval, input_field_name, input_field_val, count) -> None:
    """
        Inputs:
            time <int>: Time in seconds
            input_field_name <string>: Field name to extract data from db
            input_field_val <string>: Field value to extract data from db


        Outputs:
            Dictionary {
                    "data_timestamp" : List[vals],
                    FIELD_NAME : List[vals],
            }

    """

    # And returns the obtained results from the database in Dictionary.
    time_made = datetime.datetime.now() - datetime.timedelta(seconds=time_interval)

    # psycopg QUERY FROM POSTGRES query = f" SELECT count(*) FROM input_field_name
    #                                WHERE date_timestamp <= time_made AND {input_field_name} = input_field_val "
    # And suppose each query takes 0.01 seconds to 0.9 seconds
    # And stores the result in obtained_db_results
    # For Example, with the code: obtained_db_results = await conn.fetch(query)
    time.sleep(random.random())

    # Following line of code, produce 20% chances to produce invalid request, with randomness.
    obtained_db_results = int(random.random() * (count + (count/5)))
    if time_interval > 60:
        error_time_mnts = str(int(time_interval/60)) + " minutes"
        error_time_seconds = str(int(time_interval % 60)) + " seconds"
        total_time = error_time_mnts + error_time_seconds
    else:
        total_time = str(time_interval) + " seconds"
    if obtained_db_results > count:
        error_message = f"Request Unauthorized !!! This request to access cannot be processed further, as {input_field_name} : {input_field_val} has " +\
                        f"accessed {obtained_db_results} times in the last {total_time}. The allowed access is only  {count} times."
        raise Exception(error_message)
    else:
        print(
            f"Validating Request ...  {input_field_name} : {input_field_val} has " +
            f"accessed {obtained_db_results} times in the last {total_time}. The allowed access is only  {count} times."
        )
    return True


def check_field_i(
    input_field_name: str,
    input_field_val: str,
    count: List[int],
    time_in_seconds:  List[int]
) -> List[bool]:

    if len(count) != len(time_in_seconds):
        raise Exception(
            "The length of count and time_in_seconds should be same."
        )
    responses = []
    for i in range(len(count)):
        total_time_in_seconds_i = time_in_seconds[i]
        count_i = count[i]

        resp = validate_data_from_db(
            time_interval=total_time_in_seconds_i,
            input_field_name=input_field_name,
            input_field_val=input_field_val,
            count=count_i,
        )
        responses.append(resp)
    return responses


def field_checker(client_ip: str,
                  cookie_id: str,
                  username: str,):
    clientIP_checker = check_field_i(
        input_field_name="client_ip",
        input_field_val=client_ip,
        count=[5, 15],
        time_in_seconds=[60, 60*60],  # 1 minute, 1 hour
    )
    cookieId_checker = check_field_i(
        input_field_name="cookie_id",
        input_field_val=cookie_id,
        count=[2],
        time_in_seconds=[10],  # 10 seconds
    )
    username_checker = check_field_i(
        input_field_name="username",
        input_field_val=username,
        count=[10],
        time_in_seconds=[60*60],  # 1 hour
    )


def loginRateLimiter(
    client_ip: str,
    cookie_id: str,
    username: str,
) -> None:
    time_now = datetime.datetime.now()
    global lastDbDataDeleted

    field_checker_inputs = [client_ip, cookie_id, username]
    field_checker(*field_checker_inputs)
    # The idea behind the following check routine is that the  after every 61 minutes,
    # the data which is older than one hour be deleted, as per the requirement, that data is not
    # of our use. By this way, the db size is limited only for the data i.e., from the last one hour.
    # Moreover, this will ensure, efficient query time, and reduce irrelevant space consumption.
    # The reason for deleting after only one hour that there is no overlap in time for data deletion.
    # And it is not lesser than one hour because the highest time that is required to analyze is one hour.
    if time_now - lastDbDataDeleted > datetime.timedelta(minutes=61):
        call_db_data_delete()
        lastDbDataDeleted = datetime.datetime.now()
    print("Request Authorized...")


if __name__ == "__main__":
    loginRateLimiter(
        client_ip="0.0.0.0",
        cookie_id="dummy_cookie_id",
        username="_user_01",
    )
