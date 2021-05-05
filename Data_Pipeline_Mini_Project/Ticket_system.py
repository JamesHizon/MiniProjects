# Ticket System Project

# Import packages
import os
import pandas as pd
import mysql.connector
import logging


# Set up database connection:
def get_db_connection():
    connection = None
    try:
        connection = mysql.connector.connect(user="root",
                                             password="Romans116",
                                             host="localhost",
                                             port="3306",
                                             database="ticket_system_db")
    except Exception as error:
        logging.error("Error while connecting to database for job tracker", error)
    return connection


# Load CSV to table:
def load_third_party(connection, file_path_csv):

    """
    This function is used to create a cursor to establish a database connection, execute DDL statement, as well as read the CSV file to store
    data into MySQL. Currently, I am stuck on the following error:

        Message: 'Error while connecting to MySQL'
        Arguments: (DatabaseError(1265, "1265 (01000): Data truncated for column 'trans_date' at row 1", '01000'),)

        Note:
            - I have tried using both pandas and CSV packages to iterate through CSV file and store data.


    :param connection: use get_db_connection() to obtain database connection
    :param file_path_csv:
    :return: Nothing is returned. Simply, data is being loaded, but we can try to
    create a logging statement, etc.
    """
    try:
        # Create cursor object pointing to connection
        cursor = connection.cursor()
        # Iterate through CSV file and execute insert statement
        ticket_data = pd.read_csv(file_path_csv, header=None)
        # Create tickets table to store data
        sql_ddl_statement = """
        
        DROP DATABASE IF EXISTS ticket_system_db;
        CREATE DATABASE ticket_system_db;
        USE ticket_system_db;
        
        CREATE TABLE ticket_sales(
            ticket_id INT,
            trans_date DATE,
            event_id INT,
            event_name VARCHAR(50),
            event_date DATE,
            event_type VARCHAR(10),
            event_city VARCHAR(20),
            customer_id INT,
            price DECIMAL(7,2),
            num_tickets INT,
            PRIMARY KEY(ticket_id)
            );
        """

        for _ in cursor.execute(sql_ddl_statement, multi=True):
            pass

        for i, row in ticket_data.iterrows():
            sql = """INSERT INTO ticket_system_db.ticket_sales
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""
            # print(row)
            for value in row:
                value = str(value)
            cursor.execute(sql, tuple(row))
        connection.commit()
        cursor.close()
    except Exception as e:
        cursor.close()
        logging.error("Error while connecting to MySQL", e)


# Display statistical information:
def query_popular_tickets(connection):
    # Get the most popular ticket in the past month
    sql_statement = """
    SELECT event_name, SUM(num_tickets) AS total_tickets 
    FROM ticket_system_db.ticket_sales
    GROUP BY event_name 
    ORDER BY SUM(num_tickets) DESC 
    LIMIT 1;
    """
    cursor = connection.cursor()
    cursor.execute(sql_statement)
    records = cursor.fetchall()
    cursor.close()
    return records


if __name__ == "__main__":

    # Connect to MySQL database
    connect = get_db_connection()

    # Load data into MySQL (first specify file path):
    current_wd = os.getcwd()
    # csv_file_path = current_wd + '/third_party_sales_1.csv'
    csv_file_path = current_wd + '/sales_data.csv'
    load_third_party(connect, csv_file_path)

    # Execute SQL query
    print(query_popular_tickets(connect))