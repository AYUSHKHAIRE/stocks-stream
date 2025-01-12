#!/bin/bash

# Check MySQL service status
SERVICE="mysql"

if systemctl is-active --quiet $SERVICE; then
    echo "MySQL is running."
else
    echo "MySQL is not running."
    echo "Attempting to start MySQL..."
    
    # Try to start MySQL
    if systemctl start $SERVICE; then
        echo "MySQL started successfully."
    else
        echo "Failed to start MySQL. Check the logs for more details."
    fi
fi

# show all databases 
if [ "$#" -ne 3 ]; then
    echo "Usage for mysql : $0 <username> <password> <database_name>"
    exit 1
fi

USERNAME=$1
PASSWORD=$2
DATABASE=$3

# Create the database
mysql -u "$USERNAME" -p"$PASSWORD" -e "CREATE DATABASE $DATABASE;"

# Check if the database creation was successful
if [ $? -eq 0 ]; then
    echo "Database '$DATABASE' created successfully."
else
    echo "Failed to create the database '$DATABASE'."
fi