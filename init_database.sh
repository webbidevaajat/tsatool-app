#!/bin/sh
#
echo "
This script checks if Postgres and TimescaleDB
are installed on the machine.
If yes, a new database for tsatool as well as
necessary tables are created.
A database configuration file for the app
is created accordingly.
"
# 
# Assert root user status
if [ "$USER" != "root" ]
then
    echo "Please run this script as root user,
e.g. by running 'sudo su' and providing root password first."
    exit
fi
# 
# Check Postgres server installation
# TODO
# 
# Check TimescaleDB installation
# TODO
# 
# Create config file and ask parameters
# TODO
# 
# Create database
# TODO
# 
# Create Timescale extension
# TODO
# 
# Create users
# TODO
# 
# Create tables
# TODO
