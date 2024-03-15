#! /bin/bash


cd $DATACOVES__DBT_HOME

# sqlmesh --version
# sqlmesh init -t dbt

# This command retrieves the private IP address using
# hostname -I, extracts the first IP address with
# awk '{print $1}', and then pipes it to xargs
# to append it to the
# sqlmesh ui --host command.

hostname -I | awk '{print $1}' | xargs sqlmesh ui --host
