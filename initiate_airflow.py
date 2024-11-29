import subprocess

# Start Airflow services
subprocess.run(["/opt/airflow/start-services.sh"], check=True)
subprocess.run(["/opt/airflow/start.sh"], check=True)

# Create an Airflow user with admin privileges
subprocess.run([
    "airflow", "users", "create",
    "--email", "", #Add your email here
    "--firstname", "", #Add firstname
    "--lastname", "", #Second name
    "--password", "", #Password
    "--role", "", #Role
    "--username", "" #Username
], check=True)
