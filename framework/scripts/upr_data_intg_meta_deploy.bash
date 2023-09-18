#!/bin/bash

# Function to stop the EC2 instance
stop_ec2_instance() {
    # Stop the EC2 instance
    if aws ec2 stop-instances --instance-ids $EC2_INSTANCE_ID; then
        echo "Stopping EC2 instance..."
    else
        echo "Error: Unable to stop the EC2 instance."
    fi

    # Wait for the instance to stop
    if aws ec2 wait instance-stopped --instance-ids $EC2_INSTANCE_ID; then
        echo "EC2 instance has been stopped."
    else
        echo "Error: EC2 instance failed to stop."
    fi
}

if [[ $# -lt 5 ]]; then
    echo "Invalid number of parameters. Please provide at least 5 parameters."
    echo "Expected parameters:"
    echo "1: Environment variable, which should be either 'dev', 'stg', 'stage02', or 'prod'."
    echo "2: Configuration file path."
    echo "3: Flag to control whether framework objects need to be created. Possible values are 'true' or 'false'."
    echo "4: Flag to control whether metadata components need to be deployed. Possible values are 'true' or 'false'."
    echo "5: Flag to control whether data components need to be deployed. Possible values are 'true' or 'false'."
    echo "6: Branch name for Git repository. If not provided, defaults to 'master'."
    exit 1
fi

# Read the environment variable and branch from command line
ENV=$(echo "$1" | tr '[:lower:]' '[:upper:]')
CONF_FILE=$2
FRAMEWORK_OBJECT_CREATION=$(echo "$3" | tr '[:lower:]' '[:upper:]')
METADATA_COMPONENT_DEPLOYMENT=$(echo "$4" | tr '[:lower:]' '[:upper:]')
DATA_COMPONENT_DEPLOYMENT=$(echo "$5" | tr '[:lower:]' '[:upper:]')
if [ -z "$6" ]; then
    BRANCH="master"
else
    BRANCH="$6"
fi

# Read the configuration file
if source $CONF_FILE; then
    echo "Configuration file loaded successfully."
else
    echo "Error: Unable to load the configuration file."
    exit 1
fi

# Set the variables based on the environment
if [[ $ENV == "DEV" ]]; then
    EC2_INSTANCE_ID=$DEV_INSTANCE_ID
    #PROJECT_PATH=$HOME_PATH/$ENV/$PROJECT_RELATIVE_PATH
    PROJECT_PATH=$HOME_PATH/$PROJECT_RELATIVE_PATH
elif [[ $ENV == "STG" ]]; then
    EC2_INSTANCE_ID=$DEV_INSTANCE_ID
    PROJECT_PATH=$HOME_PATH/$ENV/$PROJECT_RELATIVE_PATH
elif [[ $ENV == "STAGE02" ]]; then
    EC2_INSTANCE_ID=$STAGE02_INSTANCE_ID
    PROJECT_PATH=$HOME_PATH/$PROJECT_RELATIVE_PATH
elif [[ $ENV == "PROD" ]]; then
    EC2_INSTANCE_ID=$PROD_INSTANCE_ID
    PROJECT_PATH=$HOME_PATH/$PROJECT_RELATIVE_PATH
else
    echo "Invalid environment. Please specify dev/stg, stage02, or prod."
    exit 1
fi

# Validate deployment options
if [[ "$FRAMEWORK_OBJECT_CREATION" == "FALSE" && \
      "$METADATA_COMPONENT_DEPLOYMENT" == "FALSE" && \
      "$DATA_COMPONENT_DEPLOYMENT" == "FALSE" ]]; then
    echo "Invalid option. Please specify at least one of the following: \
    framework_object_creation, metadata_component_deployment, \
    data_component_deployment."
    exit 1
fi

# Start the EC2 instance
if aws ec2 start-instances --instance-ids $EC2_INSTANCE_ID; then
    echo "EC2 instance starting..."
else
    echo "Error: Unable to start the EC2 instance."
    exit 1
fi

# Wait for the instance to start
if aws ec2 wait instance-status-ok --instance-ids $EC2_INSTANCE_ID; then
    echo "EC2 instance started successfully."
else
    echo "Error: EC2 instance failed to start."
    exit 1
fi

# Get the public IP address of the instance
#EC2_IP=$(aws ec2 describe-instances --instance-ids $EC2_INSTANCE_ID --query "Reservations[0].Instances[0].PrivateIpAddress" --output=text)
EC2_IP=$(aws ec2 describe-instances --instance-ids $EC2_INSTANCE_ID --query "Reservations[0].Instances[0].PublicIpAddress" --output=text)

echo "EC2 IP Address: $EC2_IP"

VAR_NAME="${ENV}_PRIVATE_KEY_FILE"
KEY_FILE="${!VAR_NAME}"
echo "$KEY_FILE"

# Open SSH connection
ssh_conn="ssh -i $KEY_FILE -o StrictHostKeyChecking=no $EC2_USER@$EC2_IP"

# Deploy the code
if $ssh_conn <<EOF
    cd $HOME_PATH
    echo "Deployment starts..."

	if [[ $DATA_COMPONENT_DEPLOYMENT = "TRUE" || $METADATA_COMPONENT_DEPLOYMENT = "TRUE" ]];then
        echo "Deploying the code from Git repo..."
        if bash -i -c "\
            cd $HOME_PATH && \
            rm -rf $PROJECT_PATH && \
            git clone --branch $BRANCH $GIT_REPO $PROJECT_PATH && \
            cd $PROJECT_PATH && \
            python -m pip install -r requirements.txt"; then
            echo "Code deployment successful."
        else
            echo "Error: Code deployment failed."
            exit 1
        fi

		if [[ $METADATA_COMPONENT_DEPLOYMENT = "TRUE" ]];then
			echo "Deploying metadata component..."
            export SNOWSQL_PRIVATE_KEY_PASSPHRASE=$DEV_PASS_PHRASE
			if snowsql -c $ENV -f $PROJECT_PATH/framework/sql/metadata_component.sql -D FILE_HOME_PATH=$PROJECT_PATH -o variable_substitution=true; then
				echo "Metadata component code deployment successful."
			else
				echo "Error: SQL file failed to execute. Metadata component deployment failed."
				exit 1
            fi
		else
			echo "Skipping Metadata Code deployment."
        fi
    else
        echo "Skipping Code deployment."
    fi



    if [[ $FRAMEWORK_OBJECT_CREATION = "TRUE" ]];then
        echo "Creating framework objects creation..."
        export SNOWSQL_PRIVATE_KEY_PASSPHRASE=$DEV_PASS_PHRASE
        if snowsql -c $ENV -f $PROJECT_PATH/framework/sql/create_framework_objects.sql; then
            echo "Framework objects have been created successfully."
        else
            echo "Error: SQL file failed to execute. Framework objects creation failed."
            exit 1
        fi
    else
        echo "Skipping creation of framework objects."
    fi


EOF
then
    stop_ec2_instance
    echo "script has been completed successfully"
    echo "SSH connection closed."
else
    stop_ec2_instance
    echo "script has been failed"
    exit 1
fi