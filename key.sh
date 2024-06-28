#!/bin/bash

# Function to create an SSH key pair if it does not already exist
create_ssh_key() {
    if [ ! -f ~/.ssh/id_rsa ]; then
        echo "SSH key not found. Creating a new SSH key pair."
        ssh-keygen -t rsa -b 4096 -N "" -f ~/.ssh/id_rsa
    else
        echo "SSH key already exists."
    fi
}

# Function to distribute the SSH public key to a list of machines
distribute_ssh_key() {
    local machines_file=$1
    local public_key=$(cat ~/.ssh/id_rsa.pub)

    if [ ! -f "$machines_file" ]; then
        echo "Machines file not found!"
        exit 1
    fi

    while IFS= read -r machine; do
        if [ -n "$machine" ]; then
            echo "Copying SSH key to $machine"
            ssh-copy-id -i ~/.ssh/id_rsa.pub "$machine"
        fi
    done < "$machines_file"
}

# Main script execution
main() {
    machines_file="machines.txt"
    create_ssh_key
    distribute_ssh_key "$machines_file"
    echo "SSH key distribution completed."
}

# Run the main function
main
