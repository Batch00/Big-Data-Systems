# Use ubuntu custom base image
FROM ubuntu:23.10

# Update and install any necessary dependencies
RUN apt-get update && \
    apt-get install -y bash wget unzip 

# Copy the count.sh script into the container
COPY count.sh /usr/local/bin/count.sh

# Define the command to run when the container starts
CMD ["/usr/local/bin/count.sh"]
