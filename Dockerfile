# Use Python 3.11 as the base image
FROM python:3.11

# Set the working directory
WORKDIR /projectKafka
ENV PYTHONPATH "."

# Install the required packages
COPY requirements.txt /projectKafka/
RUN pip install -r requirements.txt

# Copy the publisher and consumer scripts
COPY ./ /projectKafka/
# Set the entrypoint
ENTRYPOINT ["python"]
# Set the default command to run the publisher and consumer
CMD ["/projectKafka/consumers/address_resolve.py"]