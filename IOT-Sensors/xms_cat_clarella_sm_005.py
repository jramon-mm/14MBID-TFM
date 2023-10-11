#!/usr/bin/env python3

from confluent_kafka import Producer
import re
import json
import time

FILENAME = '/home/jramon/Defensa/input/XMS-CAT_XMS-CAT_Clarella_sm_0.050000_0.050000_CS655_20230101_20230401.stm'

def define_sensor_type():
	sensor_type = ''

	# The name of the file give us the sensor type
	if FILENAME.split('/')[-1].split('_')[3] == 'sm':
		sensor_type = 'Soil moisture'
	elif FILENAME.split('/')[-1].split('_')[3] == 'ts':
		sensor_type = 'Soil temperature'
	elif FILENAME.split('/')[-1].split('_')[3] == 'ta':
		sensor_type = 'Air temperature'
	elif FILENAME.split('/')[-1].split('_')[3] == 'p':
		sensor_type = 'Precipitation'
	else:
		sensor_type = 'Unknown'

	return sensor_type

def process_header(header_line):
	# Removing not desirable blankspaces
	header_line = re.sub('\\s+', ' ', header_line)
	
	# Splitting data header fields
	header_fields = header_line.split(' ')

	# Saving data header info into a dictionary
	header_data = {
		'cse_id': header_fields[0],
		'network': header_fields[1],
		'station': header_fields[2],
		'latitude': float(header_fields[3]),
		'longitude': float(header_fields[4]),
		'elevation': float(header_fields[5]),
		'depth_from': float(header_fields[6]),
		'depth_to': float(header_fields[7]),
		'sensor_model': header_fields[8]
	}

	return header_data

def complete_message(message, values_line):
	# Removing not desirable blankspaces
	values_line = re.sub('\\s+', ' ', values_line)

	# Splitting values line fields
	values_fields = values_line.split(' ')

	# Creating dictionary with desired data
	message['measurement_datetime'] = values_fields[0] + ' ' + values_fields[1]
	message['measurement'] = float(values_fields[2])
	message['ismn_quality_flag'] = values_fields[3]
	message['data_owner_quality_flag'] = values_fields[4]

def read_and_send_data():
	# Figuring out what sensor is
	sensor_type = define_sensor_type()

	# Creating kafka producer
	producer = Producer({'bootstrap.servers': 'localhost:9092'})

	# Reading file
	with open(FILENAME, 'r') as f:
		# Processing file header info
		message = process_header(f.readline())

		# Reading and sending lines
		for line in f:
			# Adding values line data to the message
			complete_message(message, line)

			# Adding sensor type
			message['sensor_type'] = sensor_type

			# Setting target topic
			topic = sensor_type.lower().replace(' ', '_')

			msg_key = str(message['latitude']) + '_' + str(message['longitude']) + '_' + str(message['elevation'])

			# Sending message and sleeping 10 seconds
			producer.produce(topic, key=msg_key, value=json.dumps(message).encode('utf-8'))
			print('A Message has been sent')
			time.sleep(10)

		# Closing file
		f.close()

		# Assuring that all messages are sent
		producer.flush()

def main():
	read_and_send_data()

if __name__ == '__main__':
	main()