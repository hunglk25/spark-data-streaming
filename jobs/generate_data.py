from datetime import datetime, timedelta
import os
from confluent_kafka import SerializingProducer
import simplejson as json
import random
import uuid

# Define coordinates for HoChiMinh and HaNoi
HOCHIMINH_COORDINATES = {"latitude": 10.762622, "longitude": 106.660172}
HANOI_COORDINATES = {"latitude": 21.028511, "longitude": 105.804817}

# Calculate movement increments for simulating vehicle movement
LATITUDE_INCREMENT = (HANOI_COORDINATES['latitude'] - HOCHIMINH_COORDINATES["latitude"])/100
LONGITUDE_INCREMENT = (HANOI_COORDINATES['longitude'] - HOCHIMINH_COORDINATES["longitude"])/100


# Environment Variables for Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC =os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

random.seed(42) # Set seed for reproducibility
start_time = datetime.now()  # Initialize start time
start_location = HOCHIMINH_COORDINATES.copy()  # Start location set to HoChiMinh


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60)) # Increment time by a random interval
    return start_time

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()  
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'
    }

def generate_gps_data(device_id, timestamp, vehicle_type = 'private'):
    return {
        'id': uuid.uuid4(), 
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),
        'direction': 'North-East',
        'vehicleType': vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
    }

def generate_weather_data(device_id, timestamp, location):
    return  {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100),
        'airQualityIndex': random.uniform(0, 500)
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
    }


# Custom JSON serializer for UUIDs
def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj) 
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

# Callback function for message delivery report
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def producer_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()    # Ensure all messages are sent

def simulate_vehicle_movement():
    global start_location
    
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    

    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    
    return start_location


    
def simulate_journey(producer, device_id):
    while True:
        # Generate various types of data for the vehicle journey
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],  vehicle_data['location'], 'Camera123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],  vehicle_data['location'])
        
        # Check if the vehicle has reached HaNoi to end the simulation
        if(vehicle_data['location'][0] >= HANOI_COORDINATES['latitude'] 
            and vehicle_data['location'][1] <= HANOI_COORDINATES['longitude']):
            print('Vehicle has reached HaNoi. Simulation ending...')
            break
        
        # Send generated data to respective Kafka topics
        producer_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        producer_data_to_kafka(producer, GPS_TOPIC, gps_data)
        producer_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        producer_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        producer_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        
        
        



if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)
    
    try:
        # Start simulating the vehicle journey
        simulate_journey(producer, 'Journey-from-HoChiMinh-to-HaNoi')

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected Error occurred: {e}')


