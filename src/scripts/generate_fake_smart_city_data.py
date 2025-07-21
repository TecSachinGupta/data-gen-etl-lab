"""
Smart City IoT Analytics Platform - Data Generators
Comprehensive data simulation for Azure Data Engineering services
"""

import json
import random
import datetime
import time
import uuid
from typing import Dict, List, Any
from dataclasses import dataclass
import pandas as pd
from azure.eventhub import EventHubProducerClient, EventData
from azure.iot.device import IoTHubDeviceClient, Message
import asyncio
import threading

# Configuration
CITY_BOUNDS = {
    'lat_min': 40.7000,
    'lat_max': 40.7800,
    'lon_min': -74.0200,
    'lon_max': -73.9500
}

NYC_NEIGHBORHOODS = [
    "Manhattan", "Brooklyn", "Queens", "Bronx", "Staten Island",
    "Midtown", "Downtown", "Upper East Side", "Upper West Side", 
    "Financial District", "Times Square", "Central Park"
]

EMERGENCY_TYPES = [
    "traffic_accident", "fire", "medical_emergency", "crime",
    "infrastructure_failure", "weather_incident", "public_disturbance"
]

FEEDBACK_CATEGORIES = [
    "transportation", "public_safety", "infrastructure", "environment",
    "public_services", "housing", "recreation", "education"
]

# ==================== IoT SENSOR DATA GENERATOR ====================

@dataclass
class IoTSensor:
    sensor_id: str
    sensor_type: str
    location: Dict[str, float]
    battery_level: float = 100.0
    last_maintenance: datetime.datetime = None
    
    def __post_init__(self):
        if self.last_maintenance is None:
            self.last_maintenance = datetime.datetime.now() - datetime.timedelta(days=random.randint(1, 90))

class IoTDataGenerator:
    def __init__(self, num_sensors: int = 1000):
        self.sensors = self._create_sensors(num_sensors)
        self.time_factor = 1.0  # Speed up simulation
        
    def _create_sensors(self, num_sensors: int) -> List[IoTSensor]:
        sensors = []
        sensor_types = ["traffic", "air_quality", "noise", "parking", "weather", "pedestrian"]
        
        for i in range(num_sensors):
            sensor_id = f"{random.choice(sensor_types)}_{i:04d}"
            location = {
                "lat": random.uniform(CITY_BOUNDS['lat_min'], CITY_BOUNDS['lat_max']),
                "lon": random.uniform(CITY_BOUNDS['lon_min'], CITY_BOUNDS['lon_max'])
            }
            
            sensor = IoTSensor(
                sensor_id=sensor_id,
                sensor_type=random.choice(sensor_types),
                location=location,
                battery_level=random.uniform(20, 100)
            )
            sensors.append(sensor)
            
        return sensors
    
    def generate_traffic_data(self, sensor: IoTSensor) -> Dict[str, Any]:
        """Generate realistic traffic sensor data"""
        hour = datetime.datetime.now().hour
        
        # Rush hour patterns
        if 7 <= hour <= 9 or 17 <= hour <= 19:
            base_count = random.randint(80, 150)
            base_speed = random.uniform(15, 35)
        elif 22 <= hour or hour <= 5:
            base_count = random.randint(5, 25)
            base_speed = random.uniform(45, 60)
        else:
            base_count = random.randint(30, 70)
            base_speed = random.uniform(35, 50)
            
        # Add some randomness and anomalies
        anomaly_factor = 1.0
        if random.random() < 0.05:  # 5% chance of anomaly
            anomaly_factor = random.uniform(0.3, 3.0)
            
        return {
            "sensorId": sensor.sensor_id,
            "sensorType": "traffic",
            "timestamp": datetime.datetime.now().isoformat(),
            "location": sensor.location,
            "vehicleCount": max(0, int(base_count * anomaly_factor)),
            "avgSpeed": max(5, base_speed * anomaly_factor),
            "occupancy": min(100, random.uniform(0, 100) * anomaly_factor),
            "batteryLevel": sensor.battery_level,
            "status": "active" if sensor.battery_level > 10 else "low_battery"
        }
    
    def generate_air_quality_data(self, sensor: IoTSensor) -> Dict[str, Any]:
        """Generate realistic air quality sensor data"""
        # Simulate daily patterns and weather effects
        hour = datetime.datetime.now().hour
        base_aqi = 50 + (hour - 12) ** 2 * 0.5  # Worse during midday
        
        # Add weather and traffic correlation
        weather_factor = random.uniform(0.8, 1.3)
        traffic_factor = random.uniform(0.9, 1.4)
        
        return {
            "sensorId": sensor.sensor_id,
            "sensorType": "air_quality",
            "timestamp": datetime.datetime.now().isoformat(),
            "location": sensor.location,
            "aqi": max(0, min(500, int(base_aqi * weather_factor * traffic_factor))),
            "pm25": random.uniform(5, 35),
            "pm10": random.uniform(10, 50),
            "ozone": random.uniform(20, 120),
            "no2": random.uniform(10, 100),
            "co": random.uniform(0.1, 2.0),
            "temperature": random.uniform(15, 35),
            "humidity": random.uniform(30, 80),
            "batteryLevel": sensor.battery_level,
            "status": "active" if sensor.battery_level > 10 else "low_battery"
        }
    
    def generate_noise_data(self, sensor: IoTSensor) -> Dict[str, Any]:
        """Generate realistic noise sensor data"""
        hour = datetime.datetime.now().hour
        
        # Base noise levels by time of day
        if 6 <= hour <= 22:
            base_noise = random.uniform(55, 75)
        else:
            base_noise = random.uniform(35, 55)
            
        # Add event-based spikes
        if random.random() < 0.1:  # 10% chance of noise event
            base_noise += random.uniform(10, 30)
            
        return {
            "sensorId": sensor.sensor_id,
            "sensorType": "noise",
            "timestamp": datetime.datetime.now().isoformat(),
            "location": sensor.location,
            "decibelLevel": min(120, base_noise),
            "frequency": random.uniform(100, 8000),
            "duration": random.uniform(1, 300),
            "eventType": random.choice(["traffic", "construction", "emergency", "normal"]),
            "batteryLevel": sensor.battery_level,
            "status": "active" if sensor.battery_level > 10 else "low_battery"
        }
    
    def generate_parking_data(self, sensor: IoTSensor) -> Dict[str, Any]:
        """Generate realistic parking sensor data"""
        hour = datetime.datetime.now().hour
        
        # Business hours have higher occupancy
        if 9 <= hour <= 17:
            occupancy_rate = random.uniform(0.7, 0.95)
        elif 18 <= hour <= 22:
            occupancy_rate = random.uniform(0.5, 0.8)
        else:
            occupancy_rate = random.uniform(0.2, 0.6)
            
        total_spots = random.randint(20, 200)
        occupied_spots = int(total_spots * occupancy_rate)
        
        return {
            "sensorId": sensor.sensor_id,
            "sensorType": "parking",
            "timestamp": datetime.datetime.now().isoformat(),
            "location": sensor.location,
            "totalSpots": total_spots,
            "occupiedSpots": occupied_spots,
            "availableSpots": total_spots - occupied_spots,
            "occupancyRate": occupancy_rate,
            "averageStayDuration": random.uniform(30, 180),  # minutes
            "batteryLevel": sensor.battery_level,
            "status": "active" if sensor.battery_level > 10 else "low_battery"
        }
    
    def generate_sensor_data(self, sensor: IoTSensor) -> Dict[str, Any]:
        """Generate data based on sensor type"""
        generators = {
            "traffic": self.generate_traffic_data,
            "air_quality": self.generate_air_quality_data,
            "noise": self.generate_noise_data,
            "parking": self.generate_parking_data,
            "weather": self.generate_air_quality_data,  # Reuse for weather
            "pedestrian": self.generate_traffic_data  # Reuse for pedestrian counting
        }
        
        generator = generators.get(sensor.sensor_type, self.generate_traffic_data)
        data = generator(sensor)
        
        # Simulate battery drain
        sensor.battery_level -= random.uniform(0.001, 0.01)
        sensor.battery_level = max(0, sensor.battery_level)
        
        return data

# ==================== EMERGENCY SERVICES DATA GENERATOR ====================

class EmergencyDataGenerator:
    def __init__(self):
        self.incident_counter = 0
        
    def generate_emergency_incident(self) -> Dict[str, Any]:
        """Generate realistic emergency incident data"""
        self.incident_counter += 1
        
        incident_type = random.choice(EMERGENCY_TYPES)
        priority = random.choices(
            ["low", "medium", "high", "critical"],
            weights=[30, 40, 25, 5]
        )[0]
        
        # Response time varies by priority and type
        response_time_base = {
            "critical": random.uniform(3, 8),
            "high": random.uniform(5, 15),
            "medium": random.uniform(8, 25),
            "low": random.uniform(15, 45)
        }
        
        return {
            "incidentId": f"emg_{self.incident_counter:06d}",
            "type": incident_type,
            "priority": priority,
            "location": {
                "lat": random.uniform(CITY_BOUNDS['lat_min'], CITY_BOUNDS['lat_max']),
                "lon": random.uniform(CITY_BOUNDS['lon_min'], CITY_BOUNDS['lon_max'])
            },
            "address": f"{random.randint(1, 999)} {random.choice(['Main St', 'Broadway', 'Park Ave', 'Wall St', '5th Ave'])}",
            "neighborhood": random.choice(NYC_NEIGHBORHOODS),
            "timestamp": datetime.datetime.now().isoformat(),
            "reportedBy": random.choice(["citizen", "sensor", "patrol", "dispatch"]),
            "responseTime": response_time_base[priority],
            "unitsDispatched": random.randint(1, 5),
            "severity": random.randint(1, 10),
            "status": random.choice(["reported", "dispatched", "in_progress", "resolved"]),
            "description": f"Emergency incident of type {incident_type} in {random.choice(NYC_NEIGHBORHOODS)}"
        }

# ==================== CITIZEN FEEDBACK DATA GENERATOR ====================

class CitizenFeedbackGenerator:
    def __init__(self):
        self.feedback_counter = 0
        
    def generate_citizen_feedback(self) -> Dict[str, Any]:
        """Generate realistic citizen feedback data"""
        self.feedback_counter += 1
        
        category = random.choice(FEEDBACK_CATEGORIES)
        sentiment = random.choices(
            ["positive", "neutral", "negative"],
            weights=[25, 35, 40]
        )[0]
        
        # Sample feedback texts based on category and sentiment
        feedback_templates = {
            "transportation": {
                "positive": ["Great improvement in bus service", "New bike lanes are excellent"],
                "neutral": ["Bus frequency is adequate", "Traffic is manageable"],
                "negative": ["Traffic lights not working", "Subway delays are frequent"]
            },
            "public_safety": {
                "positive": ["Police response was quick", "Feel safe in this area"],
                "neutral": ["Average police presence", "Security cameras are visible"],
                "negative": ["Poor lighting at night", "Crime rate increasing"]
            },
            "infrastructure": {
                "positive": ["Road repairs completed well", "New park is beautiful"],
                "neutral": ["Infrastructure is adequate", "Maintenance is ongoing"],
                "negative": ["Potholes need fixing", "Bridge needs repair"]
            }
        }
        
        templates = feedback_templates.get(category, feedback_templates["transportation"])
        feedback_text = random.choice(templates.get(sentiment, templates["neutral"]))
        
        return {
            "feedbackId": f"fb_{self.feedback_counter:06d}",
            "category": category,
            "sentiment": sentiment,
            "text": feedback_text,
            "rating": random.randint(1, 5),
            "location": {
                "lat": random.uniform(CITY_BOUNDS['lat_min'], CITY_BOUNDS['lat_max']),
                "lon": random.uniform(CITY_BOUNDS['lon_min'], CITY_BOUNDS['lon_max'])
            },
            "neighborhood": random.choice(NYC_NEIGHBORHOODS),
            "timestamp": datetime.datetime.now().isoformat(),
            "userId": f"user_{random.randint(1, 10000):05d}",
            "channel": random.choice(["mobile_app", "web_portal", "phone", "email"]),
            "status": random.choice(["new", "under_review", "in_progress", "resolved"]),
            "priority": random.choice(["low", "medium", "high"]),
            "tags": random.sample(["urgent", "recurring", "infrastructure", "safety"], random.randint(1, 3))
        }

# ==================== WEATHER DATA GENERATOR ====================

class WeatherDataGenerator:
    def generate_weather_data(self) -> Dict[str, Any]:
        """Generate realistic weather data"""
        # Seasonal variations
        month = datetime.datetime.now().month
        
        if month in [12, 1, 2]:  # Winter
            temp_base = random.uniform(-5, 10)
            humidity_base = random.uniform(40, 70)
        elif month in [3, 4, 5]:  # Spring
            temp_base = random.uniform(10, 25)
            humidity_base = random.uniform(50, 80)
        elif month in [6, 7, 8]:  # Summer
            temp_base = random.uniform(20, 35)
            humidity_base = random.uniform(60, 90)
        else:  # Fall
            temp_base = random.uniform(5, 20)
            humidity_base = random.uniform(45, 75)
            
        weather_conditions = ["clear", "cloudy", "rainy", "snowy", "foggy"]
        condition = random.choice(weather_conditions)
        
        return {
            "timestamp": datetime.datetime.now().isoformat(),
            "location": {
                "lat": random.uniform(CITY_BOUNDS['lat_min'], CITY_BOUNDS['lat_max']),
                "lon": random.uniform(CITY_BOUNDS['lon_min'], CITY_BOUNDS['lon_max'])
            },
            "temperature": temp_base,
            "humidity": humidity_base,
            "pressure": random.uniform(980, 1030),
            "windSpeed": random.uniform(0, 25),
            "windDirection": random.randint(0, 360),
            "precipitation": random.uniform(0, 50) if condition == "rainy" else 0,
            "visibility": random.uniform(1, 10),
            "condition": condition,
            "uvIndex": random.randint(0, 11),
            "airPressure": random.uniform(29.5, 30.5)
        }

# ==================== TRAFFIC PATTERN DATA GENERATOR ====================

class TrafficPatternGenerator:
    def generate_traffic_pattern(self) -> Dict[str, Any]:
        """Generate realistic traffic pattern data"""
        hour = datetime.datetime.now().hour
        day_of_week = datetime.datetime.now().weekday()
        
        # Rush hour patterns
        if day_of_week < 5:  # Weekday
            if 7 <= hour <= 9:
                congestion_level = random.uniform(0.7, 0.95)
            elif 17 <= hour <= 19:
                congestion_level = random.uniform(0.6, 0.9)
            else:
                congestion_level = random.uniform(0.2, 0.6)
        else:  # Weekend
            congestion_level = random.uniform(0.3, 0.7)
            
        return {
            "timestamp": datetime.datetime.now().isoformat(),
            "location": {
                "lat": random.uniform(CITY_BOUNDS['lat_min'], CITY_BOUNDS['lat_max']),
                "lon": random.uniform(CITY_BOUNDS['lon_min'], CITY_BOUNDS['lon_max'])
            },
            "roadSegmentId": f"road_{random.randint(1, 1000):04d}",
            "congestionLevel": congestion_level,
            "averageSpeed": random.uniform(10, 60) * (1 - congestion_level),
            "vehicleCount": random.randint(50, 500),
            "estimatedTravelTime": random.uniform(300, 1800),
            "accidents": random.randint(0, 3),
            "constructionZones": random.randint(0, 2),
            "roadCondition": random.choice(["good", "fair", "poor"]),
            "weatherImpact": random.uniform(0, 1)
        }

# ==================== MAIN DATA SIMULATOR ====================

class SmartCityDataSimulator:
    def __init__(self):
        self.iot_generator = IoTDataGenerator(num_sensors=100)
        self.emergency_generator = EmergencyDataGenerator()
        self.feedback_generator = CitizenFeedbackGenerator()
        self.weather_generator = WeatherDataGenerator()
        self.traffic_generator = TrafficPatternGenerator()
        
    def generate_batch_data(self, num_records: int = 1000) -> Dict[str, List[Dict]]:
        """Generate batch data for initial load"""
        batch_data = {
            "iot_sensors": [],
            "emergency_incidents": [],
            "citizen_feedback": [],
            "weather_data": [],
            "traffic_patterns": []
        }
        
        for i in range(num_records):
            # Generate IoT sensor data
            sensor = random.choice(self.iot_generator.sensors)
            batch_data["iot_sensors"].append(self.iot_generator.generate_sensor_data(sensor))
            
            # Generate emergency incidents (less frequent)
            if i % 50 == 0:
                batch_data["emergency_incidents"].append(self.emergency_generator.generate_emergency_incident())
            
            # Generate citizen feedback (moderate frequency)
            if i % 10 == 0:
                batch_data["citizen_feedback"].append(self.feedback_generator.generate_citizen_feedback())
            
            # Generate weather data (every 100 records)
            if i % 100 == 0:
                batch_data["weather_data"].append(self.weather_generator.generate_weather_data())
            
            # Generate traffic patterns (every 20 records)
            if i % 20 == 0:
                batch_data["traffic_patterns"].append(self.traffic_generator.generate_traffic_pattern())
                
        return batch_data
    
    def save_batch_data_to_files(self, batch_data: Dict[str, List[Dict]], output_dir: str = "data"):
        """Save batch data to JSON and CSV files"""
        import os
        
        os.makedirs(output_dir, exist_ok=True)
        
        for data_type, records in batch_data.items():
            if records:
                # Save as JSON
                json_path = os.path.join(output_dir, f"{data_type}.json")
                with open(json_path, 'w') as f:
                    json.dump(records, f, indent=2)
                
                # Save as CSV
                csv_path = os.path.join(output_dir, f"{data_type}.csv")
                df = pd.json_normalize(records)
                df.to_csv(csv_path, index=False)
                
                print(f"Saved {len(records)} {data_type} records to {json_path} and {csv_path}")
    
    def simulate_real_time_stream(self, duration_minutes: int = 60, events_per_minute: int = 100):
        """Simulate real-time data stream"""
        print(f"Starting real-time simulation for {duration_minutes} minutes...")
        
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        while time.time() < end_time:
            # Generate multiple events per second
            for _ in range(events_per_minute // 60):
                event_type = random.choices(
                    ["iot", "emergency", "feedback", "weather", "traffic"],
                    weights=[70, 5, 15, 2, 8]
                )[0]
                
                if event_type == "iot":
                    sensor = random.choice(self.iot_generator.sensors)
                    data = self.iot_generator.generate_sensor_data(sensor)
                elif event_type == "emergency":
                    data = self.emergency_generator.generate_emergency_incident()
                elif event_type == "feedback":
                    data = self.feedback_generator.generate_citizen_feedback()
                elif event_type == "weather":
                    data = self.weather_generator.generate_weather_data()
                else:  # traffic
                    data = self.traffic_generator.generate_traffic_pattern()
                
                # In real implementation, send to Azure Event Hub or IoT Hub
                print(f"[{event_type.upper()}] {json.dumps(data, indent=2)}")
                
            time.sleep(1)  # Wait 1 second
        
        print("Real-time simulation completed.")

# ==================== USAGE EXAMPLE ====================

if __name__ == "__main__":
    # Create simulator instance
    simulator = SmartCityDataSimulator()
    
    # Generate batch data
    print("Generating batch data...")
    batch_data = simulator.generate_batch_data(num_records=1000)
    
    # Save to files
    simulator.save_batch_data_to_files(batch_data)
    
    # Simulate real-time stream (uncomment to run)
    # simulator.simulate_real_time_stream(duration_minutes=5, events_per_minute=60)
    
    print("Data generation completed!")
    print("\nGenerated data types:")
    for data_type, records in batch_data.items():
        print(f"  - {data_type}: {len(records)} records")
