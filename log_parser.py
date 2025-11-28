import pandas as pd
import polars as pl
from datetime import datetime, timedelta
import re
from typing import Dict, List, Any
import easygui
import logging
import os

def parse_log_file(file_path: str = None, timestamp_offset: timedelta = timedelta(hours=5, minutes=30)) -> tuple[Dict[str, pd.DataFrame], str]:
    """
    Parse the log file and return a dictionary of DataFrames for each log type.
    If file_path is None, opens a file dialog to select the log file.
    
    Args:
        file_path: Path to the log file. If None, opens file dialog.
        timestamp_offset: Timedelta to add to all timestamps. Default is +5:30 (IST).

    Returns:
        tuple: (dataframes_dict, filename)
    """
    # Store the file path
    selected_file_path = file_path

    # Use easygui to select file if path not provided
    if file_path is None:
        file_path = easygui.fileopenbox(
            msg="Select the log file to parse",
            title="Log File Selection",
            filetypes=["*.log", "*.txt", "*.*"]
        )
        if file_path is None:
            print("No file selected. Exiting.")
            return {}, ""
        selected_file_path = file_path  # Store the actual file path used

    # Store different log types
    log_data = {
        'MISSION_INFO': [],
        'MISSION_STATE_CHANGED': [],
        'RC_CHANNELS': [],
        'SERIAL_TCP_CON': [],
        'CC_PARAMETER': [],
        'CC_PARAMETER_SHELVE': [],
        'CC_PARAMETER_TINY': [],
        'AP_PARAMETER': [],
        'AP_PARAMETER_TINY': [],
        'MAVLINK_INFO': [],
        'GA_SET_PARAM': [],
        'GA_GET_PARAM': [],
        'GA_PARAM': [],
        'MAX_SPEED_ESTI': [],
        'MAVLINK_ACTIVE_PORT': [],
        'CPU': [],
        'VERSION': [],
        'BOUNDARY_INTR': [],
        'VEHICLE_COMMAND': [],
        'GUIDED_MISSION': [],
        'RESUME_MISSION': [],
        'RESUME_STATE': [],
        'CC_PARAMETER_PERF': [],
        'CC_PARAMETER_DB_PERF': [],
        'SnA_RECEIVING_DATA': [],
        'SnA_LOGGING': [],
        'SnA_INFO': [],
        'SPRAY_INFO': [],
        'SCHEDULERTASK': [],
        'FLOWMETER': [],
        'FLOWMETER_INFO': [],
        'PUMP': [],
        'NOZZLE': [],
        'ERROR': [],
        'OTHER': []
    }
    
    # Set up logging for unknown patterns
    unknown_patterns_logger = logging.getLogger('unknown_patterns')
    unknown_patterns_logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    unknown_patterns_logger.addHandler(handler)

    def parse_timestamp(ts_str: str, offset: timedelta) -> datetime:
        """Parse timestamp string, apply offset, and return datetime with 3 decimal places precision"""
        try:
            base_timestamp = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S.%f')
        except:
            try:
                base_timestamp = datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
            except:
                base_timestamp = datetime.now()  # Fallback
        
        # Apply the timestamp offset
        adjusted_timestamp = base_timestamp + offset       
        return adjusted_timestamp

    # Also add logging to other parsers for consistency
    def safe_clean_value(value: str, target_type: str = 'auto', convert_bool_to_numeric: bool = False) -> Any:
        """Safely clean and convert values with type specification - enhanced with logging."""
        if value is None or value == '':
            return None
        value = str(value).strip()
        if value == '' or value == 'None' or value == 'None,':
            return None
        
        if target_type == 'string':
            return value
        elif target_type == 'float':
            try:
                return float(value)
            except ValueError:
                unknown_patterns_logger.warning(f"Failed to convert '{value}' to float")
                return None
        elif target_type == 'int':
            try:
                return int(value)
            except ValueError:
                unknown_patterns_logger.warning(f"Failed to convert '{value}' to int")
                return None
        elif target_type == 'bool_numeric':
            if value.lower() == 'true':
                return 1
            elif value.lower() == 'false':
                return 0
            else:
                unknown_patterns_logger.warning(f"Unexpected boolean value: '{value}'")
                return None
        else:  # auto
            if value in ['True', 'False']:
                if convert_bool_to_numeric:
                    return 1 if value == 'True' else 0
                else:
                    return value == 'True'
            try:
                if '.' in value:
                    return float(value)
                else:
                    return int(value)
            except ValueError:
                return value
    
    def convert_boolean_to_numeric(value: str) -> Any:
        """Convert True/False to 1/0, keep None as None."""
        if value is None or value == '':
            return None
        value = str(value).strip()
        if value.lower() == 'true':
            return 1
        elif value.lower() == 'false':
            return 0
        else:
            return None
    
    def parse_list_values(value_str: str) -> List[float]:
        """Parse comma-separated values or bracket notation like [1,2,3] into a list of floats."""
        if not value_str or value_str.strip() == '':
            return []
        
        # Remove brackets if present
        cleaned = value_str.strip()
        if cleaned.startswith('[') and cleaned.endswith(']'):
            cleaned = cleaned[1:-1]
        
        # Split by comma and parse
        parts = [p.strip() for p in cleaned.split(',') if p.strip()]
        values = []
        for part in parts:
            try:
                values.append(float(part))
            except:
                try:
                    values.append(int(part))
                except:
                    values.append(part)  # Keep as string if can't convert
        return values
    
    def parse_cpu_info(data_part: str) -> Dict[str, Any]:
        """Parse CPU info line and extract numeric values with descriptive headers."""
        result = {}
        
        # CPU usage percentage
        cpu_match = re.search(r'CPU usage: ([\d.]+)%', data_part)
        if cpu_match:
            result['cpu_usage_percent'] = float(cpu_match.group(1))
        
        # RAM usage in MB
        ram_match = re.search(r'RAM usage: (\d+) MB', data_part)
        if ram_match:
            result['ram_usage_mb'] = int(ram_match.group(1))
        
        # Load averages
        load_match = re.search(r'Load : \(([\d., ]+)\)', data_part)
        if load_match:
            loads = [float(x.strip()) for x in load_match.group(1).split(',')]
            result['load_avg_1min'] = loads[0] if len(loads) > 0 else None
            result['load_avg_5min'] = loads[1] if len(loads) > 1 else None
            result['load_avg_15min'] = loads[2] if len(loads) > 2 else None
        
        # Temperature
        temp_match = re.search(r'Temp: ([\d.]+)°C', data_part)
        if temp_match:
            result['temp_celsius'] = float(temp_match.group(1))
        elif 'Temp: N/A' in data_part:
            result['temp_celsius'] = None
        else:
            # If no Temp field found at all (backward compatibility), set to None
            result['temp_celsius'] = None
        
        return result
    
    print(f"Parsing log file: {file_path}")
    
    def is_error_message(log_content: str) -> bool:
        """Check if log content contains error/failure related keywords."""
        error_keywords = [
            'error', 'warning', 'exception', 'failed', 'failure', 'fault', 
            'critical', 'timeout', 'crash', 'abort', 'denied', 'refuse', 
            'invalid', 'corrupt', 'missing', 'not found', 'unable', 
            'disconnect', 'lost', 'broken', 'malfunction', 'alert',
            'emergency', 'panic', 'fatal', 'severe', 'bad', 'wrong'
        ]
        
        log_lower = log_content.lower()
        return any(keyword in log_lower for keyword in error_keywords)

    def append_to_error_log(timestamp, log_content):
        """Helper function to append errors to ERROR log with automatic header initialization."""
        if len(log_data['ERROR']) == 0:
            log_data['ERROR'].append({
                'timestamp': None, 
                'log_content': '::  This is a list of ERROR/FAILURE items extracted from logs  ::'
            })
            log_data['ERROR'].append({
                'timestamp': None, 
                'log_content': '---------------------------------------------------------------------------------'
            })
        
        log_data['ERROR'].append({
            'timestamp': timestamp,
            'log_content': log_content
        })

    with open(file_path, 'r', encoding='utf-8') as file:
        for line_num, line in enumerate(file, 1):
            line = line.strip()
            if not line:
                continue
                
            # Extract timestamp
            timestamp_match = re.match(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})', line)
            if not timestamp_match:
                continue
                
            timestamp = parse_timestamp(timestamp_match.group(1), timestamp_offset)
            
            # Extract the rest of the line after timestamp
            rest_of_line = line[timestamp_match.end():].strip()
            
            # Split by comma and find the log content after INFO/WARNING/CRITICAL
            comma_parts = rest_of_line.split(',')
            
            log_content = ""
            info_found = False
            severity_level = None
            added_to_error = False

            for i, part in enumerate(comma_parts):
                if any(level in part.strip() for level in ['INFO', 'WARNING', 'CRITICAL']):
                    # Capture the severity level
                    for level in ['WARNING', 'CRITICAL', 'INFO']:
                        if level in part.strip():
                            severity_level = level
                            break
                    
                    # Join all parts after this one
                    if i + 1 < len(comma_parts):
                        log_content = ','.join(comma_parts[i + 1:]).strip()
                    info_found = True
                    break

            if not info_found or not log_content:
                continue

            # Add WARNING and CRITICAL messages to ERROR log
            if severity_level in ['WARNING', 'CRITICAL']:
                append_to_error_log(timestamp, log_content)
                added_to_error = True
            
            try:
                # MISSION_INFO parsing
                if log_content.startswith('MISSION_INFO'):
                    if 'mode, armed, flying' in log_content:  # Skip header line
                        continue
                    data_parts = log_content.split(',')[1:]  # Skip 'MISSION_INFO'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 9:
                        # Extract original values
                        mode_str = safe_clean_value(data_parts[0], 'string') if len(data_parts) > 0 else None
                        armed_str = safe_clean_value(data_parts[1], 'string') if len(data_parts) > 1 else None
                        flying_str = safe_clean_value(data_parts[2], 'string') if len(data_parts) > 2 else None
                        
                        # Map mode string to enum value
                        mode_mapping = {
                            'STABILIZE': 0,
                            'ACRO': 1,
                            'ALT_HOLD': 2,
                            'AUTO': 3,
                            'GUIDED': 4,
                            'LOITER': 5,
                            'RTL': 6,
                            'CIRCLE': 7,
                            'LAND': 9,
                            'DRIFT': 11,
                            'SPORT': 13,
                            'FLIP': 14,
                            'AUTOTUNE': 15,
                            'POSHOLD': 16,
                            'BRAKE': 17,
                            'THROW': 18,
                            'AVOID_ADSB': 19,
                            'GUIDED_NOGPS': 20,
                            'SMART_RTL': 21,
                            'FLOWHOLD': 22,
                            'FOLLOW': 23,
                            'ZIGZAG': 24,
                            'SYSTEMID': 25,
                            'AUTOROTATE': 26
                        }
                        mode_val = mode_mapping.get(mode_str.upper() if mode_str else None, None)
                        if mode_val is not None:  # Use 'is not None' instead of truthiness
                            mode_val = int(mode_val)
                        
                        # Map armed string to numeric value
                        armed_val = None
                        if armed_str:
                            if armed_str.lower() == 'armed':
                                armed_val = 1
                            elif armed_str.lower() == 'disarmed':
                                armed_val = 0
                        
                        # Map flying string to numeric value
                        flying_val = None
                        if flying_str:
                            if flying_str.lower() == 'flying':
                                flying_val = 1
                            elif flying_str.lower() == 'not-flying':
                                flying_val = 0
                        
                        record = {
                            'timestamp': timestamp,
                            'flight_mode': mode_str,
                            'flight_mode_val': mode_val,
                            'armed': armed_str,
                            'armed_val': armed_val,
                            'flying': flying_str,
                            'flying_val': flying_val,
                            'height_m': safe_clean_value(data_parts[3], 'float') if len(data_parts) > 3 else None,
                            'speed_ms': safe_clean_value(data_parts[4], 'float') if len(data_parts) > 4 else None,
                            'climb_rate_ms': safe_clean_value(data_parts[5], 'float') if len(data_parts) > 5 else None,
                            'heading_deg': safe_clean_value(data_parts[6], 'float') if len(data_parts) > 6 else None,
                            'lat_deg': safe_clean_value(data_parts[7], 'float') if len(data_parts) > 7 else None,
                            'lon_deg': safe_clean_value(data_parts[8], 'float') if len(data_parts) > 8 else None
                        }
                        log_data['MISSION_INFO'].append(record)
                
                # RC_CHANNELS parsing
                elif log_content.startswith('RC_CHANNELS'):
                    if 'rc1, rc2, rc3' in log_content:  # Skip header line
                        continue
                    data_parts = log_content.split(',')[1:]  # Skip 'RC_CHANNELS'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 9:
                        # Handle special format like c6=1101
                        cleaned_parts = []
                        for part in data_parts[:10]:  # Take first 10 parts
                            if '=' in part:
                                cleaned_parts.append(part.split('=')[1])
                            else:
                                cleaned_parts.append(part)
                        
                        record = {
                            'timestamp': timestamp,
                            'rc1': safe_clean_value(cleaned_parts[0], 'int') if len(cleaned_parts) > 0 else None,
                            'rc2': safe_clean_value(cleaned_parts[1], 'int') if len(cleaned_parts) > 1 else None,
                            'rc3': safe_clean_value(cleaned_parts[2], 'int') if len(cleaned_parts) > 2 else None,
                            'rc4': safe_clean_value(cleaned_parts[3], 'int') if len(cleaned_parts) > 3 else None,
                            'rc5': safe_clean_value(cleaned_parts[4], 'int') if len(cleaned_parts) > 4 else None,
                            'rc6': safe_clean_value(cleaned_parts[5], 'int') if len(cleaned_parts) > 5 else None,
                            'rc7': safe_clean_value(cleaned_parts[6], 'int') if len(cleaned_parts) > 6 else None,
                            'rc8': safe_clean_value(cleaned_parts[7], 'int') if len(cleaned_parts) > 7 else None,
                            'rc9': safe_clean_value(cleaned_parts[8], 'int') if len(cleaned_parts) > 8 else None,
                            'rc10': safe_clean_value(cleaned_parts[9], 'int') if len(cleaned_parts) > 9 else None
                        }
                        log_data['RC_CHANNELS'].append(record)
                
                # RESUME_STATE parsing
                elif log_content.startswith('RESUME_STATE'):
                    if 'lat, lon, height' in log_content:  # Skip header line
                        continue
                    data_parts = log_content.split(',')[1:]  # Skip 'RESUME_STATE'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 6:
                        record = {
                            'timestamp': timestamp,
                            'lat': safe_clean_value(data_parts[0], 'float'),
                            'lon': safe_clean_value(data_parts[1], 'float'),
                            'height': safe_clean_value(data_parts[2], 'float'),
                            'yaw': safe_clean_value(data_parts[3], 'float'),
                            'wp': safe_clean_value(data_parts[4], 'int'),
                            'spray': safe_clean_value(data_parts[5], 'int')
                        }
                        log_data['RESUME_STATE'].append(record)
                
                # SERIAL_TCP_CON parsing
                elif log_content.startswith('SERIAL_TCP_CON'):
                    if 'serial_recv_bytes' in log_content or 'Waiting for connection' in log_content or 'Connecting with tcp' in log_content:
                        continue  # Skip header lines and status messages
                    data_parts = log_content.split(',')[1:]  # Skip 'SERIAL_TCP_CON'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2:
                        record = {
                            'timestamp': timestamp,
                            'serial_recv_bytes': safe_clean_value(data_parts[0], 'int'),
                            'serial_send_bytes': safe_clean_value(data_parts[1], 'int')
                        }
                        log_data['SERIAL_TCP_CON'].append(record)

                # VEHICLE_COMMAND parsing
                elif log_content.startswith('VEHICLE_COMMAND'):
                    # Skip lines that are just "VEHICLE_COMMAND," without additional content
                    if log_content.strip() == 'VEHICLE_COMMAND' or log_content.strip() == 'VEHICLE_COMMAND,':
                        continue
                    
                    # Extract message after "VEHICLE_COMMAND, "
                    if log_content.startswith('VEHICLE_COMMAND, '):
                        message = log_content[len('VEHICLE_COMMAND, '):].strip()
                    elif log_content.startswith('VEHICLE_COMMAND,'):
                        message = log_content[len('VEHICLE_COMMAND,'):].strip()
                    else:
                        message = log_content[len('VEHICLE_COMMAND'):].strip()
                    
                    # Skip empty messages
                    if not message:
                        continue
                    
                    # Default message type
                    message_type = ''
                    
                    # Check if this is a status text message
                    if message.startswith('Sent statustext: '):
                        # Extract severity level number
                        status_part = message[len('Sent statustext: '):]
                        if ':' in status_part:
                            severity_str = status_part.split(':')[0].strip()
                            try:
                                severity_level = int(severity_str)
                                # Map severity level to enum name
                                severity_map = {
                                    0: 'EMERGENCY',
                                    1: 'ALERT', 
                                    2: 'CRITICAL',
                                    3: 'ERROR',
                                    4: 'WARNING',
                                    5: 'NOTICE',
                                    6: 'INFO',
                                    7: 'DEBUG',
                                    8: 'NONE'
                                }
                                severity_name = severity_map.get(severity_level, f'UNKNOWN_{severity_level}')
                                message_type = f'STATUS_TEXT_{severity_name}'
                                
                                # Strip the "Sent statustext: [severity]:" part from the message
                                colon_index = status_part.find(':')
                                if colon_index != -1:
                                    message = status_part[colon_index + 1:].strip()
                                
                            except ValueError:
                                message_type = 'STATUS_TEXT_UNKNOWN'
                    
                    record = {
                        'timestamp': timestamp,
                        'message_type': message_type,
                        'message': message
                    }
                    log_data['VEHICLE_COMMAND'].append(record)

                # SCHEDULERTASK parsing
                elif log_content.startswith('SCHEDULERTASK,'):
                    data_parts = log_content.split(',')[1:]  # Skip 'SCHEDULERTASK'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 3:
                        record = {
                            'timestamp': timestamp,
                            'task_name': data_parts[0].strip(),
                            'parameter': data_parts[1].strip(),
                            'task_id': safe_clean_value(data_parts[2], 'int')
                        }
                        log_data['SCHEDULERTASK'].append(record)
                
                # Key-value pair entries - KEEP True/False as strings
                elif log_content.startswith('CC_PARAMETER,'):
                    # Check for error patterns first
                    error_patterns = [
                        'Created missing shelve key during startup race:',
                        'Created missing TinyDB record during startup race:',
                        'not found in memory or shelve DB',
                        'not found in TinyDB memory or file'
                    ]
                    
                    is_error = any(pattern in log_content for pattern in error_patterns)
                    if is_error:
                        if not added_to_error:
                            append_to_error_log(timestamp, log_content)
                            added_to_error = True
                    
                    # Try to parse as key-value pair (for normal CC_PARAMETER entries)
                    data_parts = log_content.split(',', 2)[1:]  # Skip 'CC_PARAMETER'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2 and not is_error:
                        record = {
                            'timestamp': timestamp,
                            'key': data_parts[0].strip(),
                            'value': data_parts[1].strip()  # Keep as string - don't convert True/False
                        }
                        log_data['CC_PARAMETER'].append(record)
                
                elif log_content.startswith('CC_PARAMETER_SHELVE,'):
                    data_parts = log_content.split(',', 2)[1:]  # Skip 'CC_PARAMETER_SHELVE'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2:
                        record = {
                            'timestamp': timestamp,
                            'key': data_parts[0].strip(),
                            'value': data_parts[1].strip()  # Keep as string - don't convert True/False
                        }
                        log_data['CC_PARAMETER_SHELVE'].append(record)
                
                elif log_content.startswith('CC_PARAMETER_TINY,'):
                    data_parts = log_content.split(',', 2)[1:]  # Skip 'CC_PARAMETER_TINY'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2:
                        record = {
                            'timestamp': timestamp,
                            'key': data_parts[0].strip(),
                            'value': data_parts[1].strip()  # Keep as string - don't convert True/False
                        }
                        log_data['CC_PARAMETER_TINY'].append(record)
                
                elif log_content.startswith('AP_PARAMETER,'):
                    data_parts = log_content.split(',', 2)[1:]  # Skip 'AP_PARAMETER'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2:
                        record = {
                            'timestamp': timestamp,
                            'key': data_parts[0].strip(),
                            'value': data_parts[1].strip()  # Keep as string - don't convert True/False
                        }
                        log_data['AP_PARAMETER'].append(record)
                
                elif log_content.startswith('AP_PARAMETER_TINY,'):
                    # Handle special case: AP_PARAMETER_TINY, VERSION, ARDUPILOT, 4_1_1_v11
                    data_parts = log_content.split(',')[1:]  # Skip 'AP_PARAMETER_TINY'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2:
                        if len(data_parts) >= 3:
                            # For cases like VERSION, ARDUPILOT, 4_1_1_v11
                            key = f"{data_parts[0]}_{data_parts[1]}" if len(data_parts) > 2 else data_parts[0]
                            value = data_parts[2] if len(data_parts) > 2 else data_parts[1]
                        else:
                            key = data_parts[0]
                            value = data_parts[1]
                        record = {
                            'timestamp': timestamp,
                            'key': key.strip(),
                            'value': value.strip()  # Keep as string - don't convert True/False
                        }
                        log_data['AP_PARAMETER_TINY'].append(record)
                
                elif log_content.startswith('GA_SET_PARAM,'):
                    data_parts = log_content.split(',', 2)[1:]  # Skip 'GA_SET_PARAM'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2:
                        record = {
                            'timestamp': timestamp,
                            'key': data_parts[0].strip(),
                            'value': data_parts[1].strip()  # Keep as string - don't convert True/False
                        }
                        log_data['GA_SET_PARAM'].append(record)
                
                elif log_content.startswith('GA_GET_PARAM,'):
                    data_parts = log_content.split(',', 1)[1:]  # Skip 'GA_GET_PARAM'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 1:
                        record = {
                            'timestamp': timestamp,
                            'key': data_parts[0].strip(),
                            'value': None  # GET requests don't have values
                        }
                        log_data['GA_GET_PARAM'].append(record)
                
                elif log_content.startswith('GA_PARAM,'):
                    data_parts = log_content.split(',', 2)[1:]  # Skip 'GA_PARAM'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2:
                        record = {
                            'timestamp': timestamp,
                            'key': data_parts[0].strip(),
                            'value': data_parts[1].strip()  # Keep as string - don't convert True/False
                        }
                        log_data['GA_PARAM'].append(record)
                
                # MAVLINK_INFO parsing
                elif log_content.startswith('MAVLINK_INFO'):
                    if 'name, messages_count' in log_content:  # Skip header line
                        continue
                    data_parts = log_content.split(',')[1:]  # Skip 'MAVLINK_INFO'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 3:
                        record = {
                            'timestamp': timestamp,
                            'name': data_parts[0].strip(),
                            'messages_count': safe_clean_value(data_parts[1], 'int'),
                            'last_message': data_parts[2].strip(),
                            'status': data_parts[3].strip() if len(data_parts) > 3 else None
                        }
                        log_data['MAVLINK_INFO'].append(record)
                
                # MAX_SPEED_ESTI parsing
                elif 'MAX_SPEED_ESTI' in log_content and 'Calculated max speed' in log_content:
                    speed_match = re.search(r'Calculated max speed ([\d.]+)', log_content)
                    if speed_match:
                        record = {
                            'timestamp': timestamp,
                            'max_speed': float(speed_match.group(1))
                        }
                        log_data['MAX_SPEED_ESTI'].append(record)
                
                # MAVLINK_ACTIVE_PORT parsing
                elif 'MAVLINK ACTIVE_MAVLINK_PORT is :' in log_content:
                    port_match = re.search(r'MAVLINK ACTIVE_MAVLINK_PORT is : (\d+)', log_content)
                    if port_match:
                        record = {
                            'timestamp': timestamp,
                            'active_port': int(port_match.group(1))
                        }
                        log_data['MAVLINK_ACTIVE_PORT'].append(record)
                
                # CPU parsing
                elif log_content.startswith('CPU,'):
                    cpu_data = parse_cpu_info(log_content)
                    if cpu_data:
                        record = {'timestamp': timestamp}
                        record.update(cpu_data)
                        log_data['CPU'].append(record)

                # VERSION parsing - ONLY direct VERSION lines and SnAInfo lines
                elif log_content.startswith('VERSION,'):
                    # Handle direct VERSION lines like "VERSION, v4.0.9" or "VERSION, AP/FCS version : ..."
                    version_data = log_content[8:].strip()  # Remove "VERSION," prefix
                    
                    # Parse different VERSION formats
                    if version_data.startswith('v') and len(version_data.split()) == 1:
                        # Simple version like "v4.0.9"
                        record = {
                            'timestamp': timestamp,
                            'component': 'CC',
                            'version': version_data,
                            'component_type': 'Companion Computer',
                            'raw_data': log_content
                        }
                    elif 'AP/FCS version' in version_data:
                        # AP/FCS version line
                        # Pattern: "AP/FCS version : GA ArduCopter V4.1.1 v18a (b1df2355)"
                        # Extract: "4.1.1 v18a"
                        # try to extract everything after "GA ArduCopter V" and before "("
                        version_match = re.search(r'GA ArduCopter V([^(]+)', version_data)
                        if version_match:
                            version_str = version_match.group(1).strip()
                        else:
                            # Last resort: take everything after ":"
                            version_str = version_data.split(':')[1].strip() if ':' in version_data else version_data
                        record = {
                            'timestamp': timestamp,
                            'component': 'AP/FCS',
                            'version': version_str,
                            'component_type': 'Flight Controller',
                            'raw_data': log_content
                        }
                    elif 'GCS App version' in version_data:
                        # GCS App version line
                        version_str = version_data.split(':')[1].strip() if ':' in version_data else version_data
                        record = {
                            'timestamp': timestamp,
                            'component': 'GCS App',
                            'version': version_str,
                            'component_type': 'Ground Station',
                            'raw_data': log_content
                        }
                    else:
                        # Generic VERSION line
                        record = {
                            'timestamp': timestamp,
                            'component': 'Unknown',
                            'version': version_data,
                            'component_type': 'unknown',
                            'raw_data': log_content
                        }
                    
                    log_data['VERSION'].append(record)

                # SnA version parsing into VERSION
                elif log_content.startswith('SnAInfo,') and 'SnA version:' in log_content:
                    version_match = re.search(r'SnA version: (\w+)', log_content)
                    if version_match:
                        record = {
                            'timestamp': timestamp,
                            'component': 'SnA',
                            'version': version_match.group(1),
                            'component_type': 'Sense & Avoid',
                            'raw_data': log_content
                        }
                        log_data['VERSION'].append(record)

                # SnA:Receiving data parsing
                elif log_content.startswith('SnA:Receiving data,'):
                    # Extract only the tabular data after "SnA:Receiving data,"
                    data_part = log_content[len('SnA:Receiving data,'):].strip()
                    data_parts = data_part.split(',')
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 9:
                        record = {
                            'timestamp': timestamp,
                            'data_val1': safe_clean_value(data_parts[0], 'float'),
                            'data_val2': safe_clean_value(data_parts[1], 'float'),
                            'data_val3': safe_clean_value(data_parts[2], 'float'),
                            'data_val4': safe_clean_value(data_parts[3], 'float'),
                            'data_val5': safe_clean_value(data_parts[4], 'float'),
                            'data_val6': safe_clean_value(data_parts[5], 'float'),
                            'data_val7': safe_clean_value(data_parts[6], 'float'),
                            'data_val8': safe_clean_value(data_parts[7], 'int'),
                            'data_val9': safe_clean_value(data_parts[8], 'int')
                        }
                        log_data['SnA_RECEIVING_DATA'].append(record)

                # SnAInfo parsing
                elif log_content.startswith('SnAInfo'):
                    # Skip lines that are just "SnAInfo," without additional content
                    if log_content.strip() == 'SnAInfo' or log_content.strip() == 'SnAInfo,':
                        continue
                    
                    # Extract message after "SnAInfo, " or "SnAInfo: " or "SnAInfo,"
                    if log_content.startswith('SnAInfo, '):
                        message = log_content[len('SnAInfo, '):].strip()
                    elif log_content.startswith('SnAInfo: '):
                        message = log_content[len('SnAInfo: '):].strip()
                    elif log_content.startswith('SnAInfo : '):
                        message = log_content[len('SnAInfo : '):].strip()
                    elif log_content.startswith('SnAInfo,'):
                        message = log_content[len('SnAInfo,'):].strip()
                    else:
                        message = log_content[len('SnAInfo'):].strip()
                    
                    # Skip empty messages
                    if not message:
                        continue
                    
                    # Determine category based on message content
                    category = ''  # Default for basic messages
                    
                    if message.startswith('BOUNDARY_DATA'):
                        category = 'BOUNDARY DATA'
                    elif message.startswith('BOUNDARY_ITEM_INT'):
                        category = 'BOUNDARY ITEM'
                    elif message.startswith('BOUNDARY_COUNT'):
                        category = 'BOUNDARY COUNT'
                    elif 'Grid obstacle details:' in message:
                        category = 'GRID OBSTACLE'
                    elif 'Real time obstacle details:' in message:
                        category = 'REAL-TIME OBSTACLE'
                    elif any(keyword in message for keyword in ['Guided GOTO_LAT_LON_CHANGED:', 'Guided ALT_CHANGED:', 
                                                                'Ignoring radar data marker', 'Checking obstacle in',
                                                                'Ignore and Ignoring radar data markers']):
                        category = 'GUIDED/NAVIGATION INFO'
                    elif 'Grid data updated with' in message and 'data\'s in logs/sna/' in message:
                        category = 'GRID UPDATE'
                    
                    record = {
                        'timestamp': timestamp,
                        'category': category,
                        'message': message
                    }
                    log_data['SnA_INFO'].append(record)

                # GUIDED_MISSION and GUIDED_INFO parsing (unified)
                elif log_content.startswith(('GUIDED_MISSION,', 'GUIDED_INFO,')):
                    # Determine message type and extract content
                    if log_content.startswith('GUIDED_MISSION,'):
                        message_type = 'GUIDED_MISSION'
                        guided_data = log_content[len('GUIDED_MISSION,'):].strip()
                    elif log_content.startswith('GUIDED_INFO,'):
                        message_type = 'GUIDED_INFO'
                        guided_data = log_content[len('GUIDED_INFO,'):].strip()
                    
                    # Handle GUIDED_INFO format: "FLIGHT_MODE, STATE" (e.g., "GUIDED, TAKE_OFF")
                    if message_type == 'GUIDED_INFO':
                        if ', ' in guided_data:
                            parts = guided_data.split(', ', 1)
                            flight_mode = parts[0].strip()
                            state_key = parts[1].strip()
                            description = f"{flight_mode}, {state_key}"
                        else:
                            # Single value case
                            state_key = guided_data
                            description = guided_data
                    else:
                        # Handle GUIDED_MISSION format - more complex parsing
                        known_messages = {
                            'START MISSION RECEIVED': 'START_MISSION_RECEIVED',
                            'RTL COMMAND RECEIVED': 'RTL_COMMAND_RECEIVED', 
                            'RESUME COMMAND RECEIVED': 'RESUME_COMMAND_RECEIVED',
                            'GOTO COMMAND RECEIVED': 'GOTO_COMMAND_RECEIVED',
                            'STOPPING GUIDED CONTROLLER': 'STOPPING_CONTROLLER',
                            'RESTART FROM SNA': 'RESTART_FROM_SNA',
                            'Mode changed to guided': 'MODE_CHANGE_GUIDED',
                            ' Mode changed to guided': 'MODE_CHANGE_GUIDED',
                            'guided take_off': 'TAKEOFF',
                            'taking done': 'TAKEOFF_COMPLETE',
                            'Turning Yaw': 'SET_YAW',
                            'Checking HDNG': 'CHECK_HEADING',
                            'Checking Yaw': 'CHECK_YAW',
                            'No obstacle in front': 'NO_OBSTACLE',
                            'following guided path.': 'FOLLOWING_PATH',
                            'Smart_Path updated': 'PATH_UPDATED',
                            'Resume Triggered, Guided Mode.': 'RESUME_TRIGGERED',
                            'RTL Mode, Guided Mode': 'RTL_TRIGGERED',
                            'Starting spray': 'START_SPRAY',
                            'change_mode_to_auto': 'MODE_CHANGE_AUTO',
                            'Resetting smart path index and sending resume waypoint message': 'RESET_PATH_INDEX',
                            'starting scheduler_update_mission task.': 'START_SCHEDULER',
                        }
                        
                        state_key = None
                        description = guided_data
                        
                        # Check for exact matches first
                        if guided_data in known_messages:
                            state_key = known_messages[guided_data]
                            description = guided_data
                        # Check for partial matches in known patterns
                        else:
                            for pattern, key in known_messages.items():
                                if pattern in guided_data:
                                    state_key = key
                                    description = guided_data
                                    break
                        
                        # Handle special structured patterns
                        if state_key is None:
                            if guided_data.startswith('home_point :'):
                                state_key = 'HOME_POINT'
                                # Extract coordinates from "[lat, lon]"
                                coords_match = re.search(r'\[(.*?)\]', guided_data)
                                description = coords_match.group(1) if coords_match else guided_data
                            elif guided_data.startswith('GOTO_TEST,'):
                                state_key = 'GOTO_TEST'
                                description = guided_data[len('GOTO_TEST,'):].strip()
                            elif 'new_path_updated :' in guided_data:
                                state_key = 'NEW_PATH_UPDATED'
                                description = guided_data.split(':')[1].strip() if ':' in guided_data else guided_data
                            elif 'Going to waypoint' in guided_data:
                                state_key = 'GOTO_WAYPOINT'
                                description = guided_data
                            elif 'Next waypoint set to:' in guided_data:
                                state_key = 'NEXT_WAYPOINT_SET'
                                description = guided_data.split(':', 1)[1].strip() if ':' in guided_data else guided_data
                            elif 'increased smart_path_index + 1 :' in guided_data:
                                state_key = 'INCREMENT_PATH_INDEX'
                                index_match = re.search(r': (\d+)', guided_data)
                                description = index_match.group(1) if index_match else guided_data
                            elif 'time taken for calculation :' in guided_data:
                                state_key = 'CALCULATION_TIME'
                                time_match = re.search(r': (\d+)', guided_data)
                                description = time_match.group(1) if time_match else guided_data
                            elif 'Yaw is not within tolerance' in guided_data:
                                state_key = 'YAW_TOLERANCE_FAILED'
                                yaw_match = re.search(r'yaw_diff=([\d.]+)', guided_data)
                                description = yaw_match.group(1) if yaw_match else guided_data
                            elif 'State change failed' in guided_data:
                                state_key = 'STATE_CHANGE_FAILED'
                                time_match = re.search(r'time elapsed (\d+)', guided_data)
                                description = time_match.group(1) if time_match else guided_data
                        
                        # If no pattern matched, it's a generic/new message
                        if state_key is None:
                            state_key = ''
                            description = guided_data
                    
                    record = {
                        'timestamp': timestamp,
                        'message_type': message_type,
                        'state_key': state_key,
                        'description': description
                    }
                    log_data['GUIDED_MISSION'].append(record)

                # RESUME parsing  
                elif log_content.startswith(('RESUME_MISSION_STATUS,', 'RESUME_INFO,', 'RESUME,')):
                    # Determine message type and extract content
                    if log_content.startswith('RESUME_MISSION_STATUS,'):
                        message_type = 'RESUME_MISSION_STATUS'
                        resume_data = log_content[len('RESUME_MISSION_STATUS,'):].strip()
                    elif log_content.startswith('RESUME_INFO,'):
                        message_type = 'RESUME_INFO'
                        resume_data = log_content[len('RESUME_INFO,'):].strip()
                    elif log_content.startswith('RESUME,'):
                        message_type = 'RESUME'
                        resume_data = log_content[len('RESUME,'):].strip()
                    
                    # Known RESUME patterns from the code
                    known_resume_patterns = {
                        'setting resume height': 'SET_RESUME_HEIGHT',
                        'Resume mission aborted': 'MISSION_ABORTED',
                        'TAKEOFF, taking off': 'TAKEOFF_OPERATION',
                        'taking done': 'TAKEOFF_COMPLETE',
                        'climb_to_clearance_alt': 'CLIMB_CLEARANCE',
                        'change_mode_to_guided': 'MODE_CHANGE_GUIDED',
                        'change_mode_to_auto': 'MODE_CHANGE_AUTO',
                        'Turning Yaw': 'TURNING_YAW',
                        'Checking Yaw': 'CHECKING_YAW',
                        'Setting YAW': 'SETTING_YAW',
                        'No obstacle in front': 'NO_OBSTACLE',
                        'Starting spray': 'START_SPRAY',
                        'goto_rtl_loc': 'GOTO_RTL_LOCATION',
                        'descent_msn_alt': 'DESCENT_MISSION_ALT',
                        'flight_mode, resume_state': 'INFO_HEADER'
                    }
                    
                    state_key = None
                    description = resume_data
                    
                    # Check for known patterns
                    for pattern, key in known_resume_patterns.items():
                        if pattern in resume_data:
                            state_key = key
                            # Extract specific values for some patterns
                            if pattern == 'setting resume height':
                                height_match = re.search(r': ([\d.]+)', resume_data)
                                description = height_match.group(1) if height_match else resume_data
                            else:
                                description = resume_data
                            break
                    
                    # Handle RESUME state enum values from the code
                    resume_states = ['NONE', 'INITIATE', 'MODE_CHANGE_GUIDED', 'TAKE_OFF', 'CLIMB_CLR_ALT', 
                                    'SET_HDNG', 'CHECK_HDNG', 'OBST_IN_FRONT', 'GOTO_RTL_LOC', 
                                    'DESCENT_MSN_ALT', 'START_SPRAY', 'SET_NEXT_WP', 'MODE_CHANGE_AUTO', 
                                    'ABORT', 'OBST_DETECTED', 'RTL', 'STOP_YAW', 'AUTO_OBS_INFRONT', 'START_AUTO']
                    
                    if state_key is None:
                        for state in resume_states:
                            if state in resume_data.upper():
                                state_key = f'STATE_{state}'
                                description = resume_data
                                break
                    
                    # If no pattern matched, it's a generic/new message
                    if state_key is None:
                        state_key = 'UNKNOWN_MESSAGE'
                        description = resume_data
                    
                    record = {
                        'timestamp': timestamp,
                        'message_type': message_type,
                        'state_key': state_key,
                        'description': description
                    }
                    log_data['RESUME_MISSION'].append(record)
                    
                    # Add errors to ERROR log
                    if 'aborted' in resume_data.lower() or 'failed' in resume_data.lower():
                        if not added_to_error:
                            append_to_error_log(timestamp, f"RESUME ERROR: {resume_data}")
                            added_to_error = True

                # CC_PARAMETER_PERF parsing
                elif log_content.startswith('CC_PARAMETER_PERF:'):
                    perf_data = log_content[len('CC_PARAMETER_PERF:'):].strip()
                    
                    # Extract thread_id
                    thread_match = re.match(r'T(\d+)\s+(.+)', perf_data)
                    if thread_match:
                        thread_id = int(thread_match.group(1))
                        remaining = thread_match.group(2).strip()
                        
                        # Split remaining parts
                        parts = remaining.split()
                        
                        param_name = None
                        value = None
                        state = None
                        time_taken = None
                        
                        if 'NOT_FOUND' in remaining:
                            # Format: param_name NOT_FOUND {time}ms
                            param_name = parts[0]
                            value = None
                            state = 'NOT_FOUND'
                            # Extract time from last part
                            time_match = re.search(r'([\d.]+)ms', remaining)
                            if time_match:
                                time_taken = float(time_match.group(1))
                        
                        elif 'NO_CHANGE' in remaining:
                            # Format: param_name {val} {time}ms NO_CHANGE
                            param_name = parts[0]
                            # Find position of NO_CHANGE
                            no_change_idx = remaining.find('NO_CHANGE')
                            before_no_change = remaining[:no_change_idx].strip().split()
                            if len(before_no_change) >= 2:
                                value = before_no_change[1]
                            state = 'NO_CHANGE'
                            time_match = re.search(r'([\d.]+)ms', remaining)
                            if time_match:
                                time_taken = float(time_match.group(1))
                        
                        elif 'OUT_OF_RANGE' in remaining:
                            # Format: param_name {val} OUT_OF_RANGE(x-y) {time}ms
                            param_name = parts[0]
                            value = parts[1]
                            # Extract OUT_OF_RANGE with range
                            out_of_range_match = re.search(r'OUT_OF_RANGE\([^)]+\)', remaining)
                            if out_of_range_match:
                                state = out_of_range_match.group(0)
                            time_match = re.search(r'([\d.]+)ms', remaining)
                            if time_match:
                                time_taken = float(time_match.group(1))
                        
                        elif 'SUCCESS' in remaining:
                            # Format: param_name {val} {time}ms SUCCESS
                            param_name = parts[0]
                            value = parts[1]
                            state = 'SUCCESS'
                            time_match = re.search(r'([\d.]+)ms', remaining)
                            if time_match:
                                time_taken = float(time_match.group(1))
                        
                        elif 'FAILED' in remaining:
                            # Format: param_name {val} {time}ms FAILED
                            param_name = parts[0]
                            value = parts[1]
                            state = 'FAILED'
                            time_match = re.search(r'([\d.]+)ms', remaining)
                            if time_match:
                                time_taken = float(time_match.group(1))
                        
                        record = {
                            'timestamp': timestamp,
                            'thread_id': thread_id,
                            'param_name': param_name,
                            'value': value,
                            'state': state,
                            'time_taken_ms': time_taken
                        }
                        log_data['CC_PARAMETER_PERF'].append(record)

                # CC_PARAMETER_DB_PERF parsing
                elif log_content.startswith('CC_PARAMETER_DB_PERF:'):
                    db_perf_data = log_content[len('CC_PARAMETER_DB_PERF:'):].strip()
                    
                    # Extract thread_id
                    thread_match = re.match(r'T(\d+)\s+(.+)', db_perf_data)
                    if thread_match:
                        thread_id = int(thread_match.group(1))
                        remaining = thread_match.group(2).strip()
                        
                        state = None
                        description = remaining
                        
                        # Check for states and extract them
                        if remaining.startswith('NO_CHANGE'):
                            state = 'NO_CHANGE'
                            description = remaining[len('NO_CHANGE'):].strip()
                        elif 'SUCCESS' in remaining:
                            # Find SUCCESS at the end and split
                            success_idx = remaining.rfind('SUCCESS')
                            if success_idx != -1:
                                state = 'SUCCESS'
                                description = remaining[:success_idx].strip()
                        elif remaining.startswith('ERROR') or 'ERROR in' in remaining:
                            state = 'ERROR'
                            # Keep everything except the ERROR keyword at the start
                            if remaining.startswith('ERROR'):
                                description = remaining[len('ERROR'):].strip()
                            else:
                                # If ERROR appears in middle like "ERROR in db_name"
                                description = remaining
                        
                        record = {
                            'timestamp': timestamp,
                            'thread_id': thread_id,
                            'state': state,
                            'description': description
                        }
                        log_data['CC_PARAMETER_DB_PERF'].append(record)

                # SnALogging parsing - CONVERT True/False to 1/0 for structured data
                elif log_content.startswith('SnALogging,'):
                    if 'flight_mode |' in log_content:  # Skip header line
                        continue
                    
                    # Extract data after "SnALogging," and split by pipes
                    data_part = log_content[len('SnALogging,'):].strip()
                    pipe_parts = [part.strip() for part in data_part.split('|')]
                    
                    if len(pipe_parts) >= 10:
                        # Create record with base timestamp
                        record = {'timestamp': timestamp}
                        
                        # Define field names
                        field_names = [
                            'flight_mode', 'guided_mission_state', 'guided_controller_type', 
                            'obstacle_x', 'obstacle_y', 'obstacle_x_rot', 'obstacle_y_rot', 
                            'obstacle_sector', 'roll', 'pitch', 'yaw', 'px', 'py', 'pz', 
                            'vx', 'vy', 'speed', 'speed_rate', 'terrain_alt', 'mission_height', 
                            'target_altitude', 'clearance_altitude', 'course_over_ground',
                            'course_over_ground_wVelocity', 'COG_heading_angle_diff',
                            'stopping_distance', 'critical_stopping_distance', 'heading_available',
                            'horizontal_movement', 'ignore_radar_data_near_waypoint',
                            'ignoring_radar_data_near_waypoint', 'ignore_radar_data_till',
                            'braked_in_sna', 'avoidance_state', 'edge_avoidance_state',
                            'false_trigger_restart_state', 'obstacle_msg_delay_ms',
                            'radar_delay', 'grid_update_loop_time', 'loop_time', 'obstacle_buffer'
                        ]
                        
                        # Fields that should be expanded into multiple columns (comma-separated values)
                        multi_value_fields_data = {'obstacle_sector': 6, 'obstacle_buffer': 2}
                        multi_value_fields = list(multi_value_fields_data.keys())
                        
                        # Fields that should be kept as arrays (comma-separated values)
                        array_fields = ['obstacle_x', 'obstacle_y', 'obstacle_x_rot', 'obstacle_y_rot']
                        
                        # Fields that should have True/False converted to 1/0
                        boolean_fields = ['heading_available', 'horizontal_movement', 'ignore_radar_data_near_waypoint',
                                        'ignoring_radar_data_near_waypoint', 'braked_in_sna']
                        
                        # Fields that should be parsed as numeric even if they look like strings
                        numeric_string_fields = ['guided_mission_state', 'guided_controller_type', 
                                            'avoidance_state', 'edge_avoidance_state', 'false_trigger_restart_state']
                        
                        for i, field_name in enumerate(field_names):
                            if i < len(pipe_parts):
                                value = pipe_parts[i].strip() if pipe_parts[i] else ''
                                
                                # Handle empty or None values
                                if not value or value.upper() == 'NONE':
                                    if field_name in multi_value_fields:
                                        # For multi-value fields, create columns with None values
                                        ran = multi_value_fields_data[field_name] + 1
                                        for j in range(1, ran):
                                            record[f"{field_name}{j}"] = None
                                    elif field_name in array_fields:
                                        # For array fields, store as empty list
                                        record[field_name] = []
                                    else:
                                        record[field_name] = None
                                
                                # Handle array fields (keep as arrays)
                                elif field_name in array_fields:
                                    values = parse_list_values(value)
                                    # Ensure it's a regular Python list, not a numpy array
                                    record[field_name] = list(values) if values else []
                                
                                # Handle multi-value fields (comma-separated or bracket notation)
                                elif field_name in multi_value_fields:
                                    values = parse_list_values(value)
                                    # Create individual columns for each value
                                    ran = multi_value_fields_data[field_name] + 1
                                    for j in range(1, ran):
                                        if j <= len(values):
                                            record[f"{field_name}{j}"] = values[j-1]
                                        else:
                                            record[f"{field_name}{j}"] = None
                                
                                # Handle boolean fields - CONVERT True/False to 1/0
                                elif field_name in boolean_fields:
                                    record[field_name] = convert_boolean_to_numeric(value)
                                
                                # Handle numeric string fields - try to convert to numeric
                                elif field_name in numeric_string_fields:
                                    try:
                                        # Try to convert to float first
                                        numeric_value = float(value)
                                        # If it's a whole number, convert to int
                                        if numeric_value.is_integer():
                                            record[field_name] = int(numeric_value)
                                        else:
                                            record[field_name] = numeric_value
                                    except (ValueError, TypeError):
                                        # If conversion fails, keep as string but still allow it to be treated as categorical numeric
                                        record[field_name] = value if value and value.upper() != 'NONE' else None
                                
                                # Handle flight_mode (keep as string)
                                elif field_name == 'flight_mode':
                                    record[field_name] = value if value and value.upper() != 'NONE' else None
                                
                                else:
                                    # Numeric fields - use existing logic
                                    record[field_name] = safe_clean_value(value, 'auto')
                            else:
                                # Field not present - set to None or empty list
                                if field_name in multi_value_fields:
                                    ran = multi_value_fields_data[field_name] + 1
                                    for j in range(1, ran):
                                        record[f"{field_name}{j}"] = None
                                elif field_name in array_fields:
                                    record[field_name] = []
                                else:
                                    record[field_name] = None
                        
                        log_data['SnA_LOGGING'].append(record)

                # MISSION_STATE_CHANGED parsing
                elif log_content.startswith('MISSION_STATE_CHANGED,'):
                    # Extract the state value after "MISSION_STATE_CHANGED,"
                    state_value = log_content[len('MISSION_STATE_CHANGED,'):].strip()
                    
                    record = {
                        'timestamp': timestamp,
                        'value': safe_clean_value(state_value, 'int')
                    }
                    log_data['MISSION_STATE_CHANGED'].append(record)

                # SPRAY_INFO parsing
                elif log_content.startswith('SPRAY_INFO'):
                    if 'spray_status, pump_pwm' in log_content:  # Skip header line
                        continue
                    data_parts = log_content.split(',')[1:]  # Skip 'SPRAY_INFO'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 12:
                        record = {
                            'timestamp': timestamp,
                            'spray_status': safe_clean_value(data_parts[0], 'int'),
                            'pump_pwm': safe_clean_value(data_parts[1], 'int'),
                            'nozzle_pwm': safe_clean_value(data_parts[2], 'int'),
                            'req_flowrate_lpm': safe_clean_value(data_parts[3], 'float'),
                            'actual_flowrate_lpm': safe_clean_value(data_parts[4], 'float'),
                            'flowmeter_pulse': safe_clean_value(data_parts[5], 'int'),
                            'payload_rem_l': safe_clean_value(data_parts[6], 'float'),
                            'area_sprayed_acre': safe_clean_value(data_parts[7], 'float'),
                            'req_dosage_l_acre': safe_clean_value(data_parts[8], 'float'),
                            'actual_dosage_l_acre': safe_clean_value(data_parts[9], 'float'),
                            'prv_wp': safe_clean_value(data_parts[10], 'int'),
                            'next_wp': safe_clean_value(data_parts[11], 'int')
                        }
                        log_data['SPRAY_INFO'].append(record)
                
                # FlOWMETER parsing
                elif log_content.startswith('FlOWMETER,') or log_content.startswith('FLOWMETER,'):
                    if 'Flowrate(l/m), Signal_Count' in log_content:  # Skip header
                        continue
                    data_parts = log_content.split(',')[1:]  # Skip 'FlOWMETER'
                    data_parts = [p.strip() for p in data_parts]
                    if len(data_parts) >= 2:
                        record = {
                            'timestamp': timestamp,
                            'flowrate_lpm': safe_clean_value(data_parts[0], 'float'),
                            'signal_count': safe_clean_value(data_parts[1], 'int')
                        }
                        log_data['FLOWMETER'].append(record)

                # FLOWMETER_INFO parsing
                elif log_content.startswith('FLOWMETER_INFO,'):
                    data_parts = log_content.split(',')[1:]  # Skip 'FLOWMETER_INFO'
                    record = {
                        'timestamp': timestamp,
                        'flow_sensor_info': ' '.join(data_parts).strip()
                    }
                    log_data['FLOWMETER_INFO'].append(record)

                # PUMP parsing
                elif log_content.startswith('PUMP,'):
                    data_parts = log_content.split(',')[1:]  # Skip 'PUMP'
                    record = {
                        'timestamp': timestamp,
                        'pump_info': ' '.join(data_parts).strip()
                    }
                    log_data['PUMP'].append(record)

                # NOZZLE parsing
                elif log_content.startswith('NOZZLE,'):
                    # Extract the nozzle state after "NOZZLE,"
                    nozzle_state = log_content[len('NOZZLE,'):].strip()
                    
                    # Map nozzle states to numeric values
                    state_mapping = {
                        'Stopped': 0,
                        'Stop triggered': 0.5,
                        'Start': 1
                    }
                    
                    # Get numeric value, default to None if unknown state
                    numeric_value = state_mapping.get(nozzle_state, None)
                    
                    record = {
                        'timestamp': timestamp,
                        'value': numeric_value,
                        'raw_value': nozzle_state
                    }
                    log_data['NOZZLE'].append(record)

                # BOUNDARY_INTR parsing
                elif log_content.startswith('BOUNDARY_INTR'):
                    # Skip lines that are just "BOUNDARY_INTR," without additional content
                    if log_content.strip() == 'BOUNDARY_INTR' or log_content.strip() == 'BOUNDARY_INTR,':
                        continue
                    
                    # Extract message after "BOUNDARY_INTR, "
                    if log_content.startswith('BOUNDARY_INTR, '):
                        message = log_content[len('BOUNDARY_INTR, '):].strip()
                    elif log_content.startswith('BOUNDARY_INTR,'):
                        message = log_content[len('BOUNDARY_INTR,'):].strip()
                    else:
                        message = log_content[len('BOUNDARY_INTR'):].strip()
                    
                    # Skip empty messages
                    if not message:
                        continue
                    
                    record = {
                        'timestamp': timestamp,
                        'message': message
                    }
                    log_data['BOUNDARY_INTR'].append(record)

                else:
                    # Store other log types for reference
                    record = {
                        'timestamp': timestamp,
                        'log_content': log_content
                    }
                    log_data['OTHER'].append(record)
                    # Also, if error/failure message add to ERROR
                    if is_error_message(log_content):
                        if not added_to_error:
                            append_to_error_log(timestamp, log_content)
                            added_to_error = True
                    
            except Exception as e:
                print(f"Error parsing line {line_num}: {line}")
                print(f"Error: {e}")
                continue
    
    # Convert to DataFrames
    dataframes = {}
    for log_type, data in log_data.items():
        if data:  # Only create DataFrame if there's data
            dataframes[log_type] = pd.DataFrame(data)
            # Ensure timestamp is the first column
            cols = ['timestamp'] + [col for col in dataframes[log_type].columns if col != 'timestamp']
            dataframes[log_type] = dataframes[log_type][cols]
    
    print(f"\nParsing complete. Found {len(dataframes)} data types:")
    for log_type, df in dataframes.items():
        print(f"  {log_type}: {len(df)} records")
        if len(df) > 0 and log_type != 'OTHER':
            print(f"    Columns: {list(df.columns)}")
    
    # At the end of the function, extract filename and return both
    filename = os.path.basename(selected_file_path) if selected_file_path else ""
    
    return dataframes, filename

def convert_to_polars(pandas_dfs: Dict[str, pd.DataFrame]) -> Dict[str, pl.DataFrame]:
    """Convert pandas DataFrames to Polars DataFrames with proper error handling."""
    polars_dfs = {}
    for name, df in pandas_dfs.items():
        try:
            # Convert with explicit handling of mixed types
            polars_dfs[name] = pl.from_pandas(df, nan_to_null=True)
        except Exception as e:
            print(f"Warning: Could not convert {name} to Polars DataFrame: {e}")
            print(f"DataFrame info for {name}:")
            print(f"  Shape: {df.shape}")
            print(f"  Columns: {list(df.columns)}")
            print(f"  Data types:\n{df.dtypes}")
            print(f"Skipping {name} in Polars conversion")
            continue
    return polars_dfs

def main():
    # For testing, you can specify the file path directly and custom offset
    # Example with different offsets:
    # dataframes, filename = parse_log_file("tst_log.txt", timedelta(hours=0))  # UTC
    # dataframes, filename = parse_log_file("tst_log.txt", timedelta(hours=-5))  # EST
    # dataframes, filename = parse_log_file("tst_log.txt", timedelta(hours=8))  # China Standard Time
    
    # Default IST (+5:30)
    pandas_dataframes, filename = parse_log_file()  # FIXED: Unpack the tuple
    
    if not pandas_dataframes:
        return None, None
    
    # Get polars DataFrames with error handling
    polars_dataframes = convert_to_polars(pandas_dataframes)  # Now pandas_dataframes is just the dict
    
    # Show sample data for key DataFrames
    key_dfs = ['MISSION_INFO', 'RC_CHANNELS', 'SPRAY_INFO', 'GUIDED_MISSION', 'RESUME_MISSION', 'SnA_RECEIVING_DATA', 
                'SnA_LOGGING', 'ERROR']
    
    for df_name in key_dfs:
        if df_name in pandas_dataframes and len(pandas_dataframes[df_name]) > 0:
            print(f"\n=== {df_name} sample (first 3 rows) ===")
            print(pandas_dataframes[df_name].head(3))
            print(f"Total records: {len(pandas_dataframes[df_name])}")
    
    # ADDED: Print the loaded filename
    if filename:
        print(f"\nLoaded file: {filename}")
    
    return pandas_dataframes, polars_dataframes

if __name__ == "__main__":
    pandas_dfs, polars_dfs = main()
    if pandas_dfs:
        print("\nParsing completed successfully!")
        print("Use log_plotter.py to visualize the data.")