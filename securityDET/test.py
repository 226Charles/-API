from collections import deque
from datetime import datetime
import redis
from redis import ConnectionPool
import json
import numpy as np

class CircularBuffer:
    def __init__(self, size):
        self.size = size
        self.buffer = deque(maxlen=size)
        self.current_index = -1  # 初始化为-1，表示尚未添加元素

    def append(self, item):
        self.current_index = (self.current_index + 1) % self.size
        if len(self.buffer) < self.size:
            self.buffer.append(item)
        else:
            self.buffer[self.current_index] = item

    def modify_current(self, modification_function):
        if self.current_index >= 1:
            self.buffer[self.current_index] = modification_function(self.buffer[self.current_index],self.get_previous())
        else:
            raise ValueError("No data to modify yet")

    def get_current(self):
        if len(self.buffer) >= 1:
            return self.buffer[self.current_index]
        else:
            raise ValueError("No data in the buffer to get current")

    def get_previous(self):
        if len(self.buffer) >= 2:
            previous_index = (self.current_index - 1) % self.size
            return self.buffer[previous_index]
        else:
            raise ValueError("Not enough elements in the buffer to get previous")

    def is_empty(self):
        return len(self.buffer) == 0

def modify_function(data1, data2):
    data1 = data2 +5
    return data1

# 创建一个存储 deviceid 和对应 CircularBuffer 的字典
device_buffers = {}

# 存入数据到对应的 CircularBuffer，如果没有找到对应的 deviceid 则创建新的 CircularBuffer
def append_data(deviceid, data, buffer_size):
    if deviceid not in device_buffers:
        device_buffers[deviceid] = CircularBuffer(buffer_size)
    device_buffers[deviceid].append(data)

# 示例：定义一个修改函数，将当前数据的某一部分乘以2
def modify_function(data1,data2):
    data1 = data2 + 5
    return data1

# 示例：存入数据到对应的 CircularBuffer，会自动创建新的 CircularBuffer（如果需要）
for i in range(1000):
    append_data("device1", i, 1000)
    append_data("device2", i * 2, 500)
device_buffers["device1"].modify_current(modify_function)

print(device_buffers["device1"].get_current())
# 示例：查询设备的前一个元素
print("Previous element for device1:", device_buffers["device1"].get_previous())
print("Previous element for device2:", device_buffers["device2"].get_previous())


timestamp1_str  = "1693191805324"
timestamp2_str  = "1693191804524"

timestamp1 = int(timestamp1_str)
timestamp2 = int(timestamp2_str)

time_difference = timestamp2 - timestamp1

print("时间差：", time_difference)

# 创建连接池
'''pool = ConnectionPool(host='localhost', port=6379, db=0, decode_responses=True)

# 连接到 Redis 服务器
redis_client = redis.Redis(connection_pool=pool)

redis_client.hset("test", 1, [1,2,3])
buffer_data_json = redis_client.hget("test", 1)'''

'''class DataProcessor:
    def __init__(self, batch_size, outlier_threshold):
        self.batch_size = batch_size
        self.outlier_threshold = outlier_threshold
        self.data_list = []
    
    def process_data(self, new_data):
        self.data_list.append(new_data)
        if len(self.data_list) >= self.batch_size:
            batch_data = self.data_list
            variances = [np.var(batch_data[:i] + batch_data[i+1:]) for i in range(len(batch_data))]
            
            min_variance_index = np.argmin(variances)
            print(min_variance_index)
            print(variances)
            print(np.var(batch_data))
            if abs(np.var(batch_data) - variances[min_variance_index]) > self.outlier_threshold:
                removed_data = self.data_list.pop(min_variance_index)
                print(f"Removed data {removed_data} due to high variance")
    
    def get_data_list(self):
        return self.data_list
    
    def get_and_remove_first_data(self):
        if len(self.data_list) == self.batch_size:
            first_data = self.data_list.pop(0)
            print(f"Removed and returned first data {first_data}")
            return first_data
        else:
            print("No data to remove.")
            return None
        
    def _is_batch_full(self):
        return len(self.data_list) == self.batch_size
    

# Parameters
batch_size = 12
outlier_threshold = 10000  # Adjust the threshold as needed

# Create an instance of DataProcessor
processor = DataProcessor(batch_size, outlier_threshold)

# Simulate incoming data
incoming_data = [10,20,30,20,10,500,500,20,30,20,10,50,40]

# Process incoming data
for data in incoming_data:
    processor.process_data(data)

# Get the final processed data
final_processed_data = processor.get_data_list()
print("Final processed data:", final_processed_data)'''

class DataProcessor:
    def __init__(self, batch_size, vib_threshold, gas_threshold):
        self.batch_size = batch_size
        self.vib_threshold = vib_threshold
        self.gas_threshold = gas_threshold
        self.data_list = []
    
    def process_data(self, new_data):
        vib_data = new_data["vib"]
        gas_data = new_data["gas"]
        
        self.data_list.append(new_data)
        if len(self.data_list) >= self.batch_size:
            self._process_batch(vib_data, gas_data)
    
    def _process_batch(self, vib_data, gas_data):
        batch_data = self.data_list
        vib_variances = [np.var([entry["vib"] for entry in batch_data[:i]] + [entry["vib"] for entry in batch_data[i+1:]]) for i in range(len(batch_data))]
        gas_variances = [np.var([entry["gas"] for entry in batch_data[:i]] + [entry["gas"] for entry in batch_data[i+1:]]) for i in range(len(batch_data))]

        vib_min_variance_index = np.argmin(vib_variances)
        gas_min_variance_index = np.argmin(gas_variances)
        current_vib_variance = np.var([entry["vib"] for entry in batch_data])
        current_gas_variance = np.var([entry["gas"] for entry in batch_data])
        
        if abs(current_vib_variance - vib_variances[vib_min_variance_index]) > self.vib_threshold:
            removed_data = self.data_list.pop(vib_min_variance_index)
            print(f"Removed data {removed_data} due to high vib variance")
        if abs(current_gas_variance - gas_variances[gas_min_variance_index]) > self.gas_threshold:
            removed_data = self.data_list.pop(gas_min_variance_index)
            print(f"Removed data {removed_data} due to high gas variance")
    
    def get_data_list(self):
        return self.data_list
    
    def _is_batch_full(self):
        return len(self.data_list) == self.batch_size

    def get_and_remove_first_data(self):
        if len(self.data_list) == self.batch_size:
            first_data = self.data_list.pop(0)
            print("_------------")
            print(len(self.data_list))
            print(f"Removed and returned first data {first_data}")
            return first_data
        else:
            print("No data to remove.")
            return None

# Parameters
batch_size = 12
vib_threshold = 10000
gas_threshold = 10000

# Create an instance of DataProcessor
processor = DataProcessor(batch_size, vib_threshold, gas_threshold)


incoming_data = [
    {"vib": 10, "gas": 5},
    {"vib": 20, "gas": 10},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
    {"vib": 30, "gas": 8},
]

# Process incoming data
for data in incoming_data:
    processor.process_data(data)

# Get the final processed data
final_processed_data = processor.get_data_list()
print("Final processed data:", final_processed_data)
print(processor._is_batch_full())
print(processor.get_and_remove_first_data())