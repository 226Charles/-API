from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity
from pandas import Timestamp
from pandas import to_datetime
from flask import jsonify
from flask import request
from datetime import datetime, timedelta
from collections import defaultdict
import requests
from collections import deque
import logging
import sys
import redis
from redis import ConnectionPool
import json
import numpy as np



#8.28时间戳改为相减 毫秒级 完成
#传公司镜像
#内存改redis 完成
#外面套一层 处理噪音 完成


# 指定目标URL
# 部署时改为内网
target_url = "http://157.0.243.12:9999/iotResult"
'''
需要的参数：
1、vibration_acceleration
2、gas_concentration
3、gas_concentration_threshold
4、vibration_acceleration_threshold
5、time 时间戳 (算法不需要)
6、deviceid 设备ID (算法不需要)
'''
app = Flask(__name__)
CORS(app)
vibration_acceleration_threshold = 0
gas_concentration_threshold = 0

# 创建连接池
pool = ConnectionPool(host='localhost', port=6379, db=1, decode_responses=True)

# 连接到 Redis 服务器
redis_client = redis.Redis(connection_pool=pool)


'''
初始状态判断：气体< 加速度< 
刚转换状态 判断上一状态是否是持续开风机的结束状态 检测时间
否则是持续静止
'''
def type_0_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    if persist_time == 0 and old_persist_time == 0:#刚开机器
        return True
    if persist_time == 0 :
        if old_gas_concentration < gas_concentration_threshold and old_vibration_acceleration >= vibration_acceleration_threshold:
            if old_persist_time >= 1800000:
                return True
            else:
                return False
        else:
            return False
    else:
        return True
    
'''
状态1 提前30分钟通风 只要判断是不是静止转过来的就好
'''
def type_1_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    if old_vibration_acceleration < vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
        return True
    else:
        if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
            return True
        else:
            return False
    
'''
状态2 开始工作，判断是不是满足30分钟以上通风的状态1转移过来 以及是不是保持2
'''
def type_2_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
        if  old_persist_time >= 1800000:
            return True
        else:
            return False
    else:
        if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration >= gas_concentration_threshold:
            return True
        else:
            return False
'''
状态3 准备停工，判断是不是工作状态2转移来即可
'''
def type_3_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration >= gas_concentration_threshold:
        return True
    else:
        return False
    
'''
状态4 停工后保持通风 检测30分钟
'''
def type_4_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
        if  old_persist_time >= 1800000:
            return True
        else:
            return False
    else:
        return False


'''
状态5 低浓度持续通风 适用于持续的1以及持续的4
'''
def type_5_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
        return True
    else:
        return False

        
def type_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    
    flag = 0
    change = False

    #0
    if vibration_acceleration < vibration_acceleration_threshold and gas_concentration < gas_concentration_threshold:
        flag = 0
        if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
            flag = 4
            change = True
        elif old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration >= gas_concentration_threshold:
            flag = 10#缺失工作后通风
            change = True
    else:
        if vibration_acceleration >= vibration_acceleration_threshold and gas_concentration < gas_concentration_threshold:
            if old_vibration_acceleration < vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
                flag = 1
                change = True
            else:
                if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
                    flag = 5
                elif old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration >= gas_concentration_threshold:
                    flag = 3
                    change = True
                else :
                    flag = 10
        else:
            if vibration_acceleration >= vibration_acceleration_threshold and gas_concentration >= gas_concentration_threshold:
                if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
                    flag = 2
                    change = True
                elif old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration >= gas_concentration_threshold:
                    flag = 2
                elif old_vibration_acceleration < vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
                    flag = 10#缺失工作前通风
                    change = True
            else:
                flag = 10

    return flag, change

def logic_control(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):

    ans = False
    work = False
    
    flag,change = type_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    
    if flag == 0 :
        ans = type_0_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    elif flag == 1:
        ans = type_1_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    elif flag == 2:
        ans = type_2_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    elif flag == 3:
        ans = type_3_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    elif flag == 4:
        ans = type_4_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    elif flag == 5:
        ans = type_5_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    else:
        ans = False

    if(flag != 0):
        work = True

    return ans,change,work

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
        if self.current_index >= 0:
            self.buffer[self.current_index] = modification_function(self.buffer[self.current_index],self.get_previous())
        else:
            raise ValueError("No data to modify yet")

    def get_previous(self):
        if len(self.buffer) >= 2:
            previous_index = (self.current_index - 1 + self.size) % self.size
            return self.buffer[previous_index]
        else:
            raise ValueError("Not enough elements in the buffer to get previous")

    def is_empty(self):
        return len(self.buffer) == 0
    
    def to_dict(self):
        return {
            "size": self.size,
            "buffer": list(self.buffer),
            "current_index": self.current_index
        }

# 定义一个自定义的 JSONEncoder 子类，用于将 CircularBuffer 转换为 JSON 格式
class CircularBufferEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, CircularBuffer):
            return {
                "size": obj.size,
                "buffer": obj.buffer,
                "current_index": obj.current_index
            }
        return super().default(obj)

def modify_function1(data1, data2):
    data1['oldpers'] = data2['oldpers'] + (data1['time'] - data2['time'])
    return data1

def modify_function2(data1, data2):
    data1['oldpers'] = 0
    return data1

# 创建一个存储 deviceid 和对应 CircularBuffer 的字典
device_buffers = {}
device_work = {}

# 存入数据到对应的 CircularBuffer，如果没有找到对应的 deviceid 则创建新的 CircularBuffer
def append_data(deviceid, data, buffer_size):
    if deviceid not in device_buffers:
        device_buffers[deviceid] = CircularBuffer(buffer_size)
    device_buffers[deviceid].append(data)

buffer_size = 1000

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
            print(f"Removed and returned first data {first_data}")
            return first_data
        else:
            print("No data to remove.")
            return None
    
@app.route('/api/noise', methods=['GET'])
def test():

    global vibration_acceleration_threshold
    global gas_concentration_threshold
    batch_size = 12

    #风机阈值300 浓度阈值100
    vibration_acceleration_threshold = float(request.args.get('vibthr'))
    gas_concentration_threshold = float(request.args.get('gasthr'))

    time = int(request.args.get('time'))
    device_id = request.args.get('id')
    vibration_acceleration = float(request.args.get('vib'))
    gas_concentration = float(request.args.get('gas'))
    vs = float(request.args.get('vs'))
    gs = float(request.args.get('gs'))

    processor = DataProcessor(batch_size, vs, gs)

    value = redis_client.hget("device_noise", device_id)
    device_noise = []
    first_data = {}
    temp_cached = {
            'vibthr': vibration_acceleration_threshold,
            'gasthr': gas_concentration_threshold,
            'time':time,
            'id':device_id,
            'vib':vibration_acceleration,
            'gas':gas_concentration,
            'vs':vs,
            'gs':gs
        }
    
    if value == None:
        device_noise.append(temp_cached)
        redis_client.hset("device_noise", device_id, json.dumps(device_noise))
    else:
        old_device_noise = json.loads(value)
        old_device_noise.append(temp_cached)
        for data in old_device_noise:
            processor.process_data(data)
        final_processed_data = processor.get_data_list()
        print(len(processor.data_list))
        if processor._is_batch_full():
            first_data = processor.get_and_remove_first_data()
        redis_client.hset("device_noise", device_id, json.dumps(final_processed_data))

    #print(first_data)
    if(first_data == {}):
        return jsonify({"error":"data is not enough"})
    else:
        vibration_acceleration_threshold = first_data['vibthr']
        gas_concentration_threshold = first_data['gasthr']
        time = first_data['time']
        device_id = first_data['id']
        vibration_acceleration = first_data['vib']
        gas_concentration = first_data['gas']
        return safety(vibration_acceleration_threshold,gas_concentration_threshold,time,device_id,vibration_acceleration,gas_concentration)

@app.route('/api/work', methods=['GET'])
def is_Work():
    device_id = request.args.get('id')
    
    if device_id is None:
        return jsonify({"error": "Missing 'id' parameter"}), 400  # 返回一个错误响应
    
    try:
        #is_work = device_work[device_id]
        work = redis_client.hget("device_work", device_id)
        is_work = False
        print(work)
        if work is not None and int(work) == 1:
            is_work = True
        ans = {
            "work": is_work
        }
        return jsonify(ans)
    except KeyError:
        return jsonify({"error": "Device not found"}), 404  # 返回一个错误响应

def safety(vibration_acceleration_threshold,gas_concentration_threshold,time,device_id,vibration_acceleration,gas_concentration):

    #风机阈值300 浓度阈值100
    '''vibration_acceleration_threshold = float(request.args.get('vibthr'))
    gas_concentration_threshold = float(request.args.get('gasthr'))

    time = int(request.args.get('time'))
    device_id = request.args.get('id')
    vibration_accelertaion = float(request.args.get('vib'))
    gas_concentration = float(request.args.get('gas'))'''

    # 从缓存中获取上一组数据
    old_vibration_acceleration = 0
    old_gas_concentration = 0
    old_persist_time = 0
    old_time = time
    persist_time = 0

    value = redis_client.hget("device_buffers", device_id)
    flag_null = False
    #检查该设备是否有缓冲
    if value == None:
        print(f"Field {device_id} does not exist in the hash table.")
        device_buffers[device_id] = CircularBuffer(buffer_size)  # 默认 buffer size 为 1000
        cached_data = {
            'oldvib': vibration_acceleration,
            'oldgas': gas_concentration,
            'time':time,
            'oldpers': 0
        }
        append_data(device_id, cached_data, 1000)
        flag_null = True
        old_vibration_acceleration = vibration_acceleration
        old_gas_concentration = gas_concentration
        old_persist_time = 0
    else:
        cached_data = {
            'oldvib': vibration_acceleration,
            'oldgas': gas_concentration,
            'time':time,
            'oldpers': 0
        }
        stored_circular_buffer_data = value
        if stored_circular_buffer_data:
            stored_circular_buffer_data = json.loads(stored_circular_buffer_data)
            device_buffers[device_id] = CircularBuffer(stored_circular_buffer_data["size"])
            device_buffers[device_id].buffer = deque(stored_circular_buffer_data["buffer"])
            device_buffers[device_id].current_index = stored_circular_buffer_data["current_index"]
            #print(device_buffers[device_id].buffer)
        else:
            print(f"No CircularBuffer data found for device: {device_id}")
        append_data(device_id, cached_data, 1000)
        old_cached_data = device_buffers[device_id].get_previous()
        old_vibration_acceleration = old_cached_data['oldvib']
        old_gas_concentration = old_cached_data['oldgas']
        old_persist_time = old_cached_data['oldpers']
        old_time = int(old_cached_data['time'])

    persist_time = old_persist_time + (time - old_time)
    print(persist_time,flush=True)

    ans,change,work = logic_control(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    
    if change == True:
        device_buffers[device_id].modify_current(modify_function2) #变了=0
    else:
        if flag_null == False:
            device_buffers[device_id].modify_current(modify_function1) #没变+时间戳
    
    device_work[device_id] = work
    if(work == False):
        work = 0
    else:
        work = 1
    redis_client.hset("device_work", device_id, int(work))

    # 存储 CircularBuffer 实例到 Redis
    #buffer_data = json.dumps(device_buffers[device_id], cls=CircularBufferEncoder)
    buffer_data = json.dumps(device_buffers[device_id].to_dict())
    redis_client.hset("device_buffers", device_id, buffer_data)
    #redis_client.hset("device_buffers", device_id, json.dumps(device_buffers[device_id]))
    
    response_data = {
        "safe": ans,
        "change": change,
        "id": device_id,
        "time": time
    }

    # 定义要发送的数据
    result_data = {
        "deviceId": device_id,
        "timestamp": time,  # 当前时间的毫秒时间戳
        "safe": ans
    }

    if ans == False:
        # 发送POST请求
        response = requests.post(target_url, json=result_data)
        
        # 检查响应
        if response.status_code == 200:
            print("POST request successful")
        else:
            print(f"POST request failed with status code {response.status_code}")

    print("-----------------------------", flush=True)
    print("safe : {}, timestamp : {}, change : {}, vib : {}, gas : {}".format(ans,time,change,vibration_acceleration,gas_concentration), flush=True)

    return jsonify(response_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8083)