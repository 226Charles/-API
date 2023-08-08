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

# 指定目标URL
target_url = "http://157.0.243.12:9999/iotResult"
'''
需要的参数：
1、persist_time 默认为0
2、vibration_acceleration
3、gas_concentration
4、gas_concentration_threshold
5、vibration_acceleration_threshold
6、old_persist_time 默认为0
'''
app = Flask(__name__)
CORS(app)
vibration_acceleration_threshold = 0
gas_concentration_threshold = 0

'''
初始状态判断：气体< 加速度< 
刚转换状态 判断上一状态是否是持续开风机的结束状态 检测时间
否则是持续静止
'''
def type_0_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    if persist_time == 0 :
        if old_gas_concentration < gas_concentration_threshold and old_vibration_acceleration >= vibration_acceleration_threshold:
            if old_persist_time >= 1800:
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
        if  old_persist_time >= 1800:
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
状态4 停工后保持通风 检测5分钟
'''
def type_4_judge(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):
    if old_vibration_acceleration >= vibration_acceleration_threshold and old_gas_concentration < gas_concentration_threshold:
        if  old_persist_time >= 300:
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
            else:
                flag = 10
    
    return flag, change

def logic_control(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time):

    ans = False
    
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
    return ans,change

def cleanup_expired_cache():
    now = datetime.now()
    expired_keys = [key for key, data in data_cache.items() if now - data.get('timestamp', datetime.min) > timedelta(seconds=CACHE_EXPIRATION)]
    for key in expired_keys:
        data_cache.pop(key, None)

# 用于缓存上一组数据
data_cache = {}

# 定义缓存有效期（以秒为单位）
CACHE_EXPIRATION = 60 * 10  # 10分钟;
TIME_DELTA_SECONDS = 5  # 5秒的时间间隔，单位为秒


@app.route('/api/safety', methods=['GET'])
def main():

    global vibration_acceleration_threshold
    global gas_concentration_threshold

    #风机阈值300 浓度阈值100
    vibration_acceleration_threshold = float(request.args.get('vibthr'))
    gas_concentration_threshold = float(request.args.get('gasthr'))

    '''
    测试通过
    当前风机100
    浓度99
    持续时间0
    上一帧风机350
    浓度99
    持续时间1800
    '''
    time = int(request.args.get('time'))
    device_id = request.args.get('id')
    vibration_acceleration = float(request.args.get('vib'))
    gas_concentration = float(request.args.get('gas'))
    #persist_time = int(request.args.get('pers'))

    # 计算 previous_time 和 now_time，不需要转换为 datetime 对象
    previous_time = time - (TIME_DELTA_SECONDS * 1000)
    now_time = time

    # 构建唯一键
    cache_key = f"{device_id}_{previous_time}"
    now_cache_key = f"{device_id}_{now_time}"
    '''# 构建唯一键，使用当前时间减去 5 秒作为 time
    previous_time = datetime.strptime(time, '%Y%m%d%H%M%S') - TIME_DELTA
    now_time = datetime.strptime(time, '%Y%m%d%H%M%S')
    cache_key = f"{device_id}_{previous_time.strftime('%Y%m%d%H%M%S')}"
    now_cache_key = f"{device_id}_{now_time.strftime('%Y%m%d%H%M%S')}"'''
    print(cache_key)
    print(now_cache_key)

    # 清理过期数据
    cleanup_expired_cache()

    # 从缓存中获取上一组数据
    cached_data = data_cache.get(cache_key)
    if cached_data is None:
        cached_data = {
            'timestamp': datetime.now(),  # 记录时间戳
            'oldvib': vibration_acceleration,
            'oldgas': gas_concentration,
            'oldpers': 0
        }
        data_cache[cache_key] = cached_data
    
    old_vibration_acceleration = cached_data.get('oldvib')
    old_gas_concentration = cached_data.get('oldgas')
    old_persist_time = cached_data.get('oldpers')

    persist_time = old_persist_time + TIME_DELTA_SECONDS

    '''old_vibration_acceleration = float(request.args.get('oldvib'))
    old_gas_concentration = float(request.args.get('oldgas'))
    old_persist_time = int(request.args.get('oldpers'))'''
    '''print(vibration_acceleration)
    print(gas_concentration)
    print(persist_time)
    print(old_vibration_acceleration)
    print(old_gas_concentration)
    print(old_persist_time)'''
    ans,change = logic_control(vibration_acceleration, gas_concentration, persist_time, old_vibration_acceleration, old_gas_concentration, old_persist_time)
    #print(change)
    if change == True:
        # 将本次的数据保存到缓存中
        data_cache[now_cache_key] = {
            'timestamp': datetime.now(),  # 记录时间戳
            'oldvib': vibration_acceleration,
            'oldgas': gas_concentration,
            'oldpers': 0
        }
    else:
        # 将本次的数据保存到缓存中
        data_cache[now_cache_key] = {
            'timestamp': datetime.now(),  # 记录时间戳
            'oldvib': vibration_acceleration,
            'oldgas': gas_concentration,
            'oldpers': persist_time
        }
    print(data_cache)
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

    # 发送POST请求
    response = requests.post(target_url, json=result_data)

    # 检查响应
    if response.status_code == 200:
        print("POST request successful")
    else:
        print(f"POST request failed with status code {response.status_code}")

    return jsonify(response_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)