import pandas as pd
from datetime import datetime, timedelta
import pymysql
from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity
from pandas import Timestamp
from pandas import to_datetime
from flask import jsonify
from flask import request

#API接口

app = Flask(__name__)
CORS(app)
'''
# 设置 JWT 密钥，用于加密和解密 JWT
app.config['JWT_SECRET_KEY'] = 'your_secret_key'
jwt = JWTManager(app)

# 校验 JWT 授权令牌
@jwt.user_identity_loader
def user_identity_lookup(user):
    return user['api_key']

# 生成 JWT 授权令牌
def generate_jwt_token(api_key):
    return create_access_token(identity={'api_key': api_key})

# 校验 API 密钥
def verify_api_key(api_key):
    # 在实际应用中，你可以根据自己的需求，从数据库或其他存储中获取 API 密钥并进行校验
    return api_key == 'your_api_key'

@app.route('/get_token', methods=['POST'])
def get_token():
    api_key = request.json.get('api_key')

    if verify_api_key(api_key):
        token = generate_jwt_token(api_key)
        return jsonify({'token': token})
    else:
        return jsonify({'message': 'Unauthorized'}), 401


@app.route('/detect_gas', methods=['GET'])
def detect_gas():

    return {'result': True}
'''

def get_data_within_12_hours(timestamps_str, conn):

    # 将timestamps_str转换为datetime对象
    timestamps = datetime.strptime(timestamps_str, '%Y%m%d%H%M%S')

    # 计算前12小时的时间范围
    start_time = timestamps - timedelta(hours=12)
    end_time = timestamps - timedelta(seconds=5)

    # 将时间范围转换为字符串格式
    start_time_str = start_time.strftime('%Y%m%d%H%M%S')
    end_time_str = end_time.strftime('%Y%m%d%H%M%S')

    # 查询数据的SQL语句
    sql = f"SELECT * FROM security WHERE timestamps >= '{start_time_str}' AND timestamps <= '{end_time_str}';"

    # 使用pandas的read_sql函数读取数据并返回DataFrame
    data_df = pd.read_sql(sql, conn)

    return data_df

def get_data_within_12_hours_6_mins(timestamps_str, conn):

    # 将timestamps_str转换为datetime对象
    timestamps = datetime.strptime(timestamps_str, '%Y%m%d%H%M%S')

    # 计算前12小时的时间范围
    start_time = timestamps - timedelta(hours=12)
    end_time = timestamps - timedelta(minutes=6)

    # 将时间范围转换为字符串格式
    start_time_str = start_time.strftime('%Y%m%d%H%M%S')
    end_time_str = end_time.strftime('%Y%m%d%H%M%S')

    # 查询数据的SQL语句
    sql = f"SELECT * FROM security WHERE timestamps >= '{start_time_str}' AND timestamps <= '{end_time_str}';"

    # 使用pandas的read_sql函数读取数据并返回DataFrame
    data_df = pd.read_sql(sql, conn)

    return data_df

#12小时内是否一直通风
def check_vibration_acceleration_12_hours(data_df, vibration_acceleration_threshold):

    # 检查最近12小时内vibration_acceleration是否一直大于阈值
    all_above_threshold = (data_df['vibration_acceleration'] > vibration_acceleration_threshold).all()
    return all_above_threshold

#2.1正在进行状态
def check_vibration_acceleration_within_30_minutes(timestamps_str, conn, gas_concentration_threshold, vibration_acceleration_threshold):

    # 获取当前时间戳之前12小时内的数据
    data_df = get_data_within_12_hours(timestamps_str, conn)

    # 将data_df中的timestamps列转换为datetime对象
    data_df['timestamps'] = pd.to_datetime(data_df['timestamps'], format='%Y%m%d%H%M%S')

    # 找到离当前时间戳最近且小于gas_concentration_threshold的数据点
    filtered_data = data_df[data_df['gas_concentration'] < gas_concentration_threshold]
    if not filtered_data.empty:
        target_timestamp = Timestamp(datetime.strptime(timestamps_str, '%Y%m%d%H%M%S'))
        nearest_index = (filtered_data['timestamps'] - target_timestamp).abs().idxmin()
        #print(nearest_index)
        # 找到该数据点前30分钟到现在的时间范围
        start_time = data_df.loc[nearest_index, 'timestamps'] - timedelta(minutes=30)
        end_time = datetime.strptime(timestamps_str, '%Y%m%d%H%M%S')

        # 检查该时间范围内的所有数据点的vibration_acceleration是否都大于vibration_acceleration_threshold
        filtered_data = data_df[(data_df['timestamps'] >= start_time) & (data_df['timestamps'] <= end_time)]
        all_above_threshold = (filtered_data['vibration_acceleration'] > vibration_acceleration_threshold).all()

        return all_above_threshold
    
    # 如果没有小于gas_concentration_threshold的数据点，则直接检测12小时内是否一直通风
    return check_vibration_acceleration_12_hours(data_df, vibration_acceleration_threshold)  

#2.2刚刚结束状态
def check_vibration_acceleration_within_30_minutes_6_mins(timestamps_str, conn, gas_concentration_threshold, vibration_acceleration_threshold):

    # 获取当前时间戳之前12小时内的数据
    data_df = get_data_within_12_hours_6_mins(timestamps_str, conn)

    # 将data_df中的timestamps列转换为datetime对象
    data_df['timestamps'] = pd.to_datetime(data_df['timestamps'], format='%Y%m%d%H%M%S')

    # 找到离当前时间戳最近且小于gas_concentration_threshold的数据点
    filtered_data = data_df[data_df['gas_concentration'] < gas_concentration_threshold]
    #print(filtered_data)
    if not filtered_data.empty:
        target_timestamp = Timestamp(datetime.strptime(timestamps_str, '%Y%m%d%H%M%S'))
        nearest_index = (filtered_data['timestamps'] - target_timestamp).abs().idxmin()

        # 找到该数据点前30分钟到现在的时间范围
        start_time = data_df.loc[nearest_index, 'timestamps'] - timedelta(minutes=30)
        end_time = datetime.strptime(timestamps_str, '%Y%m%d%H%M%S')

        # 检查该时间范围内的所有数据点的vibration_acceleration是否都大于vibration_acceleration_threshold
        filtered_data = data_df[(data_df['timestamps'] >= start_time) & (data_df['timestamps'] <= end_time)]
        all_above_threshold = (filtered_data['vibration_acceleration'] > vibration_acceleration_threshold).all()

        return all_above_threshold

    # 如果没有小于gas_concentration_threshold的数据点，则直接检测12小时内是否一直通风
    return check_vibration_acceleration_12_hours(data_df, vibration_acceleration_threshold)  

#2.3 检测最近开工是否是30分钟内
def check_gas_concentration_within_30_minutes(timestamps_str, conn, gas_concentration_threshold):

    # 获取当前时间戳之前12小时内的数据
    data_df = get_data_within_12_hours(timestamps_str, conn)
    #print(data_df)

    # 将data_df中的timestamps列转换为datetime对象
    data_df['timestamps'] = pd.to_datetime(data_df['timestamps'], format='%Y%m%d%H%M%S')

    # 找到离当前时间戳最近且gas大于等于阈值的数据点
    filtered_data = data_df[data_df['gas_concentration'] >= gas_concentration_threshold]
    #print(filtered_data)
    if not filtered_data.empty:
        target_timestamp = Timestamp(datetime.strptime(timestamps_str, '%Y%m%d%H%M%S'))
        nearest_index = (filtered_data['timestamps'] - target_timestamp).abs().idxmin()

        # 计算该数据点的时间戳距离当前时间戳的时间差
        time_diff = datetime.strptime(timestamps_str, '%Y%m%d%H%M%S') - data_df.loc[nearest_index, 'timestamps']
        time_diff_minutes = time_diff.total_seconds() / 60
        print(time_diff_minutes)
        # 检查时间差是否不超过30分钟
        return time_diff_minutes <= 30

    return False  # 如果没有gas大于阈值的数据点，则直接返回False

#2.4 结束通风未彻底结束阶段
def check_vibration_acceleration_within_time_range(timestamps_str, conn, gas_concentration_threshold, vibration_acceleration_threshold):

    # 获取当前时间戳之前12小时内的数据
    data_df = get_data_within_12_hours(timestamps_str, conn)

     # 将data_df中的timestamps列转换为datetime对象
    data_df['timestamps'] = pd.to_datetime(data_df['timestamps'], format='%Y%m%d%H%M%S')

    # 找到gas超过阈值的数据点，并取距离当前时间戳最近的数据点
    filtered_data = data_df[data_df['gas_concentration'] >= gas_concentration_threshold]
    if not filtered_data.empty:
        target_timestamp = Timestamp(datetime.strptime(timestamps_str, '%Y%m%d%H%M%S'))
        nearest_index = (filtered_data['timestamps'] - target_timestamp).abs().idxmin()

        # 检查该数据点到现在的所有数据点的vibration_acceleration是否都大于vibration_acceleration_threshold
        time_range_data = data_df[data_df.index >= nearest_index]
        all_above_threshold = (time_range_data['vibration_acceleration'] > vibration_acceleration_threshold).all()
        return all_above_threshold

    return False  # 如果没有gas超过阈值的数据点，则直接返回False


def get_data_within_5_minutes(timestamps_str, conn):

    # 将timestamps_str转换为datetime对象
    timestamps = datetime.strptime(timestamps_str, '%Y%m%d%H%M%S')

    # 计算前五分钟的时间范围
    start_time = timestamps - timedelta(minutes=5)
    end_time = timestamps

    # 将时间范围转换为字符串格式
    start_time_str = start_time.strftime('%Y%m%d%H%M%S')
    end_time_str = end_time.strftime('%Y%m%d%H%M%S')

    # 查询数据的SQL语句
    sql = f"SELECT * FROM security WHERE timestamps >= '{start_time_str}' AND timestamps <= '{end_time_str}';"

    # 使用pandas的read_sql函数读取数据并返回DataFrame
    data_df = pd.read_sql(sql, conn)

    return data_df

#检测5分钟内是否有任何气体超过阈值
def check_gas_concentration_within_5_minutes(timestamps_str, conn, threshold):

    # 获取时间节点五分钟前内的数据
    data_df = get_data_within_5_minutes(timestamps_str, conn)

    # 检查是否有气体浓度超过阈值的数据点
    gas_concentration_exceed_threshold = (data_df['gas_concentration'] > threshold).any()

    return gas_concentration_exceed_threshold

def insert_data_around_timestamp(start_timestamps_str, end_timestamps_str, interval_seconds, vibration_acceleration, gas_concentration, conn):
    # 将起始时间字符串和结束时间字符串转换为datetime对象
    start_timestamps = datetime.strptime(start_timestamps_str, '%Y%m%d%H%M%S')
    end_timestamps = datetime.strptime(end_timestamps_str, '%Y%m%d%H%M%S')

    # 构建插入数据的SQL语句
    sql = "INSERT INTO security (timestamps, vibration_acceleration, gas_concentration) VALUES (%s, %s, %s);"

    # 插入数据
    current_timestamps = start_timestamps
    while current_timestamps <= end_timestamps:
        # 将数据转换为元组
        data_tuple = (current_timestamps.strftime('%Y%m%d%H%M%S'), vibration_acceleration, gas_concentration)

        # 查询数据库是否已经存在相同timestamps的数据
        with conn.cursor() as cursor:
            query_sql = f"SELECT * FROM security WHERE timestamps = '{current_timestamps.strftime('%Y%m%d%H%M%S')}';"
            cursor.execute(query_sql)
            existing_data = cursor.fetchone()

        # 如果数据库中没有相同timestamps的数据，则进行插入
        if not existing_data:
            with conn.cursor() as cursor:
                cursor.execute(sql, data_tuple)
                conn.commit()

        # 增加时间间隔
        current_timestamps += timedelta(seconds=interval_seconds)

#given_timestamps, given_vibration_acceleration, given_gas_concentration, gas_concentration_threshold, vibration_acceleration_threshold

@app.route('/api/safety', methods=['GET'])
def main_control():

    host = 'localhost'
    port = 3306
    user = 'root'
    password = 'root'
    database = 'aidong_xuzhou'

    # 创建和数据库服务器的连接
    conn = pymysql.connect(host=host, port=port, user=user, password=password,
                           database=database, charset='utf8')
    
    #insert_data_around_timestamp('20230801153505','20230801163000',5,150,100,conn)
    #1、5个输入
    given_timestamps = request.args.get('time')#'20230801154000'  # 格式为YYYYMMDDHHMMSS
    given_vibration_acceleration = float(request.args.get('vib')) #150
    given_gas_concentration = float(request.args.get('gas')) #100
    gas_concentration_threshold = float(request.args.get('gas_thre')) #350
    vibration_acceleration_threshold = float(request.args.get('vib_thre')) #149
    
    status = 0
    safe_flag = False

    #2、判断气体浓度 3类
    #2.1 正在进行状态
    if given_gas_concentration >= gas_concentration_threshold:
        status = 1  
        safe_flag = check_vibration_acceleration_within_30_minutes(given_timestamps,conn,gas_concentration_threshold,vibration_acceleration_threshold)
    else:
        #2.2 刚刚结束状态
        if check_gas_concentration_within_5_minutes(given_timestamps, conn, given_gas_concentration) == True:
            status = 2
            safe_flag = check_vibration_acceleration_within_30_minutes_6_mins(given_timestamps,conn,gas_concentration_threshold,vibration_acceleration_threshold)
        else:
            #2.3 空闲闲置状态 检测30分钟内是否在作业
            if check_gas_concentration_within_30_minutes(given_timestamps, conn, gas_concentration_threshold) == False:
                #被其他三点覆盖 肯定安全
                status = 3
                safe_flag = True
            else:
                status = 4
                safe_flag = check_vibration_acceleration_within_time_range(given_timestamps, conn, gas_concentration_threshold,vibration_acceleration_threshold)
                
    print(f"status: {status}, flag: {safe_flag}")

    safe_flag = bool(safe_flag)

    conn.close()
    response_data = {
        "status": status,
        "safe_flag": safe_flag
    }
    return jsonify(response_data)

#main_control('20230801154000', 150, 100 , 350 ,149)

#数据插入20230801143000-20230801153000 间隔5s
def insert_data_around_timestamp(start_timestamps_str, end_timestamps_str, interval_seconds, vibration_acceleration, gas_concentration, conn):
    # 将起始时间字符串和结束时间字符串转换为datetime对象
    start_timestamps = datetime.strptime(start_timestamps_str, '%Y%m%d%H%M%S')
    end_timestamps = datetime.strptime(end_timestamps_str, '%Y%m%d%H%M%S')

    # 构建插入数据的SQL语句
    sql = "INSERT INTO security (timestamps, vibration_acceleration, gas_concentration) VALUES (%s, %s, %s);"

    # 插入数据
    current_timestamps = start_timestamps
    while current_timestamps <= end_timestamps:
        # 将数据转换为元组
        data_tuple = (current_timestamps.strftime('%Y%m%d%H%M%S'), vibration_acceleration, gas_concentration)

        # 查询数据库是否已经存在相同timestamps的数据
        with conn.cursor() as cursor:
            query_sql = f"SELECT * FROM security WHERE timestamps = '{current_timestamps.strftime('%Y%m%d%H%M%S')}';"
            cursor.execute(query_sql)
            existing_data = cursor.fetchone()

        # 如果数据库中没有相同timestamps的数据，则进行插入
        if not existing_data:
            with conn.cursor() as cursor:
                cursor.execute(sql, data_tuple)
                conn.commit()

        # 增加时间间隔
        current_timestamps += timedelta(seconds=interval_seconds)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080)

'''
    start_timestamps = datetime.strptime(given_timestamps, '%Y%m%d%H%M%S') - timedelta(minutes=30)
    end_timestamps = datetime.strptime(given_timestamps, '%Y%m%d%H%M%S') + timedelta(minutes=30)

    # 插入前后30分钟的数据，每5秒一个间隔
    interval_seconds = 5
    vibration_acceleration = 150
    gas_concentration = 350
    insert_data_around_timestamp(start_timestamps.strftime('%Y%m%d%H%M%S'), end_timestamps.strftime('%Y%m%d%H%M%S'), interval_seconds, vibration_acceleration, gas_concentration, conn)
'''
