from collections import deque

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
