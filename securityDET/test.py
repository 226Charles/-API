import pandas as pd
from datetime import datetime, timedelta
import pymysql
from flask import Flask
from flask_cors import CORS
from flask_jwt_extended import JWTManager, jwt_required, create_access_token, get_jwt_identity
from pandas import Timestamp
from pandas import to_datetime
app = Flask(__name__)

# 使通过jsonify返回的中文显示正常，否则显示为ASCII码
app.config["JSON_AS_ASCII"] = False

@app.route('/detect_gas', methods=['GET'])
def detect_gas():

    return {'result': True}

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080)