from configparser import ConfigParser
import sqlalchemy
from sqlalchemy.types import NVARCHAR, Float, Integer

#读取配置文件
cp = ConfigParser()
cp.read('analysis.conf')

host = cp.get("mysql", "db_host")
port = cp.getint("mysql", "db_port")
user = cp.get("mysql", "db_user")
password = cp.get("mysql", "db_password")
database = cp.get("mysql", "db_database")

#engine = sqlalchemy.create_engine("mysql+pymysql://root:1qaz!QAZ@127.0.0.1:3306/analysis?charset=utf8mb4")
engine = sqlalchemy.create_engine("mysql+pymysql://"+user+":"+password+"@"+host+":%d"%(port)+"/"+database+"?charset=utf8mb4")

#MYSQL入库自动类型转换
def mapping_df_types(df):
    dtypedict = {}
    for i, j in zip(df.columns, df.dtypes):
        if "object" in str(j):
            dtypedict.update({i: NVARCHAR(length=255)})
        if "float" in str(j):
            dtypedict.update({i: Float(precision=7, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: Integer()})
        if "Date" in str(j):
            dtypedict.update({i: DATETIME()})
    return dtypedict
