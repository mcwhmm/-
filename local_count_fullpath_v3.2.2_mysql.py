"""
$after日志格式
                              0                 1                       2          3                             4                        5                 6            7     8      9
                            stage              uuid                    system     account                      interface             start_time          end_time      time   size    ip
[2019-07-22 00:00:00,919][INFO]$after||8a9894886c0ba3ab016c1542d6df49f5||CMS||boce_s@nm.cmcc||queryNotificationsBizInfoExp||2019-07-22 00:00:00||2019-07-22 00:00:00||5ms||0.0kb||10.24.20.8


['[2019-12-11 06:32:24,292][INFO]$after', '0.0', '04569ded-10cb-439a-b514-0cc7c0a1fe3c', 'CMS', 'huangwanjie_cd@sc.cmcc', 'findActivityInstByActivityInstID', '10.24.18.14, 10.24.20.238', '2019-12-11 06:32:24', '2019-12-11 06:32:24', '8ms', '0.0kb', '10.24.20.1', '10.24.18.14, 10.24.20.238']


$before日志格式

ip_strip[0]: [2019-12-23 14:55:04,881][INFO]$before ip_strip[1]: 0.0 ip_strip[2]: 0e156d7c-5a99-4bf2-b53d-537909ec4d52 ip_strip[3]: null ip_strip[4]: null ip_strip[5]: getParticipantsByActivityDefIdByOrgCode ip_strip[6]: {"userID":"luoxiao_dy@sc.cmcc","activityDefId":"manualActivity8","pmBusinessCode":"","processDefId":"857162","pmMechanismCode":"","pmApprovalMechanismCode":""} ip_strip[7]: 2019-12-23 14:55:04 ip_strip[8]: 10.24.20.5
"""
# -*- coding: utf-8 -*-

import pandas as pd
from openpyxl import Workbook
import os, time, sys, datetime, gc
import sqlalchemy
from insert_mysql import *
from functools import singledispatch

directory_name = 'all_great_2_second'
# directory_name = datetime.datetime.now().strftime('%Y-%m-%d')
directory = 'F:\\流程平台日志分析\\interfaceDetail\\20191223\\RESULT\\'
path = directory + directory_name + '\\'



# 测试EXCEL是未并闭
wb = Workbook()
wb.save('F:\流程平台日志分析\interfaceDetail_result\\' + directory_name + '.xlsx')
tenant_id = set()


class Analysis(object):

    origin_data = pd.DataFrame()
    converted_obj = pd.DataFrame()
    data = pd.DataFrame()


    list_after = []
    list_before = []

    def __init__(self):
        for dirpath, dirnames, filenames in os.walk(path):
            for file in filenames:
                file_name = os.path.join(dirpath, file)
                print(file_name)
                with open(file_name, 'r', encoding='utf-8') as f:  # windows下用gbk，linux下用utf-8
                    for ip in f:
                        if '$after' in ip:
                            ip = ip.strip('\n')  # 去掉IP后面的回车
                            ip_strip = ip.split('||')
                            if ip != '\n' and len(ip_strip) > 3:
                                #print(ip_strip)
                                if 'ms' not in ip_strip[9]:  # ip_strip[7]不是含MS时间记录时有报错，暂时跳过
                                    continue
                                elapsed = float(ip_strip[9].strip('ms')) / 1000  # 计算接口调用时长，转换为秒
                                size = ip_strip[10].strip('kb')
                                second = round(elapsed, 0)
                                ##############取消MYSQL入库时业务系统及省分中文件转换###############
                                # for key, value in province.items():  # 根据账号将省分编码转换为中文
                                #     if key == ip_strip[4][-8:-5]:
                                #         province_name = value
                                #         break
                                #     else:
                                #         province_name = 'NULL'
                                # for system_key, system_value in system_code.items():  # 转换系统代码为中文
                                #     if system_key.lower() == ip_strip[3].lower():
                                #         system_name = system_value
                                province_name = ip_strip[4][-8:-5]
                                system_name = ip_strip[3].lower()
                                src_ip = ip_strip[12].split(',')[0]
                                line_dict_after = {'uuid': ip_strip[2], 'system': system_name, 'account': ip_strip[4],
                                             'interface': ip_strip[5], 'consuming': int(second), 'time':second, 'real elapsed':elapsed,
                                             'size': size, 'ip': ip_strip[11], 'source_ip':src_ip,
                                             'minute': ip_strip[7][:-3], 'province': province_name, 'count':'count'}
                                            #'start_time': ip_strip[5], 'end_time': ip_strip[6],从列表中取出
                                #print(line_dict_after)
                                self.list_after.append(line_dict_after)

                        if '$before' in ip:
                            ip = ip.strip('\n')  # 去掉IP后面的回车
                            ip_strip = ip.split('||')
                            # print('ip_strip[0]:',ip_strip[0],'ip_strip[1]:',ip_strip[1],'ip_strip[2]:',ip_strip[2],'ip_strip[3]:',ip_strip[3],'ip_strip[4]:',ip_strip[4],'ip_strip[5]:',ip_strip[5],'ip_strip[6]:',ip_strip[6],'ip_strip[7]:',ip_strip[7],'ip_strip[8]:',ip_strip[8])
                            # print(ip_strip[2])
                            line_dict_before = {'uuid':ip_strip[2], 'interface':ip_strip[5],'parameter':ip_strip[6],'minute':ip_strip[7],'ip':ip_strip[8]}
                            self.list_before.append(line_dict_before)

                self.type_conversion(self.list_after,'after日志list')

    def type_conversion(self, list, log_type):
        # print('-------------将' + log_type + '转换为DataFrame-------------')
        self.data = pd.DataFrame(list)
        # print('共计', self.data.shape[0], '条数据')
        # print('list转换后的dataframe大小:', self.mem_usage(self.data))
        # print('-------------将data中的object转换为categories类型-------------')
        # print('before converted_obj:\n', len(self.converted_obj))
        self.trans_categories = self.save_obj(self.data, self.converted_obj)
        # print('类型转换后的trans_categories大小：', self.mem_usage(self.trans_categories))
        self.frames = [self.origin_data, self.trans_categories]
        self.origin_data = pd.concat(self.frames)
        # print('合并后的origin_data大小', self.mem_usage(self.origin_data))
        # print('转换后数据类型：\n', self.frames[0].dtypes)
        # print('合并后数据类型：\n', self.origin_data.dtypes)
        # print('合并后的origin_data数据量：', len(self.origin_data))
        # print('-------------清空data及converted_obj-------------')
        self.data = pd.DataFrame()
        self.converted_obj = pd.DataFrame()
        # print('-------------清空' + log_type + '-------------')
        self.list_after = []

        print('=================================================================================================')

    def save_obj(self, pandas_obj,converted_obj):
        #converted_obj = pd.DataFrame()
        for col in pandas_obj.columns:
            converted_obj.loc[:, col] = pandas_obj[col].astype('category')
            # num_unique_values = len(pandas_obj[col].unique())
            # num_total_values = len(pandas_obj[col])
            # if num_unique_values / num_total_values < 0.5:
            #     converted_obj.loc[:, col] = pandas_obj[col].astype('category')
            # else:
            #     converted_obj.loc[:, col] = pandas_obj[col]
        return converted_obj

    def mem_usage(self, pandas_obj):
        if isinstance(pandas_obj, pd.DataFrame):
            usage_b = pandas_obj.memory_usage(deep=True).sum()
        else:
            usage_b = pandas_obj.memory_usage(deep=True)
        usage_mb = usage_b / 1024 ** 2
        return "{:03.2f} MB".format(usage_mb)


analysis = Analysis()
writer_excle = pd.ExcelWriter('F:\流程平台日志分析\interfaceDetail_result\\' + directory_name + '.xlsx')
total_starttime = datetime.datetime.now()

print(analysis.origin_data.info(memory_usage='deep'))



#计算按分钟统计系统并发量
starttime = datetime.datetime.now()
pivot_result_excel = pd.pivot_table(analysis.origin_data, index=['minute'], columns=['system'], values=['uuid'],
                              aggfunc='count', margins=True)
#写入EXCEL
try:
    pivot_result_excel.to_excel(writer_excle, sheet_name='按分钟统计系统并发量')
except Exception as e:
    pass
    print('按分钟统计系统并发量已完成')
del pivot_result_excel



#####################################################分析数据写入MYSQL#####################################################
#根据分钟统计业务系统的并发量

pivot_result = pd.pivot_table(analysis.origin_data, index=['minute','system'], columns=['count'], values=['uuid'],aggfunc='count', margins=True)
try:
    pivot_result.columns = pivot_result.columns.droplevel(level=0)   #去掉透视表第一行，本例为uuid行 参考：https://blog.csdn.net/weixin_43474731/article/details/100186154
    pivot_result = pivot_result.drop(columns=['All'],index=['All'])  #去掉All行及列 参考：https://blog.csdn.net/songyunli1111/article/details/79306639
    pivot_result['date'] = datetime.datetime.now() #增加数据入库时间列
    pivot_result = pivot_result.reset_index()   #重置索引
    pivot_result['minute'] = pivot_result['minute'].apply(pd.to_datetime, format='%Y-%m-%d %H:%M:%S')    # object转datatime,参考：https://blog.csdn.net/sharkandshark/article/details/84317375
    print('===========')
    print(pivot_result['minute'].dtypes)
    print('===========')
    #pivot_result = pivot_result.drop(index=['All'])

    pivot_result = pivot_result.fillna(0)
    dtypedict = mapping_df_types(pivot_result)  #自动类型转换 参考：https://www.jianshu.com/p/4c5e1ebe8470?utm_source=oschina-app
    pivot_result.to_sql(name='concurrency_by_minute',con=engine,if_exists='replace',index=True,
                  dtype=dtypedict)
    print("mysql write success!!")
except Exception as e:
    print(e)
endtime = datetime.datetime.now()
print(endtime - starttime)
del pivot_result
gc.collect()



#
# # 按省分统计系统调用量
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['province'], columns=['system'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='按系统统计系统调用量')
# print('按系统统计系统调用量已完成')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# del pivot_result
# gc.collect()
#
# # 按省分统计调用时长-20190727新增
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['province'], columns=['system'], values=['consuming'],
#                               aggfunc='sum', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='按省分统计调用时长')
# print('按省分统计调用时长')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# del pivot_result
# gc.collect()
#
#
# #按系统接口调用时长
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['interface'], columns=['system'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='按省分统计接口调用量')
# print('按省分统计接口调用量已完成')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# del pivot_result
# gc.collect()
#
#
# # 所有接口分省统计
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['interface'], columns=['province'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='所有接口分省统计')
# print('所有接口分省统计完成')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# del pivot_result
# gc.collect()
#
#
# # 所有接口分系统统计开销--20190727新增
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['interface'], columns=['system'], values=['consuming'],
#                               aggfunc='sum', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='所有接口分系统统计开销')
# print('所有接口分系统统计开销')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# del pivot_result
# gc.collect()
#
#
# # 按查询接口分省统计
# starttime = datetime.datetime.now()
# query_interface = analysis.origin_data.query(
#     'interface == ["queryPersonWorkItemsWithBizInfoExp","queryPersonFinishedWorkItemsWithBizInfoExp","queryNotificationsBizInfoExp","queryNotificationsBizInfoFinishExp"]')
# pivot_result = pd.pivot_table(query_interface, index=['interface'], columns=['province'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='按查询接口分省统计')
# print('按查询接口分省统计完成')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# del pivot_result
# gc.collect()
#
# #分接口分秒统计调用时长
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['interface'], columns=['consuming'], values=['uuid'],
#                               aggfunc='count', margins=True)
# #print(pivot_result)
# total_value = pivot_result.iat[-1,-1]  #取第All列，ALL行数据值
# print('total_value',total_value)
# pivot_result['百分比'] = pivot_result['uuid']['All'].map(lambda x : x/total_value*100) #All列除以第All列，ALL行数据值，计算占用百分比
# pivot_result.to_excel(writer_excle, sheet_name='分接口分秒统计调用时长')
# print('分接口分秒统计调用时长')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# gc.collect()
#
#
# #分接口分秒统计调用时长分系统
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['interface','system'], columns=['consuming'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='分接口分秒统计调用时长分系统')
# total_value = pivot_result.iat[-1,-1]
# print('total_value',total_value)
# pivot_result['百分比'] = pivot_result['uuid']['All'].map(lambda x : x/total_value*100)
# pivot_result.to_excel(writer_excle, sheet_name='分接口分秒统计调用时长分系统')
# print('分接口分秒统计调用时长分系统')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# gc.collect()
#
#
# #分接口统计时间开销--20190727新增
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['interface'], columns=['consuming'], values=['time'],
#                               aggfunc='sum', margins=True)
# total_value = pivot_result.iat[-1,-1]
# print('total_value',total_value)
# pivot_result['百分比'] = pivot_result['time']['All'].map(lambda x : x/total_value*100)
# pivot_result.to_excel(writer_excle, sheet_name='分接口统计时间开销')
# print('分接口统计时间开销')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# gc.collect()
#
# #分省统计接口开销--20190727新增
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['interface'], columns=['province'], values=['consuming'],
#                               aggfunc='sum', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='分省统计接口开销')
# print('分省统计接口开销')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
# gc.collect()
#
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['ip'], columns=['system'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='根据IP分系统统计时间调用量')
# print('根据IP分系统统计时间调用量')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
#
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['interface'], columns=['ip'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='根据IP分接口统计时间调用量')
# print('根据IP分接口统计时间调用量')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
#
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['ip'], columns=['province'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='根据IP分省统计时间调用量')
# print('根据IP分省统计时间调用量')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
#
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['minute'], columns=['ip'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='根据时间统计每台服务器调用量')
# print('根据时间统计每台服务器调用量')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
#
# # 按源IP地址统计调用量
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['source_ip'], columns=['province'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='按源IP地址统计调用量')
# print('按源IP地址统计调用量')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
#
# # 按源IP地址统计各省调用量
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['source_ip'], columns=['province'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='按源IP地址统计各省调用量')
# print('按源IP地址统计各省调用量')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
#
# # 按源IP地址统计业务系统调用量
# starttime = datetime.datetime.now()
# pivot_result = pd.pivot_table(analysis.origin_data, index=['source_ip'], columns=['system'], values=['uuid'],
#                               aggfunc='count', margins=True)
# pivot_result.to_excel(writer_excle, sheet_name='按源IP地址统计业务系统调用量')
# print('按源IP地址统计业务系统调用量')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
#
# #统计3秒以上响应时间接口
# starttime = datetime.datetime.now()
# #pivot_result=analysis.origin_data[analysis.origin_data['account'].isin('34020017@ah.cmcc')] #按appitedid筛选结果appitedid
# query_interface = analysis.origin_data.query('consuming > 3')
# pivot_result = pd.pivot_table(query_interface, index=['interface','system'], columns=['consuming'], values=['uuid'],
#                               aggfunc='count', margins=True)
# total_value = pivot_result.iat[-1,-1]
# print('total_value',total_value)
# pivot_result['百分比'] = pivot_result['uuid']['All'].map(lambda x : x/total_value*100)
# pivot_result.to_excel(writer_excle,sheet_name='统计3秒以上接口响应时间')
# print('统计3秒以上接口响应时间')
# endtime = datetime.datetime.now()
# print(endtime - starttime)
#
# starttime = datetime.datetime.now()
# #pivot_result=analysis.origin_data[analysis.origin_data['account'].isin('34020017@ah.cmcc')] #按appitedid筛选结果appitedid
# query_interface = analysis.origin_data.query('consuming > 3')
# query_interface.to_excel(writer_excle,sheet_name='筛选后的原始数据',startcol=0,index=False)
# print('筛选后的原始数据')
# endtime = datetime.datetime.now()
# print(endtime - starttime)

# del pivot_result
# gc.collect()
# total_endtime = datetime.datetime.now()
# print('共用时',total_endtime - total_starttime)

print('正在写入，请等待......')
writer_excle.close()
print('已完成')
print('F:\流程平台日志分析\interfaceDetail_result\\' + directory_name + '.xlsx')
