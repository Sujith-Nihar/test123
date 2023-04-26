import boto3
import pandas as pd
import pickle
from io import StringIO
from datetime import datetime
import pytz
import datetime as DT

s3 = boto3.resource(service_name = 's3',region_name='us-east-1',aws_access_key_id='AKIAUZGDREQUJ33NDU5Y',aws_secret_access_key='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')
s3csv = boto3.client('s3', 
 region_name = 'us-east-1',
 aws_access_key_id = 'AKIAUZGDREQUJ33NDU5Y',
 aws_secret_access_key = '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'
) 

utc = pytz.utc



testdate = '2013-03-27 23:01'
test2 = DT.datetime.strptime(testdate, '%Y-%m-%d %H:%M')

final_date = utc.localize(test2)
map1 = {1:"Heart Attack",0:"No heart attack"}
first = 0
while True:
    obj1 = s3.Object('aws-model1', 'heartattackcollect.csv')
    curr = obj1.last_modified
    if final_date < curr:
        obj = s3.Bucket('aws-model1').Object('heartattackcollect.csv').get()
        data = pd.read_csv(obj['Body'])
        print(data)
        d_m1 = data[['age','sex','cp','trtbps','chol','fbs','restecg','O2 saturation','slp','thall']]
        print(d_m1)
        # d_m1.to_numpy()
        load_model1 = pickle.load(open('finalized_model.sav', 'rb'))
        y_pred1 = load_model1.predict(d_m1)
        print(y_pred1)
        d_m1['prediction']= y_pred1
        print(d_m1)

        final_date = curr
        csv_buffer1=StringIO()
        
        if first == 0:
            d_m1.to_csv(csv_buffer1, index=False)
            response1=s3csv.put_object(Body=csv_buffer1.getvalue(),
                            Bucket='aws-model1',
                            Key='heartattackprediction.csv')
            first = 1
        else:
            obj3 = s3.Bucket('aws-model1').Object('heartattackprediction.csv').get()
            data = pd.read_csv(obj3['Body'])
            vertical_concat = pd.concat([data, d_m1], axis=0)
            vertical_concat.to_csv(csv_buffer1, index=False)
            response1=s3csv.put_object(Body=csv_buffer1.getvalue(),
                            Bucket='aws-model1',
                            Key='heartattackprediction.csv')



# print(obj1.last_modified > final_date)
# print(obj1.last_modified)
# print(final_date)

# while true:

#     obj1 = s3.Object('aws-model1', 'test1.csv')






# d_m1 = data[['age','sex','cp','trtbps','chol','fbs','restecg','O2 saturation','slp','thall']]
# # d_m2 = data[['LVEF(%)','lvidd','Pulmonary Valve Peak velocity','lvids','Aortic Valver peak velocity(m/sec)']]

# print(d_m1)
# # print(d_m2)

# # d_m1.to_numpy()
# # d_m2.to_numpy()
# load_model1 = pickle.load(open('finalized_model.sav', 'rb'))
# load_model2 = pickle.load(open('model2.sav', 'rb')) 

# y_pred1 = load_model1.predict(d_m1)
# y_pred2 = load_model2.predict(d_m2)

# map1 = {1:"Heart Attack",0:"No heart attack"}
# map2 = {3:'Severe dysfunction',2:'Moderate dysfunction',1:'Mild dysfunction',0:'Normal'}
# d_m1['prediction']=map1[int(y_pred1)]
# d_m2['prediction']=map2[int(y_pred2)]
# #print(d_m1)
# #print(d_m2)

# BUCKET_NAME = 'aws-model1'
# FileName = 'pysparks3/emp.csv'
# csv_buffer1=StringIO()
# csv_buffer2=StringIO()
# d_m1.to_csv(csv_buffer1, index=False)
# d_m2.to_csv(csv_buffer2, index=False)




# response2=s3csv.put_object(Body=csv_buffer1.getvalue(),
#                            Bucket=BUCKET_NAME,
#                            Key='output/dysfunctionPrediction/dysfunction.csv')