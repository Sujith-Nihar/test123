import boto3
import pandas as pd
import pickle
from io import StringIO
from datetime import datetime
import pytz
import datetime as DT
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler

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
second = 0
third = 0
fourth = 0
fifth = 0


while True:
    obj1 = s3.Object('aws-model1', 'All_data.csv')
    curr = obj1.last_modified
    if final_date < curr:
        obj = s3.Bucket('aws-model1').Object('All_data.csv').get()
        data = pd.read_csv(obj['Body'])
        print(data)
        d_m1 = data[['age','sex','cp','trtbps','chol','fbs','restecg','O2 saturation','slp','thall']]
        d_m2 = data[['LVEF(%)','lvidd','Pulmonary Valve Peak velocity','lvids','Aortic Valver peak velocity(m/sec)']]
        d_m33 = data[['Inverter std dev','Inverter averages','RTU: ava. Setpoint 1','Transformer temp.','Control cabinet temp.','Tower temp.','Ambient temp.','Fan inverter cabinet temp.','Yaw inverter cabinet temp.','Rectifier cabinet temp.','Main carrier temp.','Nacelle cabinet temp.','Nacelle temp.','Nacelle ambient temp. 2','Nacelle ambient temp. 1','Stator temp. 2','Stator temp. 1','Rotor temp. 2','Rotor temp. 1','Blade C temp.','Blade B temp.','Blade A temp.','Pitch cabinet blade C temp.','Pitch cabinet blade B temp.','Pitch cabinet blade A temp.','Rear bearing temp.','Front bearing temp.','Spinner temp.','Sys 2 inverter 7 cabinet temp.','Sys 2 inverter 6 cabinet temp.','Sys 2 inverter 5 cabinet temp.','Sys 2 inverter 4 cabinet temp.','Sys 2 inverter 3 cabinet temp.','Sys 2 inverter 2 cabinet temp.','Sys 2 inverter 1 cabinet temp.','Sys 1 inverter 7 cabinet temp.','Sys 1 inverter 6 cabinet temp.','Sys 1 inverter 5 cabinet temp.','Sys 1 inverter 4 cabinet temp.','Sys 1 inverter 3 cabinet temp.','Sys 1 inverter 2 cabinet temp.','Sys 1 inverter 1 cabinet temp.','WEC: ava. blade angle A','WEC: min. reactive Power','WEC: max. reactive Power','WEC: ava. reactive Power','WEC: ava. Nacel position including cable twisting','WEC: min. Power','WEC: max. Power','WEC: ava. Power','WEC: min. Rotation','WEC: max. Rotation','WEC: ava. Rotation']]
        d_m3=data[['WEC: ava. Rotation', 'WEC: max. Rotation', 'WEC: min. Rotation',
       'WEC: ava. Power', 'WEC: max. Power', 'WEC: min. Power',
       'WEC: ava. Nacel position including cable twisting',
       'WEC: ava. reactive Power', 'WEC: max. reactive Power','WEC: min. reactive Power', 'WEC: ava. blade angle A',''
       'Sys 1 inverter 1 cabinet temp.', 'Sys 1 inverter 2 cabinet temp.',
       'Sys 1 inverter 3 cabinet temp.', 'Sys 1 inverter 4 cabinet temp.',
       'Sys 1 inverter 5 cabinet temp.', 'Sys 1 inverter 6 cabinet temp.',
       'Sys 1 inverter 7 cabinet temp.', 'Sys 2 inverter 1 cabinet temp.',
       'Sys 2 inverter 2 cabinet temp.', 'Sys 2 inverter 3 cabinet temp.',
       'Sys 2 inverter 4 cabinet temp.', 'Sys 2 inverter 5 cabinet temp.',
       'Sys 2 inverter 6 cabinet temp.', 'Sys 2 inverter 7 cabinet temp.',
       'Spinner temp.', 'Front bearing temp.', 'Rear bearing temp.',
       'Pitch cabinet blade A temp.', 'Pitch cabinet blade B temp.',
       'Pitch cabinet blade C temp.', 'Blade A temp.', 'Blade B temp.',
       'Blade C temp.', 'Rotor temp. 1', 'Rotor temp. 2', 'Stator temp. 1',
       'Stator temp. 2', 'Nacelle ambient temp. 1', 'Nacelle ambient temp. 2',
       'Nacelle temp.', 'Nacelle cabinet temp.', 'Main carrier temp.',
       'Rectifier cabinet temp.', 'Yaw inverter cabinet temp.',
       'Fan inverter cabinet temp.', 'Ambient temp.', 'Tower temp.',
       'Control cabinet temp.', 'Transformer temp.', 'RTU: ava. Setpoint 1',
       'Inverter averages', 'Inverter std dev']]
        d_m4 = data[['date','device','metric1','metric2','metric3','metric4','metric5','metric6','metric7','metric8','metric9']]

        ogdate = pd.to_datetime('1/1/2015')
        d_m4.date = pd.to_datetime(d_m4.date)

        d_m4['activedays']=d_m4.date-ogdate

        d_m4['month']=d_m4['date'].dt.month
        d_m4['week_day']=d_m4.date.dt.weekday
        d_m4['week_day'].replace(0,7,inplace=True)

        d_m4_date = d_m4.groupby('device').agg({'date':max})

        d_m4_date.date.to_dict()

        d_m4['max_date']=d_m4.device.map(d_m4_date.date.to_dict())

        d_m4

        d_m4.metric1.nunique()
        d_m41 = d_m4.groupby('device').agg({'date':max})
        d_m41=d_m41.reset_index()

        d_m4=d_m4.reset_index(drop=True) 

        d_m42= pd.merge(d_m41,d_m4,how='left',on=['device','date'])

        d_m42['failure_before']=0
        d_m42.loc[d_m42.device == 'S1F136J0','failure_before'] = 1
        d_m42.loc[d_m42.device == 'W1F0KCP2','failure_before'] = 1
        d_m42.loc[d_m42.device == 'W1F0M35B','failure_before'] = 1
        d_m42.loc[d_m42.device == 'S1F0GPFZ','failure_before'] = 1
        d_m42.loc[d_m42.device == 'W1F11ZG9','failure_before'] = 1
        Id = d_m42.device.values.tolist()

        Id1 = [] 
        for i in Id:
            i = i[:4]
            Id1.append(i)

        d_m42.device=Id1

        def str_to_num(str):
            return str.split(' ')[0]

        d_m42.activedays = d_m42.activedays.astype('str')

        d_m42.activedays=d_m42.activedays.apply(str_to_num)
        d_m42.activedays = d_m42.activedays.astype('int')
        d_m42.info()

        scaler = StandardScaler()

        num_ftrs =['metric1','metric2','metric6','metric3','metric4', 'metric5', 'metric7', 'metric9'] 
        d_m42[num_ftrs]=scaler.fit_transform(d_m42[num_ftrs])

        d_m4.drop('metric8',axis=1,inplace=True)
        d_m42.drop(['date','max_date'],axis=1,inplace=True)
        d_m42.drop('metric8',axis=1,inplace=True)

        file = open("saved_ohe (1).pkl",'rb')
        ohe = pickle.load(file)
        encoder_d_m4 = pd.DataFrame(ohe.transform(d_m42[['device']]).toarray())

        #merge one-hot encoded columns back with original DataFrame
        final_d_m4 = d_m42.join(encoder_d_m4)

        #view final d_m4
        print(final_d_m4)

        final_d_m4.drop(['device'], axis=1, inplace=True)
        final_d_m4.columns = final_d_m4.columns.astype(str)
        d_m42=final_d_m4

        


        d_m5 = data[['unit_number','time_cycles','setting_1','setting_2','setting_3','s_1','s_2','s_3','s_4','s_5','s_6','s_7','s_8','s_9','s_10','s_11','s_12','s_13','s_14','s_15','s_16','s_17','s_18','s_19','s_20','s_21']]
        print(d_m1)

        index_names = ['unit_number', 'time_cycles']
        setting_names = ['setting_1', 'setting_2', 'setting_3']
        sensor_names = ['s_{}'.format(i+1) for i in range(0,21)]
        col_names = index_names + setting_names + sensor_names
        train = d_m5
        #dfload = pd.read_csv('nasa_train.csv')

        drop_labels = index_names+setting_names
        X_train=train.drop(columns=drop_labels).copy()

        """##### Scaling the data"""

        
        scaler = MinMaxScaler()
        X_train_s=scaler.fit_transform(X_train)

        drop_labels2=['s_1', 's_5','s_6','s_10',  's_16', 's_18', 's_19']
        X_train_2=X_train.drop(columns=drop_labels2, axis=1) # drop the constant columns from the train dataset
        X_train_2_s=scaler.fit_transform(X_train_2) #scaling X_train_2

        df=train.copy()
        for x in X_train_2.columns:
            df[x+'_rm']=0

        df=df.drop(columns=setting_names+drop_labels2, axis=1)

        def update_rolling_mean(data, mask):
            for x, group in mask.groupby("unit_number"):
                for x in X_train_2.columns:
                    data.loc[group.index[10:], x+"_rm"] = data.loc[group.index, x].rolling(10).mean()[10:]
                    data.loc[group.index[:10], x+"_rm"] = data.loc[group.index[:10], x]

        update_rolling_mean(df, df)



        """dealing with last line problem"""

        df.iloc[-1,-14:]=df.iloc[-2,-14:]

        train_tm=df

        train_tm=train_tm.drop(columns=index_names, axis=1)

        X_train_tm_s=scaler.fit_transform(train_tm)
        



        # drop_labels2=['setting_1','setting_2','setting_3','s_1', 's_5','s_6','s_10',  's_16', 's_18', 's_19']
        # X_train_2=d_m5.drop(columns=drop_labels2, axis=1)
        # for x in X_train_2.columns:
        #     d_m5[x+'_rm']=0



        # d_m1.to_numpy()
        load_model1 = pickle.load(open('finalized_model.sav', 'rb'))
        load_model2 = pickle.load(open('model2.sav', 'rb'))
        load_model3 = pickle.load(open('model.pkl', 'rb'))
        load_model4 = pickle.load(open('saved_model.pkl', 'rb'))
        regressor = pickle.load(open('final_regressor.sav', 'rb'))
        # load_model5 = pickle.load(open('.sav', 'rb'))

        y_pred1 = load_model1.predict(d_m1)
        y_pred2 = load_model2.predict(d_m2)
        y_pred3 = load_model3.predict(d_m3)
        y_pred4 = load_model4.predict(d_m42)
        test = regressor.predict(pd.DataFrame(X_train_tm_s))
        
        # y_pred5 = load_model5.predict(d_m5)

        # pred_m = pd.DataFrame(y_pred4, columns = ['prediction4'])
        
        d_m1['prediction1']= y_pred1
        d_m2['prediction2']= y_pred2
        d_m3['prediction3']= y_pred3
        d_m42['prediction4']= y_pred4
        d_m5['prediction5']= test

        d_m1['Patient Id']= data['Patient Id']
        d_m2['Patient Id']= data['Patient Id']

        print(test)

        print("prediction for model 4",type(y_pred4))

        # print(d_m1)

        final_date = curr
        csv_buffer1=StringIO()
        csv_buffer2=StringIO()
        csv_buffer3=StringIO()
        csv_buffer4=StringIO()
        csv_buffer5=StringIO()
        
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
            
        
        
        if second == 0:
            d_m2.to_csv(csv_buffer2, index=False)

            response1=s3csv.put_object(Body=csv_buffer2.getvalue(),
                            Bucket='aws-model1',
                            Key='dysfunctionprediction.csv')
            second = 1
        else:
            obj4 = s3.Bucket('aws-model1').Object('dysfunctionprediction.csv').get()
            data = pd.read_csv(obj4['Body'])
            vertical_concat = pd.concat([data, d_m2], axis=0)
            vertical_concat.to_csv(csv_buffer2, index=False)
            response1=s3csv.put_object(Body=csv_buffer2.getvalue(),
                            Bucket='aws-model1',
                            Key='dysfunctionprediction.csv')
            
        
        if third == 0:
            d_m3.to_csv(csv_buffer3, index=False)

            response1=s3csv.put_object(Body=csv_buffer3.getvalue(),
                            Bucket='aws-model1',
                            Key='windmillprediction.csv')
            third = 0
        else:
            obj4 = s3.Bucket('aws-model1').Object('windmillprediction.csv').get()
            data = pd.read_csv(obj4['Body'])
            vertical_concat = pd.concat([data, d_m3], axis=0)
            vertical_concat.to_csv(csv_buffer3, index=False)
            response1=s3csv.put_object(Body=csv_buffer3.getvalue(),
                            Bucket='aws-model1',
                            Key='windmillprediction.csv')
            
        
        if fourth == 0:
            d_m42.to_csv(csv_buffer4, index=False)

            response1=s3csv.put_object(Body=csv_buffer4.getvalue(),
                            Bucket='aws-model1',
                            Key='predictivemaintainprediction.csv')
            fourth = 0
        else:
            obj4 = s3.Bucket('aws-model1').Object('predictivemaintainprediction.csv').get()
            data = pd.read_csv(obj4['Body'])
            vertical_concat = pd.concat([data, d_m42], axis=0)
            vertical_concat.to_csv(csv_buffer4, index=False)
            response1=s3csv.put_object(Body=csv_buffer4.getvalue(),
                            Bucket='aws-model1',
                            Key='predictivemaintainprediction.csv')

        if fifth == 0:
            d_m5.to_csv(csv_buffer5, index=False)

            response1=s3csv.put_object(Body=csv_buffer5.getvalue(),
                            Bucket='aws-model1',
                            Key='NASAprediction.csv')
            fifth = 0
        else:
            obj4 = s3.Bucket('aws-model1').Object('NASAprediction.csv').get()
            data = pd.read_csv(obj4['Body'])
            vertical_concat = pd.concat([data, d_m5], axis=0)
            vertical_concat.to_csv(csv_buffer5, index=False)
            response1=s3csv.put_object(Body=csv_buffer5.getvalue(),
                            Bucket='aws-model1',
                            Key='NASAprediction.csv')

        






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