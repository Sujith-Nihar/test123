import random
import threading
import pandas as pd
import numpy as np
import time
import boto3

from io import StringIO

#Global Variables
all_patient_data_heartattack = []
#all_patient_data_pneumonia= []
#all_patient_data_heartdisease= []


# dataset for model 2 and model 3
s3csv = boto3.client('s3', 
            region_name = 'us-east-1',
            aws_access_key_id = 'AKIAUZGDREQUJ33NDU5Y',
            aws_secret_access_key = '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'
            )

def gen_patient_data():
    global all_patient_data_heartattack
    data = {}
    d = dict(Z1F0QLC1 = 1, S1F0KYCR = 2, S1F0E9EP = 3, S1F0EGMT = 4, S1F0FGBQ=5 )
    dv=random.choice(list(d.keys()))
    t = {1:"1/1/2015",2:"1/2/2015",3:"1/3/2015",4:"1/04/2015", 5:"1/05/2015"}
    dt=random.choice(list(t.values()))

    data["Patient Id"] = str(' '.join(random.choice('0123456789ABCDEF') for i in range(11)))
    data["Machine Id"] = str(' '.join(random.choice('0123456789ABCDEFGHI') for i in range(11)))
    data["unit_number"]=str(random.randrange(1, 100))
    data["time_cycles"]=str(random.randrange(1, 195))
    data["date"]=dt
    data["device"]=dv
    data["age"]=str(random.randrange(70))
    data["sex"]=str(random.randrange(2))
    data["cp"]=str(random.randrange(4))
    data['trtbps']=str(random.randrange(94,201))
    data["chol"]=str(random.randrange(126,408))
    data["fbs"]=str(random.randrange(0,2))
    data["restecg"]=str(random.randrange(0,2))
    data["O2 saturation"]=str(round(random.uniform(90,100),1))
    data["slp"]=str(random.randrange(0,3))
    data["thall"]=str(random.randrange(0,4))
    data["LVEF(%)"]=str(random.randrange(15, 75))
    data["lvidd"]=str(round(random.uniform(2.20, 6.00),2))
    data["Pulmonary Valve Peak velocity"]=str(round(random.uniform(1.00,7.60),2))
    data["lvids"]=str(round(random.uniform(2.00,4.76),2))
    data["Aortic Valver peak velocity(m/sec)"]=str(round(random.uniform(2.00, 8.00),2))
    data["setting_1"]=str(round(random.uniform(-0.0044,0.0044),4))
    data["setting_2"]=str(round(random.uniform(-0.0007,0.0006),4))
    data["setting_3"]=str(100)
    data['s_1']=str(518.67)
    data["s_2"]=str(round(random.uniform(641.00, 643.00),2))
    data["s_3"]=str(round(random.uniform(1581.00, 1589.00),2))
    data["s_4"]=str(round(random.uniform(1392.00,1432.00),2))
    data["s_5"]=str(14.62)
    data["s_6"]=str(21.61)
    data["s_7"]=str(round(random.uniform(553.00,556.00),2))
    data["s_8"]=str(round(random.uniform(2387.00, 2389.00),2))
    data["s_9"]=str(round(random.uniform(9040.00, 9225.00),2))
    data["s_10"]=str(1.3)
    data["s_11"]=str(round(random.uniform(47.00, 49.00),2))
    data["s_12"]=str(round(random.uniform(519.00, 523.99),2))
    data["s_13"]=str(round(random.uniform(2387.00, 2389.00),2))
    data["s_14"]=str(round(random.uniform(8131.00, 8236.00),2))
    data["s_15"]=str(round(random.uniform(8.3600, 8.4600),4))
    data["s_16"]=str(0.03)
    data["s_17"]=str(random.randrange(390, 400))
    data["s_18"]=str(2388)
    data["s_19"]=str(100)
    data["s_20"]=str(round(random.uniform(38.00, 40.00),2))
    data["s_21"]=str(round(random.uniform(23.0000, 23.5000),4))
    data["metric1"]=str(random.randrange(0,244140480))
    data["metric2"]=str(random.randrange(0, 64968))
    data["metric3"]=str(random.randrange(0,24929))
    data["metric4"]=str(random.randrange(0, 166))
    data["metric5"]=str(random.randrange(1, 98))
    data["metric6"]=str(random.randrange(8, 689161))
    data["metric7"]=str(random.randrange(0, 832))
    data["metric8"]=str(random.randrange(0, 832))
    data["metric9"]=str(random.randrange(0, 70000))
    data["WEC: ava. Rotation"]=str(round(random.uniform(0.000000,14.690000),6))
    data["WEC: max. Rotation"]=str(round(random.uniform(0.000000,15.810000),6))
    data["WEC: min. Rotation"]=str(round(random.uniform(0.000000,14.290000),6))
    data["WEC: ava. Power"]=str(round(random.uniform(0.000000,3070.000000),6))
    data["WEC: max. Power"]=str(round(random.uniform(0.000000,3192.000000),6))
    data["WEC: min. Power"]=str(round(random.uniform(0.000000,14.290000),6))
    data["WEC: ava. Nacel position including cable twisting"]=str(round(random.uniform(-782.000000,707.000000),6))
    data["WEC: ava. reactive Power"]=str(round(random.uniform(-3.000000	,314.000000),6))
    data["WEC: max. reactive Power"]=str(round(random.uniform(0.000000,1094.000000),6))
    data["WEC: min. reactive Power"]=str(round(random.uniform(-9.000000	,294.000000),6))
    data["WEC: ava. blade angle A"]=str(round(random.uniform(1.000000,91.989998),6))
    data["Sys 1 inverter 1 cabinet temp."]=str(round(random.uniform(14.000000,42.000000),6))
    data["Sys 1 inverter 2 cabinet temp."]=str(round(random.uniform(15.000000,44.000000),6))
    data["Sys 1 inverter 3 cabinet temp."]=str(round(random.uniform(15.000000,43.000000),6))
    data["Sys 1 inverter 4 cabinet temp."]=str(round(random.uniform(15.000000,41.000000),6))
    data["Sys 1 inverter 5 cabinet temp."]=str(round(random.uniform(16.000000,44.000000),6))
    data["Sys 1 inverter 6 cabinet temp."]=str(round(random.uniform(16.000000,40.000000),6))
    data["Sys 1 inverter 7 cabinet temp."]=str(round(random.uniform(15.000000,41.000000),6))
    data["Sys 2 inverter 1 cabinet temp."]=str(round(random.uniform(19.000000,45.000000),6))
    data["Sys 2 inverter 2 cabinet temp."]=str(round(random.uniform(19.000000,45.000000),6))
    data["Sys 2 inverter 3 cabinet temp."]=str(round(random.uniform(17.000000,40.000000),6))
    data["Sys 2 inverter 4 cabinet temp."]=str(round(random.uniform(17.000000,40.000000),6))
    data["Sys 2 inverter 5 cabinet temp."]=str(-14.0)
    data["Sys 2 inverter 6 cabinet temp."]=str(0.0)
    data["Sys 2 inverter 7 cabinet temp."]=str(-50.0)
    data["Spinner temp."]=str(round(random.uniform(8.00000,31.00000),5))
    data["Front bearing temp."]=str(round(random.uniform(11.000000,31.000000),6))
    data["Rear bearing temp."]=str(round(random.uniform(11.000000,39.000000),6))
    data["Pitch cabinet blade A temp."]=str(round(random.uniform(22.000000,52.000000),6))
    data["Pitch cabinet blade B temp."]=str(round(random.uniform(22.000000,47.000000),6))
    data["Pitch cabinet blade C temp."]=str(round(random.uniform(22.000000,46.000000),6))
    data["Blade A temp."]=str(round(random.uniform(165.000000,167.000000),6))
    data["Blade B temp."]=str(round(random.uniform(164.000000,166.000000),6))
    data["Blade C temp."]=str(round(random.uniform(165.000000,167.000000),6))
    data["Rotor temp. 1"]=str(round(random.uniform(12.000000,122.000000),6))
    data["Rotor temp. 2"]=str(round(random.uniform(12.000000,121.000000),6))
    data["Stator temp. 1"]=str(round(random.uniform(12.000000,117.000000),6))
    data["Stator temp. 2"]=str(round(random.uniform(12.000000,116.000000),6))
    data["Nacelle ambient temp. 1"]=str(round(random.uniform(3.000000,23.000000),6))
    data["Nacelle ambient temp. 2"]=str(round(random.uniform(3.000000,23.000000),6))
    data["Nacelle temp."]=str(round(random.uniform(6.000000,27.000000),6))
    data["Nacelle cabinet temp."]=str(round(random.uniform(9.000000,30.000000),6))
    data["Main carrier temp."]=str(round(random.uniform(7.000000,25.000000),6))
    data["Rectifier cabinet temp."]=str(round(random.uniform(19.000000,43.000000),6))
    data["Yaw inverter cabinet temp."]=str(round(random.uniform(15.000000,35.000000),6))
    data["Fan inverter cabinet temp."]=str(round(random.uniform(19.000000,39.000000),6))
    data["Ambient temp."]=str(round(random.uniform(3.000000,24.000000),6))
    data["Tower temp."]=str(round(random.uniform(8.000000,36.000000),6))
    data["Control cabinet temp."]=str(round(random.uniform(18.000000,45.000000),6))
    data["Transformer temp."]=str(round(random.uniform(17.000000,70.000000),6))
    data["RTU: ava. Setpoint 1"]=str(round(random.uniform(2501.000000,3050.000000),6))
    data["Inverter averages"]=str(round(random.uniform(16.727272,41.636364),6))
    data["Inverter std dev"]=str(round(random.uniform(0.539360,3.503245),6))

    
    if len(all_patient_data_heartattack)==10:
        all_patient_data_heartattack.remove(all_patient_data_heartattack[0])
    all_patient_data_heartattack.append(data)
    
    return data


def test(ind):
    count = 0
    df = pd.DataFrame(columns=['age','sex','cp','trtbps','chol','fbs','restecg','O2 saturation','slp','thall'])
    df2=pd.DataFrame(columns=['age','sex','cp','trtbps','chol','fbs','restecg','O2 saturation','slp','thall'])
    print(df)
    while True:
        
        if count < 4:
            patientdata = gen_patient_data()
            df1 = pd.DataFrame(data=patientdata,index=[ind])
            ind=ind+1
            # print(df1)
            df = pd.concat([df, df1], ignore_index = True)
            #print("df - > ",df)
            
            count=count +1

            
            time.sleep(5)
        
        else:
            print("final frame",df[['date','unit_number']])
            BUCKET_NAME = 'aws-model1'
            
            csv_buffer1=StringIO()
            df.to_csv(csv_buffer1, index=False)

            
            
            response1=s3csv.put_object(Body=csv_buffer1.getvalue(),
                           Bucket=BUCKET_NAME,
                           Key='All_data.csv') 
            count=0

            df.drop(df.index , inplace=True)
            time.sleep(5)


        
        #print(rf3.predict(df))
        
    

if __name__ == "__main__":
    test(0)
    # t1 = threading.Thread(target = test) 
    # t1.start()