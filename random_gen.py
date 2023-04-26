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
    #data["lvef"]=str(random.randrange(15, 75))
    #data["lvid"]=str(round(random.uniform(2.20, 6.00),2))
    #data["ppv"]=str(round(random.uniform(1.00,7.60),2))
    #data["lvids"]=str(round(random.uniform(2.00,4.76),2))
    #data["avpv"]=str(round(random.uniform(2.00, 8.00),2))
    if len(all_patient_data_heartattack)==10:
        all_patient_data_heartattack.remove(all_patient_data_heartattack[0])
    all_patient_data_heartattack.append(data)
    
    return data


def test(ind):
    count = 0
    df = pd.DataFrame(columns=['age','sex','cp','trtbps','chol','fbs','restecg','O2 saturation','slp','thall'])
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
            print("final frame",df)
            BUCKET_NAME = 'aws-model1'
            
            csv_buffer1=StringIO()
            df.to_csv(csv_buffer1, index=False)

            
            
            response1=s3csv.put_object(Body=csv_buffer1.getvalue(),
                           Bucket=BUCKET_NAME,
                           Key='heartattackcollect.csv') 
            count=0

            df.drop(df.index , inplace=True)
            time.sleep(5)


        
        #print(rf3.predict(df))
        
    

if __name__ == "__main__":
    test(0)
    # t1 = threading.Thread(target = test) 
    # t1.start()