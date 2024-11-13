from flask import Flask, request, make_response
import uuid
import json
import kubernetes
from kubernetes import client, config
import sys
from cloudpathlib import CloudPath
from cloudpathlib import S3Client
import tritonclient.http.aio as httpclient
import numpy as np
if sys.version_info >= (3, 12, 0):
      import six
      sys.modules['kafka.vendor.six.moves'] = six.moves
from kafka import KafkaProducer
import os
import tarfile
import lzma
import traceback
import logging

from io import BytesIO

import pickle
import base64

import re

import subprocess

import tempfile

from pathlib import Path

import threading

import rasterio

import numpy as np

import asyncio
import csv

import time

import pandas as pd

import functools

def create_app():

      app = Flask(__name__)

      logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
      kafka_logger = logging.getLogger('kafka')
      kafka_logger.setLevel(logging.CRITICAL)

      # This is the entry point for the SSL model from Image to Feature service.
      # It will receive a message from the Kafka topic and then do the inference on the data.
      # The result will be sent to the next service.
      # The message received should be a json with the following fields:
      # previous_component_end : A boolean that indicate if the previous component has finished.
      # S3_bucket_desc : A json with the following fields:
      # folder : The folder where the data is stored.
      # The namespace of the configmap to read is the name of the pod.
      # The name of the configmap to read is given by the URL.
      # The configmap should have a field named jsonSuperviserRequest that is a json with the following fields:
      # Topics : A json with the following fields:
      # out : The name of the kafka topic to send the result.
      # S3_bucket : A json with the following fields:
      # aws_access_key_id : The access key id of the S3 bucket.
      # aws_secret_access_key : The secret access key of the S3 bucket.
      # s3-bucket_name : The name of the S3 bucket.
      # region_name : The name of the region of the S3 bucket.
      # endpoint_url : The endpoint url of the S3 bucket.
      # ML : A json with the following fields:
      # need-to-resize : A boolean that indicate if the data need to be resized.

      def log(outfile,message):
            app.logger.warning(message)
            if outfile is not None:
                  timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                  outfile.write(timestamp+':'+message+'\n')

      @app.route('/<name>', methods=['POST'])
      def cfactor(name):
            app.logger.warning('received request')
            # TODO : Debugging message to remove in production.
            # Message received.
            response=None
            try:
                  config.load_incluster_config()
                  api_instance = client.CoreV1Api()
                  configmap_name = str(name)
                  configmap_namespace = 'uc4'
                  app.logger.warning('Namespace '+str(configmap_namespace))
                  api_response = api_instance.read_namespaced_config_map(configmap_name, configmap_namespace)
                  json_data_request = json.loads(request.data)
                  json_data_configmap =json.loads(str(api_response.data['jsonSuperviserRequest']))
                  bootstrapServers =api_response.data['bootstrapServers']
                  Producer=KafkaProducer(bootstrap_servers=bootstrapServers,value_serializer=lambda v: json.dumps(v).encode('utf-8'),key_serializer=str.encode)
                  app.logger.warning('Reading json data request'+str(json_data_request))
                  app.logger.warning('Reading json data configmap'+str(json_data_configmap))
                  assert json_data_request['previous_component_end'] == 'True' or json_data_request['previous_component_end']
                  kafka_out = json_data_configmap['Topics']["out"]
                  s3_access_key = json_data_configmap['S3_bucket']['aws_access_key_id']
                  s3_secret_key = json_data_configmap['S3_bucket']['aws_secret_access_key']
                  s3_bucket_output = json_data_configmap['S3_bucket']['s3-bucket-name']
                  s3_region = json_data_configmap['S3_bucket']['region_name']
                  s3_region_endpoint = json_data_configmap['S3_bucket']['endpoint_url']

                  s3_path = json_data_request['S3_bucket_desc']['folder']
                  #s3_file = json_data_request['S3_bucket_desc'].get('filename',None)
                  log_function = functools.partial(log,None)

                  def threadentry():
                        app.logger.warning('All json data read')

                        clientS3 = S3Client(aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key,endpoint_url=s3_region_endpoint)
                        clientS3.set_as_default_client()

                        app.logger.warning('Client is ready')
                        nonlocal s3_path
                        if s3_path.endswith('/'):
                              s3_path=s3_path[:-1]
                        cp = CloudPath("s3://"+s3_bucket_output+'/'+s3_path+'/', client=clientS3)
                        cpOutput = CloudPath("s3://"+s3_bucket_output+'/result-uc4-ForestPrediction/')
                        app.logger.warning("path is s3://"+s3_bucket_output+'/result-uc4-ForestPrediction/')

                        with cpOutput.joinpath('log.txt').open('w') as fileOutput:
                              log_function = functools.partial(log,fileOutput)
                              
                              to_treat={}
                              for folder in cp.iterdir():
                                    if folder.name.endswith('.npy'):
                                          data = np.load(folder)
                                          if data.ndim==1:
                                                data=np.expand_dims(data,axis=0)
                                          def read_data(x):
                                                n=data.shape[0]
                                                inputs=[]
                                                for i in range(0,n):
                                                      inputs.append({"input":data[i,:]})
                                                return inputs
                                          input_data=read_data(data)
                                          asyncio.run(doInference(input_data,log_function))
                                          array=[]
                                          for elem in input_data:
                                                array.append(elem["result"])
                                          array=np.array(array)
                                          log_function('Output'+str(array.shape))
                                          with cpOutput.joinpath(folder.name).open('wb') as fileOutput:
                                                np.save(fileOutput,array)
                                          with cpOutput.joinpath(folder.name+'.csv').open('w') as fileOutput:
                                                np.savetxt(fileOutput,array,delimiter=',')

                              log_function('Output written')
                              log_function('Connecting to Kafka')
      
                              response_json ={
                              "previous_component_end": "True",
                              "S3_bucket_desc": {
                                    "folder": "result-uc2-FuelConsumption","filename": ""
                              },
                              "meta_information": json_data_request.get('meta_information',{})}
                              Producer.send(kafka_out,key='key',value=response_json)
                              Producer.flush()
                  thread = threading.Thread(target=threadentry)
                  thread.start()
                  response = make_response({
                              "msg": "Started the process"
                              })

            except Exception as e:
                  app.logger.warning('Got exception '+str(e))
                  app.logger.warning(traceback.format_exc())
                  app.logger.warning('So we are ignoring the message')
                  # HTTP answer that the message is malformed. This message will then be discarded only the fact that a sucess return code is returned is important.
                  response = make_response({
                  "msg": "There was a problem ignoring"
                  })
            return response

      # This function is used to do the inference on the data.
      # It will connect to the triton server and send the data to it.
      # The result will be returned.
      # The data should be a numpy array of shape (1,10,120,120) and type float32.
      # The result will be a json with the following fields:
      # model_name : The name of the model used.
      # outputs : The result of the inference.
      async def doInference(toInfer,log_function):

            triton_client = httpclient.InferenceServerClient(url="default-inference.uc4.svc.ecmwf-inference-server.local", verbose=False,conn_timeout=10000000,conn_limit=None,ssl=False)
            nb_Created=0
            nb_InferenceDone=0
            nb_Postprocess=0
            nb_done_instance=0
            list_postprocess=set()
            list_task=set()
            last_throw=0
            async def consume(task):
                  try:
                        length=task[0]
                        count=task[1]
                        inputs=[]
                        outputs=[]
                        input=np.zeros([length,toInfer[count]["input"].shape[0]],dtype=np.float32)
                        for i in range(0,length):
                              input[i,:]=toInfer[count+i]["input"]
                        inputs.append(httpclient.InferInput('input',input.shape, "FP32"))
                        inputs[0].set_data_from_numpy(input, binary_data=True)
                        outputs.append(httpclient.InferRequestedOutput('probability', binary_data=True))
                        results = await triton_client.infer('forest',inputs,outputs=outputs)
                        return (task,results)
                  except Exception as e:
                        log_function('Got exception '+str(e))
                        log_function(traceback.format_exc())
                        nonlocal last_throw
                        last_throw=time.time()
                        return await consume(task)
            
            async def postprocess(task,results):
                  length=task[0]
                  result=results.as_numpy('probability')
                  for i in range(0,length):
                        toInfer[task[1]+i]["result"]=result[i]

            def postprocessTask(task):
                  list_task.discard(task)
                  new_task=asyncio.create_task(postprocess(*task.result()))
                  list_postprocess.add(new_task)
                  def postprocessTaskDone(task2):
                        nonlocal nb_Postprocess
                        nb_Postprocess+=1
                        nonlocal nb_done_instance
                        nb_done_instance+=task.result()[0][0]
                        list_postprocess.discard(task2)
                  new_task.add_done_callback(postprocessTaskDone)
                  nonlocal nb_InferenceDone
                  nb_InferenceDone+=1

            def producer():
                  total=len(toInfer)
                  count=0
                  while total-count>=255:
                        yield (255,count)
                        count=count+255
                  yield (total-count,count)
            
            last_shown=time.time()
            start=time.time()-60
            for item in producer():
                  while time.time()-last_throw<30 or nb_Created-nb_InferenceDone>(time.time()-start)*5 or nb_Postprocess-nb_InferenceDone>(time.time()-start)*5:
                        await asyncio.sleep(0)
                  task=asyncio.create_task(consume(item))
                  list_task.add(task)
                  task.add_done_callback(postprocessTask)
                  nb_Created+=1
                  if time.time()-last_shown>60:
                        last_shown=time.time()
                        log_function('done instance '+str(nb_done_instance)+'Inference done value '+str(nb_InferenceDone)+' postprocess done '+str(nb_Postprocess)+ ' created '+str(nb_Created))
            while nb_InferenceDone-nb_Created>0 or nb_Postprocess-nb_InferenceDone>0:
                  await asyncio.sleep(0)
            await asyncio.gather(*list_task,*list_postprocess)
            log_function('Inference done')
            await triton_client.close()
      return app