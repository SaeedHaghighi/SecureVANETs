import numpy as np

Server_Address='IP:PORT'

import multiprocessing
import sys
import threading
import matplotlib
from json import JSONEncoder

from Utils_SUMO import calculate_Avg_CoSimCH, cosineSimilarity, calculate_Avg_CoSimCH_threeElements

matplotlib.use('TkAgg')

from multiprocessing import Process
from kafka import KafkaProducer, KafkaConsumer, KafkaClient
import json
from json import dumps
import random
import datetime
import numpy

from FL_learning import load_minist_data
from sklearn.linear_model import SGDClassifier

Dic_CH_Coeff = {}

public_topicc = 'public_topic4'


class NumpyArrayEncoder(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, numpy.ndarray):
            return obj.tolist()
        return JSONEncoder.default(self, obj)

def preprocess_coeff(coeff):
    try:
        coeff.shape[0]
        return coeff
    except:
        return ''
class vehicle_VIB:
    In_TIMER = 1
    MAX_HOPs = 10
    MAX_Member_CH = 100
    MAX_Member_CM = 10
    SE_TIMER =10
    tedad = 0

    producer = KafkaProducer(bootstrap_servers=[Server_Address])
    consumer = KafkaConsumer(public_topicc, bootstrap_servers=[Server_Address],
                             consumer_timeout_ms=In_TIMER * 1000)

    producer_request_CH = KafkaProducer(bootstrap_servers=[Server_Address])
    producer_respect_CH = KafkaProducer(bootstrap_servers=[Server_Address])

    producer_coeff_to_CH = KafkaProducer(bootstrap_servers=[Server_Address])
    consumer_coeff_from_CH = ''

    producer_coeff_to_CM = KafkaProducer(bootstrap_servers=[Server_Address])
    consumer_coeff_from_CM = ''

    consumer_request_CH = ''
    consumer_respect_CH = ''
    producer_request_CM = ''
    consumer_request_CM = ''
    producer_respect_CM = ''
    consumer_respect_CM = ''

    position_x = ''
    position_y = ''
    direction = 'forward'
    velocity = ''
    current_clustering_state = 'SE'
    number_of_hops_to_CH = '0'
    SourceID = ''
    clusteringMetric_averageSpeed = ''
    speed = ''
    SequenceNumeber_dataPacket = ''
    ID_to_CH = '-'
    IDs_from_CH = ''
    TRANMISSION_RANGE = '1000'
    avg_speed = 0
    avg_coeff = 0
    V_state = ''
    vib_neighbor = {}
    Member_CH = 0
    Member_CM = 0
    HOPE_CM = -1
    X_data = ''
    Y_data = ''
    coefficients = ''
    intercept_ = ''
    hello_packet = ''
    list_neighbor_VIB = list()
    Dic_recieve_coeff_in_CH = {}
    Dic_recieve_coeff_in_CH_previous={}
    Dic_recieve_coeff_in_EPC_previous={}
    mess_resp = {}
    msg_coef = {}
    mess_from_CH = {}
    have_coef=0
    msg_request_CH=''
    remain_CH=0
    alpha=0.5
    timer_CM=0
    SourceID_EPC=100
    producer_coeff_to_EPC=''
    neighbor_Avg_CoSimCH={}
    neighbor_Avg_CoSimCM={}
    consumer_EPC=''
    producer_EPC_coeff_to_CH=''
    CH_start=''
    CH_end=''
    getNoise=False
    list_remain_CH=list()

    Block_flag=False
    block_duration=0
    Unblock_Time=0
    SELECTED_CLIENT_PERCENTAGE=0.75
    RELI_SCORE=random.random(0,1)


    accuracy_weight=random.random(0,1)
    frequency_weight=random.random(0,1)
    anomaly_weight=random.random(0,1)

    def __init__(self, pos_x, pos_y, speed, SourceID,
                 X_data, Y_data, X_data_test, Y_data_test, max_iter,dic_vehicl_times,
                 isEPC,
                 haveClustering,getNoise):
        self.position_x = pos_x
        self.position_y = pos_y
        self.speed = speed
        self.SourceID = SourceID
        self.avg_speed = speed
        self.V_state = 'SE'
        self.X_data = X_data
        self.Y_data = Y_data
        self.msg_request_CH=''
        self.X_data_test = X_data_test
        self.Y_data_test = Y_data_test
        self.accuracy = 0
        self.have_coef = 0
        self.remain_CH=0
        self.timer_CM=0
        self.getNoise=getNoise
        self.sgd_classifier = SGDClassifier(random_state=42, max_iter=max_iter)
        self.sgd_classifier_CH = SGDClassifier(random_state=42, max_iter=max_iter)
        self.dic_vehicl_times=dic_vehicl_times
        self.timeChangePosition=1
        self.producer_coeff_to_EPC= KafkaProducer(bootstrap_servers=[Server_Address])
        self.producer_EPC_coeff_to_CH = KafkaProducer(bootstrap_servers=[Server_Address])
        self.neighbor_Avg_CoSimCH={}
        self.neighbor_Avg_CoSimCM={}
        self.consumer_EPC=''

        self.Block_flag = False
        self.Unblock_Time = 0
        self.SELECTED_CLIENT_PERCENTAGE = 0.75
        self.RELI_SCORE = random.random(0, 1)

        if (isEPC=='0'):  # for non-EPC vehicles

            if (haveClustering=='1'):  # for Clustering
                #====
                # Main Algorithm
                #=====
                Main_Algorithm_thread = threading.Thread(target=self.Main_Algorithm)
                Main_Algorithm_thread.start()


                CH_CM_RecieveHelloPacket_Thread = threading.Thread(target=self.CH_CM_RecieveHelloPacket)
                CH_CM_RecieveHelloPacket_Thread.start()


                Join_Request_Thread = threading.Thread(target=self.Join_Request)
                Join_Request_Thread.start()


                Connect_Request_Thread = threading.Thread(target=self.Connect_Request)
                Connect_Request_Thread.start()
            else:   # No Clustering ...
                NoClustering_HFL_Thread = threading.Thread(target=self.NoClustering_HFL)
                NoClustering_HFL_Thread.start()




        else: # is EPC
            print('is EPC')
            EPC_Thread = threading.Thread(target=self.EPC)
            EPC_Thread.start()
            # pass


    # ---
    def get_accuracy(self):
        self.sgd_classifier_CH.coef_ = self.avg_coeff
        self.sgd_classifier_CH.intercept_ = self.intercept_
        try:
            self.sgd_classifier_CH.partial_fit(self.X_data, self.Y_data, classes=numpy.unique(self.Y_data))
        except:
            self.sgd_classifier_CH.fit(self.X_data, self.Y_data)
        return str(self.sgd_classifier_CH.score(self.X_data_test, self.Y_data_test))

    def recieve_coeff_from_CH(self):

        pass

    def recieve_coeff_from_CM(self):
        wait_recieve_coff_from_CMs = 3

        self.consumer_coeff_from_CM = KafkaConsumer('SendCHCoeff_' + str(self.SourceID),
                                                    bootstrap_servers=[Server_Address])
        round_time = 1
        # ================================*********************=====================
        self.Dic_recieve_coeff_in_CH = {}
        while (True):
            msg_coff_ch = self.consumer_coeff_from_CM.poll(
                timeout_ms=wait_recieve_coff_from_CMs * 1000)
            if not msg_coff_ch:
                ''
            else:
                sum_coeff = 0
                self.tedad = 0
                list_CMs_Source_IDs = list()
                for topic_data, consumer_records in msg_coff_ch.items():  # All messages
                    for message in consumer_records:  # each message
                        mess_coeff_ch = json.loads(message.value)

                        if (mess_coeff_ch['SourceID'] not in self.Dic_recieve_coeff_in_CH.keys()):
                            self.Dic_recieve_coeff_in_CH[mess_coeff_ch['SourceID']] = {}
                            self.tedad = self.tedad + 1
                        self.Dic_recieve_coeff_in_CH[mess_coeff_ch['SourceID']]['coefficients'] = mess_coeff_ch[
                            'coefficients']
                        self.Dic_recieve_coeff_in_CH[mess_coeff_ch['SourceID']]['intercept_'] = mess_coeff_ch[
                            'intercept_']
                        list_CMs_Source_IDs.append(mess_coeff_ch['SourceID'])

                round_time = round_time + 1
                sum_coeff = 0
                sum_inter = 0
                for agent in self.Dic_recieve_coeff_in_CH.keys():
                    try:
                        sum_coeff = sum_coeff + numpy.array(self.Dic_recieve_coeff_in_CH[agent]['coefficients'])
                        sum_inter = sum_inter + numpy.array(self.Dic_recieve_coeff_in_CH[agent]['intercept_'])

                    except:
                        sum_coeff = numpy.array(self.Dic_recieve_coeff_in_CH[agent]['coefficients'])
                        sum_inter = numpy.array(self.Dic_recieve_coeff_in_CH[agent]['intercept_'])

                self.avg_coeff = numpy.asarray(sum_coeff) / len(self.Dic_recieve_coeff_in_CH.keys())
                self.intercept_ = numpy.asarray(sum_inter) / len(self.Dic_recieve_coeff_in_CH.keys())
                strr = ''
                for i in self.Dic_recieve_coeff_in_CH.keys():
                    strr = strr + ',' + str(i)
                self.accuracy = self.get_accuracy()
                # ===
                # EPC (Set Coeff Parameters)
                # ===
                if (self.SourceID not in Dic_CH_Coeff.keys()):
                    Dic_CH_Coeff[self.SourceID] = {}
                Dic_CH_Coeff[self.SourceID]['avg_coeff'] = self.avg_coeff
                Dic_CH_Coeff[self.SourceID]['intercept_'] = self.intercept_
                # =====

                self.hello_packet['coefficients'] = preprocess_coeff(self.avg_coeff)

                self.hello_packet['intercept_'] = preprocess_coeff(self.intercept_)
                for CM_ID in list_CMs_Source_IDs:
                    self.hello_packet['coefficients'] = preprocess_coeff(self.avg_coeff)
                    self.hello_packet['intercept_'] =preprocess_coeff(self.intercept_)
                    time.sleep(2)
                    try:
                        self.producer_coeff_to_CM.send('RecieveCHCoeff_' + str(CM_ID),
                                                       value=bytes(dumps(self.hello_packet,cls=NumpyArrayEncoder), 'utf-8'))
                    except:
                        print('Error in producer_coeff_to_CM')
                        continue

    # ====
    def remain_CH_thread_func(self):
        while(True):
            time.sleep(30)
            if self.remain_CH==0:
                print('Modify CH_State___' + str(self.SourceID))
                # ====
                self.V_state = 'SE'
                self.ID_to_CH = ''
                # ====
            else:
                self.remain_CH = 0


    def Join_Request(self):
        while(True):
            try:
                self.consumer_request_CH = KafkaConsumer('Join_Request_' + str(self.SourceID),
                                                         bootstrap_servers=[Server_Address])

                #========
                for message in self.consumer_request_CH :  # each message
                    if (
                            self.Member_CH <= self.MAX_Member_CH):
                        self.hello_packet = self.create_Hello_packet()
                        time.sleep(3)
                        try:
                            self.producer_respect_CH.send('Join_Response_' + str(json.loads(message.value)['SourceID']),
                                                      value=bytes(dumps(self.hello_packet,cls=NumpyArrayEncoder), 'utf-8'),
                                                          )
                        except:
                            print('Error Created for producer_respect_CH')
                            continue
                        self.remain_CH=1
            except:
                continue


    def Connect_Request(self):
        self.consumer_request_CH = KafkaConsumer('Connect_Request_' + str(self.SourceID),
                                                 bootstrap_servers=[Server_Address])

        # ========
        for message in self.consumer_request_CH:
            if (
                    self.Member_CH <= self.MAX_Member_CH):
                self.hello_packet = self.create_Hello_packet()
                time.sleep(3)
                try:
                    self.producer_respect_CH.send('Connect_Response_' + str(json.loads(message.value)['SourceID']),
                                                  value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder), 'utf-8'),
                                                  )
                except:
                    print('Error Created for producer_respect_CH')
                    continue
                self.remain_CH = 1

    # =====

    def create_Hello_packet(self):
        self.hello_packet = {}
        self.hello_packet['direction'] = self.direction
        self.hello_packet['velocity'] = self.velocity
        self.hello_packet['current_clustering_state'] = self.current_clustering_state
        self.hello_packet['number_of_hops_to_CH'] = self.number_of_hops_to_CH
        self.hello_packet['ID_to_CH'] = self.ID_to_CH
        self.hello_packet['speed'] = self.speed
        self.hello_packet['avg_speed'] = self.avg_speed
        self.hello_packet['SourceID'] = self.SourceID
        self.hello_packet['position_x'] = self.position_x
        self.hello_packet['position_y'] = self.position_y
        self.hello_packet['timestamp'] = str(datetime.datetime.now())
        self.hello_packet['V_state'] = self.V_state
        self.hello_packet['HOPE_CM'] = self.HOPE_CM
        self.hello_packet['Member_CM'] = self.Member_CM
        self.hello_packet['Member_CH'] = self.Member_CH
        self.hello_packet['coefficients'] = preprocess_coeff(self.coefficients)
        self.hello_packet['intercept_'] = preprocess_coeff(self.intercept_)
        return self.hello_packet

    def Train_Update_local_SGD(self):
        try:
            self.X_data = numpy.asarray(self.X_data)
            self.Y_data = numpy.asarray(self.Y_data)
            self.sgd_classifier.coef_ = self.coefficients
            self.sgd_classifier.intercept_ = self.intercept_
            try:
                self.sgd_classifier.partial_fit(self.X_data, self.Y_data)
            except:
                self.sgd_classifier.fit(self.X_data, self.Y_data)
            self.coefficients = self.sgd_classifier.coef_
            self.intercept_ = self.sgd_classifier.intercept_
        except Exception as e:
    def Train_sendCoeff(self):
        while (True):
            try:
                if (self.have_coef == 0):  # for First Time
                    self.coefficients = self.mess_resp['coefficients']  # get coefficients from CH
                    self.intercept_ = self.mess_resp['intercept_']  # get coefficients from CH

                self.Train_Update_local_SGD()
                self.hello_packet = self.create_Hello_packet()
                self.consumer_coeff_from_CH = KafkaConsumer('RecieveCHCoeff_' + str(self.SourceID),
                                                            bootstrap_servers=[
                                                                Server_Address])
                self.producer_coeff_to_CH.send('SendCHCoeff_' + str(self.ID_to_CH),
                                               value=bytes(dumps(self.hello_packet,cls=NumpyArrayEncoder), 'utf-8'))

                self.msg_coef = self.consumer_coeff_from_CH.poll(timeout_ms=20 * 1000)
                if not self.msg_coef:
                else:
                    for topic_data, consumer_records in self.msg_coef.items():
                        for message in consumer_records:  # each message

                            self.mess_from_CH = json.loads(message.value)
                            try:
                                self.hello_packet['coefficients'] = numpy.asarray(self.mess_from_CH['coefficients'])
                                self.hello_packet['intercept_'] = numpy.asarray(self.mess_from_CH['intercept_'])
                            except:
                            self.coefficients = numpy.asarray(self.mess_from_CH['coefficients'])
                            self.intercept_ = numpy.asarray(self.mess_from_CH['intercept_'])
                            self.have_coef = 1

                            print('EXITTTT')
                            return 1
                            #====
                # return 1
                time.sleep(5)
            except Exception as e:
                print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
                print('error training')




    def SE_Algorithm_OLD(self):
        for neighbor in self.vib_neighbor.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'CH'):  # for all CH in vib
                if (self.vib_neighbor[neighbor]['TRY_CONNECT_CH'] == 0):
                    if (int(self.vib_neighbor[neighbor]['Member_CH']) <= int(self.MAX_Member_CH)):
                        Join_Timer = 5
                        self.hello_packet = self.create_Hello_packet()
                        self.consumer_respect_CH = KafkaConsumer('RespectCH_' + str(self.SourceID),
                                                                 bootstrap_servers=[Server_Address]
                                                                 )
                        self.producer_request_CH.send('RequestCH_' + str(neighbor),
                                                      value=bytes(dumps(self.hello_packet,cls=NumpyArrayEncoder), 'utf-8'))

                        msg_respect = self.consumer_respect_CH.poll(
                            timeout_ms=Join_Timer * 1000)
                        if not msg_respect:
                        else:
                            for topic_data, consumer_records in msg_respect.items():
                                message_resp = consumer_records[0].value
                                mess_resp = json.loads(message_resp)
                                self.ID_to_CH = str(neighbor)
                                if (self.V_state == 'SE'):
                                    self.V_state = 'CM'
                                self.mess_resp = mess_resp
                                return 1
                        ######
                        #####
                        #######

                    self.vib_neighbor[neighbor]['TRY_CONNECT_CH'] = 1

        #=====
        if (self.V_state=='CM'):
            print('Modify CM_State___'+str(self.SourceID))
            #====
            self.V_state = 'SE'
            self.ID_to_CH = ''
            self.have_coef = 0
            #====
        #============
        return 0  # exit from SE_Algorithm(self)
    def SA_State_CMsection(self):
        connectionCM_neighbor = {}
        for neighbor in self.vib_neighbor.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'CM'):
                connectionCM_neighbor[neighbor]=False

        neighbor_Avg_CoSimCM={}
        for neighbor in self.vib_neighbor.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'CM'):  # for all CM in vib
                if (connectionCM_neighbor[neighbor]==False and self.HOPE_CM< (self.MAX_HOPs-1)):
                    # send Join_REQ
                    Join_Timer = 10
                    self.hello_packet = self.create_Hello_packet()
                    self.consumer_respect_CH = KafkaConsumer('Join_Response_' + str(self.SourceID),
                                                             bootstrap_servers=[Server_Address]
                                                             )

                    self.producer_request_CH.send('Join_Request_' + str(neighbor),
                                                  value=bytes(dumps(self.hello_packet,cls=NumpyArrayEncoder), 'utf-8'))

                    msg_respect = self.consumer_respect_CH.poll(timeout_ms=Join_Timer * 1000)  # wait for JoinTimer (for first msg)
                    if not msg_respect:
                    else:   # Join_Response recieved
                        for topic_data, consumer_records in msg_respect.items():
                            message_resp = consumer_records[0].value
                            self.mess_resp=''
                            self.mess_resp = json.loads(message_resp)
                            speedCM=self.mess_resp['speed']
                            speedK=self.speed
                            thetaCM=self.mess_resp['coefficients']
                            thetaK=self.coefficients
                            Avg_CoSimCM=calculate_Avg_CoSimCH(self.alpha, speedCM, speedK, thetaCM, thetaK)
                            self.vib_neighbor[neighbor]['Avg_CoSimCM']=Avg_CoSimCM
                            connectionCM_neighbor[neighbor]=True
                            neighbor_Avg_CoSimCM[neighbor]=Avg_CoSimCM

        neighbor_Avg_CoSimCM=dict(sorted(neighbor_Avg_CoSimCM.items(), key=lambda item: item[1]))
        for neighbor in self.neighbor_Avg_CoSimCM.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'CM'):  # for all CM in vib
                if (connectionCM_neighbor[neighbor]==True):
                    self.consumer_respect_CH = KafkaConsumer('Connect_Response_' + str(self.SourceID),
                                                             bootstrap_servers=[Server_Address])

                    self.producer_request_CH.send('Connect_Request_' + str(neighbor),
                                                  value=bytes(dumps(self.hello_packet,cls=NumpyArrayEncoder), 'utf-8'))
                    msg_respect = self.consumer_respect_CH.poll(timeout_ms=Join_Timer * 1000)
                    if not msg_respect:
                        ''
                    else:
                        for topic_data, consumer_records in msg_respect.items():
                            message_resp = consumer_records[0].value
                            mess_resp = json.loads(message_resp)
                            self.ID_to_CM =mess_resp['SourceID']
                            if (self.V_state == 'SE'):
                                self.V_state = 'CM'
                            self.mess_resp = mess_resp
                            return 1

                else:
                    connectionCM_neighbor[neighbor] = False
        return 0

    def SA_State_CHsection(self):
        connectionCH_neighbor = {}
        for neighbor in self.vib_neighbor.keys():
            connectionCH_neighbor[neighbor] = False
        for neighbor in self.vib_neighbor.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'CH'):  # for all CH in vib
                if (connectionCH_neighbor[neighbor] == False):
                    # send Join_REQ
                    Join_Timer = 5
                    self.hello_packet = self.create_Hello_packet()
                    self.consumer_respect_CH = KafkaConsumer('Join_Response_' + str(self.SourceID),
                                                             bootstrap_servers=[Server_Address]
                                                             )

                    self.producer_request_CH.send('Join_Request_' + str(neighbor),
                                                  value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder), 'utf-8'))

                    msg_respect = self.consumer_respect_CH.poll(
                        timeout_ms=Join_Timer * 1000)
                    if not msg_respect:
                    else:  # Join_Response recieved
                        for topic_data, consumer_records in msg_respect.items():
                            message_resp = consumer_records[0].value
                            self.mess_resp = ''
                            self.mess_resp = json.loads(message_resp)
                            speedCH = self.mess_resp['speed']
                            speedK = self.speed
                            thetaCH = self.mess_resp['coefficients']
                            thetaK = self.coefficients
                            Avg_CoSimCH = calculate_Avg_CoSimCH(self.alpha, speedCH, speedK, thetaCH, thetaK)
                            self.vib_neighbor[neighbor]['Avg_CoSimCH'] = Avg_CoSimCH
                            connectionCH_neighbor[neighbor] = True
                            self.neighbor_Avg_CoSimCH[neighbor] = Avg_CoSimCH

        self.neighbor_Avg_CoSimCH = dict(sorted(self.neighbor_Avg_CoSimCH.items(), key=lambda item: item[1]))

        # =============
        # Connect to best CH
        # =====================
        for neighbor in self.neighbor_Avg_CoSimCH.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'CH'):  # for all CH in vib
                if (connectionCH_neighbor[neighbor] == True):
                    self.consumer_respect_CH = KafkaConsumer('Connect_Response_' + str(self.SourceID),
                                                             bootstrap_servers=[Server_Address])

                    self.producer_request_CH.send('Connect_Request_' + str(neighbor),
                                                  value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder), 'utf-8'))
                    msg_respect = self.consumer_respect_CH.poll(timeout_ms=Join_Timer * 1000)
                    if not msg_respect:
                        ''
                    else:  # Connect_Response recieved
                        for topic_data, consumer_records in msg_respect.items():
                            message_resp = consumer_records[0].value
                            mess_resp = json.loads(message_resp)
                            self.ID_to_CH = mess_resp['SourceID']
                            if (self.V_state == 'SE'):
                                self.V_state = 'CM'
                            self.mess_resp = mess_resp
                            return 1

                else:
                    connectionCH_neighbor[neighbor] = False
        return 0
    def SA_State_SEsection(self):
        neighbor_Avg_CoSimSE={}
        for neighbor in self.vib_neighbor.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'SE'):  # for all CH in vib
               thetaSE_t=self.vib_neighbor[neighbor]['coefficients']
               thetaSE_t_1=self.coefficients

               left_sum = self.alpha * (self.vib_neighbor[neighbor]['avg_speed'])
               right_sum = (1 - float(self.alpha)) * (1 - float(cosineSimilarity(thetaSE_t,thetaSE_t_1)))
               sum = left_sum
               neighbor_Avg_CoSimSE[neighbor] = sum

        thetaSE_k = self.coefficients
        thetaSE_k_1 = self.coefficients
        left_sum = self.alpha * self.avg_speed
        sum_k= left_sum
        try:
            min_neighbor_Avg_CoSimSE=min(list(neighbor_Avg_CoSimSE.values()))
            if (sum_k<=min_neighbor_Avg_CoSimSE):
                if (self.Block_flag==True or self.block_duration<self.Unblock_Time):
                    self.block_duration=self.block_duration+1
                else:
                    self.V_state = 'CH'
                    self.block_duration=0
                #================================
        except:
            ''


    def SE_Algorithm(self):
        pass
    def send_Hello_packet(self):
        while (True):
            try:
                rnd = random.randint(1, 4)
                time.sleep(rnd)
                try:
                    self.dic_vehicl_times[int(self.SourceID)][self.timeChangePosition]
                    self.position_x =float(self.dic_vehicl_times[int(self.SourceID)][self.timeChangePosition]['x'])
                    self.position_y=float(self.dic_vehicl_times[int(self.SourceID)][self.timeChangePosition]['y'])
                    self.timeChangePosition=self.timeChangePosition+1
                except:
                    print('')
                    ''
                #===============================================

                self.hello_packet = self.create_Hello_packet()
                try:
                    self.hello_packet['coefficients'].shape[0]
                except:
                    self.hello_packet['coefficients']=''
                #=============
                self.producer.send(public_topicc, value=bytes(dumps(self.hello_packet,cls=NumpyArrayEncoder), 'utf-8'))




            except:
                continue


    def recieve_RequestCM(self):
        pass

    def recieve_Hello_packet(self):   #  ======= MAIN function ======
        self.consumer = KafkaConsumer(public_topicc, bootstrap_servers=[Server_Address],
                                      consumer_timeout_ms=self.In_TIMER * 1000)
        while (True):
            time1 = datetime.datetime.now()
            # ====
            self.vib_neighbor = {}
            while True:
                try:
                    date2 = datetime.datetime.now()
                    self.consumer = KafkaConsumer(public_topicc, bootstrap_servers=[Server_Address])
                    #====================================
                    msg = self.consumer.poll(self.In_TIMER * 1000)
                    if not msg:
                        time.sleep(0.1)
                        continue



                    for topic_data, consumer_records in  msg.items():
                        for message in consumer_records:  # each message
                            mess = json.loads(message.value)
                            if (mess['SourceID']==self.SourceID):
                                continue
                            point1 = numpy.array((self.position_x, self.position_y))
                            point2 = numpy.array((mess['position_x'], mess['position_y']))
                            distance = float(numpy.linalg.norm(point1 - point2))
                            if (float(distance) <= float(self.TRANMISSION_RANGE)):
                                if (mess['SourceID'] not in self.vib_neighbor.keys()):
                                    self.vib_neighbor[mess['SourceID']] = {}
                                    self.vib_neighbor[mess['SourceID']]['direction'] = mess['direction']
                                    self.vib_neighbor[mess['SourceID']]['speed'] = mess['speed']
                                    self.vib_neighbor[mess['SourceID']]['number_of_hops_to_CH'] = mess[
                                        'number_of_hops_to_CH']
                                    self.vib_neighbor[mess['SourceID']]['current_clustering_state'] = mess[
                                        'current_clustering_state']
                                    self.vib_neighbor[mess['SourceID']]['distance'] = distance
                                    self.vib_neighbor[mess['SourceID']]['SourceID'] = mess['SourceID']
                                    self.vib_neighbor[mess['SourceID']]['avg_speed'] = float(mess['avg_speed'])
                                    self.vib_neighbor[mess['SourceID']]['V_state'] = str(mess['V_state'])
                                    self.vib_neighbor[mess['SourceID']]['TRY_CONNECT_CH'] = 0
                                    self.vib_neighbor[mess['SourceID']]['HOPE_CM'] = str(mess['HOPE_CM'])
                                    self.vib_neighbor[mess['SourceID']]['Member_CM'] = str(mess['Member_CM'])
                                    self.vib_neighbor[mess['SourceID']]['Member_CH'] = str(mess['Member_CH'])
                                else:
                                    self.vib_neighbor[mess['SourceID']]['speed'] = mess['speed']
                                    self.vib_neighbor[mess['SourceID']]['number_of_hops_to_CH'] = mess[
                                        'number_of_hops_to_CH']
                                    self.vib_neighbor[mess['SourceID']]['current_clustering_state'] = mess[
                                        'current_clustering_state']
                                    self.vib_neighbor[mess['SourceID']]['distance'] = distance
                                    self.vib_neighbor[mess['SourceID']]['SourceID'] = mess['SourceID']
                                    self.vib_neighbor[mess['SourceID']]['avg_speed'] = float(mess['avg_speed'])
                                    self.vib_neighbor[mess['SourceID']]['V_state'] = str(mess['V_state'])
                                    self.vib_neighbor[mess['SourceID']]['HOPE_CM'] = str(mess['HOPE_CM'])
                                    self.vib_neighbor[mess['SourceID']]['Member_CM'] = str(mess['Member_CM'])
                                    self.vib_neighbor[mess['SourceID']]['Member_CH'] = str(mess['Member_CH'])

                    break

                except Exception as e:
                    print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

                    time.sleep(0.1)
                    continue

            sum_minus_speed = 0
            N_i = 0
            if (len(list(self.vib_neighbor.keys())) > 1):
            for neigh in self.vib_neighbor.keys():
                if (self.vib_neighbor[neigh]['direction'] == self.direction):
                    if (int(self.vib_neighbor[neigh]['number_of_hops_to_CH']) <= self.MAX_HOPs):  # within MAX_HOP
                        N_i = N_i + 1
                        speed_i = self.speed
                        speed_ij = self.vib_neighbor[neigh]['speed']
                        minus_speed = numpy.abs(speed_i - speed_ij)
                        sum_minus_speed = sum_minus_speed + minus_speed

            if (N_i != 0):
                self.avg_speed = sum_minus_speed / N_i
            else:
                self.avg_speed = self.speed

    def CH_HFL(self):
        t_Collect=0
        wait_recieve_coff_from_CMs = 0.010
        self.consumer_coeff_from_CM = KafkaConsumer('SendCHCoeff_' + str(self.SourceID),
                                                    bootstrap_servers=[Server_Address])
        round_time = 1
        # ================================*********************=====================
        self.Dic_recieve_coeff_in_CH = {}
        list_CMs_Source_IDs = list()
        list_CMs_Information = {}
        msg_coff_ch = self.consumer_coeff_from_CM.poll(timeout_ms=wait_recieve_coff_from_CMs * 1000)
        if not msg_coff_ch:
            ''
        else:
            sum_coeff = 0
            self.tedad = 0

            for topic_data, consumer_records in msg_coff_ch.items():  # All messages
                for message in consumer_records:  # each message
                    mess_coeff_ch = json.loads(message.value)

                    if (mess_coeff_ch['SourceID'] not in self.Dic_recieve_coeff_in_CH.keys()):
                        self.Dic_recieve_coeff_in_CH[mess_coeff_ch['SourceID']] = {}
                        self.tedad = self.tedad + 1
                    self.Dic_recieve_coeff_in_CH[mess_coeff_ch['SourceID']]['coefficients'] = mess_coeff_ch[
                        'coefficients']
                    self.Dic_recieve_coeff_in_CH[mess_coeff_ch['SourceID']]['intercept_'] = mess_coeff_ch[
                        'intercept_']



                    list_CMs_Source_IDs.append(mess_coeff_ch['SourceID'])

                    list_CMs_Information[mess_coeff_ch['SourceID']]={}
                    if (self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['RELI_SCORE'] is None): # agar baraye bare aval hast!
                        list_CMs_Information[mess_coeff_ch['SourceID']]['RELI_SCORE']=mess_coeff_ch['SourceID']['RELI_SCORE']
                        self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['RELI_SCORE']=mess_coeff_ch['SourceID']['RELI_SCORE']


                    list_CMs_Information[mess_coeff_ch['SourceID']]['Block_flag']=mess_coeff_ch['SourceID']['Block_flag']
                    list_CMs_Information[mess_coeff_ch['SourceID']]['Unblock_Time']=mess_coeff_ch['SourceID']['Unblock_Time']

                #====== Working on Attack ==============================
                print('list_CMs_Source_IDs_size======' + str(len(list_CMs_Source_IDs)))
                # sort dic
                sorted_CMs = dict(sorted(list_CMs_Information.items(), key=lambda x: x[1]['RELI_SCORE'], reverse=True))
                # Calculate the index up to which 75% of the sorted_CMs will be selected
                index = int(len(sorted_CMs) * 0.75)
                # Select 75% of the sorted_CMs using list slicing
                selected_Client = sorted_CMs[:index]

                for vehicle in selected_Client.keys():
                    if (selected_Client[vehicle]['Block_flag'] is True or self.block_duration<selected_Client[vehicle]['Unblock_Time']):
                        continue
                    else:
                        selected_Client[vehicle]['Block_flag']=False
                for vehicleCM in self.Dic_recieve_coeff_in_CH.keys():  # for all CM for one CH
                    cosSim=cosineSimilarity(self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['coefficients'][-1],
                                            self.Dic_recieve_coeff_in_CH[vehicleCM]['coefficients'])
                    if (cosSim>0.5):
                        selected_Client[vehicle]['Block_flag'] = True
                        self.Dic_recieve_coeff_in_CH_previous[vehicle]['Block_flag']=self.Dic_recieve_coeff_in_CH_previous[vehicle]['Block_flag']+1
                        self.block_duration=self.block_duration+1
                    

                    if (self.getNoise):
                        self.Dic_recieve_coeff_in_CH[vehicleCM]['coefficients']
                        noise = np.random.normal(0, 1, self.Dic_recieve_coeff_in_CH[vehicleCM]['coefficients'].shape)
                        self.Dic_recieve_coeff_in_CH[vehicleCM]['coefficients']=self.Dic_recieve_coeff_in_CH[vehicleCM]['coefficients']+noise

                    self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['coefficients'].append(self.Dic_recieve_coeff_in_CH[vehicleCM]['coefficients'])
                    historical_accuracy=1/len(self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['accuracy']) *sum(self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['accuracy'])
                    total_iteration=100
                    total_contribution=len(self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['accuracy'])
                    ContributionFreq=total_contribution/total_iteration
                    Anomaly_record=len(self.Dic_recieve_coeff_in_CH_previous[vehicle]['Block_flag'])/total_contribution


                    RELI_SCORE=(self.accuracy_weight*historical_accuracy)+(self.frequency_weight*ContributionFreq)+\
                    (self.anomaly_weight*Anomaly_record)

                    self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['RELI_SCORE']=RELI_SCORE

                # normalize RELI_SCORE
                list_RELI_SCORE_allCM=list()
                for vehicleCM in self.Dic_recieve_coeff_in_CH.keys():  # for all CM for one CH
                    list_RELI_SCORE_allCM.append(self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['RELI_SCORE'])


                #= Normalize==========
                array_list_RELI_SCORE_allCM=np.array(list_RELI_SCORE_allCM)
                normalize_array=(array_list_RELI_SCORE_allCM-np.min(array_list_RELI_SCORE_allCM))/(np.max(array_list_RELI_SCORE_allCM)-np.min(array_list_RELI_SCORE_allCM))
                index=0
                for vehicleCM in self.Dic_recieve_coeff_in_CH.keys():  # for all CM for one CH
                    self.Dic_recieve_coeff_in_CH_previous[vehicleCM]['RELI_SCORE']=list(normalize_array)[index]
                    index=index+1
                #==================
                # Finish ALgorithm 1
                #===============================================================

            round_time = round_time + 1

            # === Average Coefficients===
            sum_coeff = 0
            sum_inter = 0
            for agent in self.Dic_recieve_coeff_in_CH.keys():
                try:
                    sum_coeff = sum_coeff + numpy.array(self.Dic_recieve_coeff_in_CH[agent]['coefficients'])
                    sum_inter = sum_inter + numpy.array(self.Dic_recieve_coeff_in_CH[agent]['intercept_'])

                except:
                    sum_coeff = numpy.array(self.Dic_recieve_coeff_in_CH[agent]['coefficients'])
                    sum_inter = numpy.array(self.Dic_recieve_coeff_in_CH[agent]['intercept_'])

            self.avg_coeff = numpy.asarray(sum_coeff) / len(self.Dic_recieve_coeff_in_CH.keys())
            self.intercept_ = numpy.asarray(sum_inter) / len(self.Dic_recieve_coeff_in_CH.keys())
            strr = ''
            for i in self.Dic_recieve_coeff_in_CH.keys():
                strr = strr + ',' + str(i)

        self.accuracy = self.get_accuracy()


        #=== set accuracy ========
        for agent in self.Dic_recieve_coeff_in_CH.keys():
            self.Dic_recieve_coeff_in_CH_previous[agent]['accuracy'].append(self.accuracy)

        #================

        # =========== send Coefficient to EPC
        self.hello_packet = self.create_Hello_packet()
        self.hello_packet['coefficients']=self.avg_coeff
        self.hello_packet['intercept_']=self.intercept_
        self.coefficients=self.avg_coeff
        self.producer_coeff_to_EPC.send('SendTOEPCCoeff_' + str(EPC_sourceID),
                                        value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder), 'utf-8'),
                                        )
        # recieve Coefficient from EPC
        self.comsumer_coeff_from_EPC = KafkaConsumer('RecieveFromEPCCoeff_' + str(self.SourceID),
                                                    bootstrap_servers=[Server_Address])
        msg_coff_fromEPC = self.comsumer_coeff_from_EPC.poll(timeout_ms=wait_recieve_coff_from_CMs * 1000)  # wait from now to timeout_ms for recieving all messages within time
        if not msg_coff_fromEPC:
            ''
        else:
            for topic_data, consumer_records in msg_coff_fromEPC.items():  # All messages
                for message in consumer_records:  # each message
                    mess_coeff_EPC = json.loads(message.value)
                    self.avg_coeff=mess_coeff_EPC['coefficients']
                    self.intercept_=mess_coeff_EPC['intercept_']



        # send Coefficient to all CMs
        for CM_ID in list_CMs_Source_IDs:
            self.hello_packet['coefficients'] = preprocess_coeff(self.avg_coeff)
            self.hello_packet['intercept_'] = preprocess_coeff(self.intercept_)
            time.sleep(2)
            try:
                self.producer_coeff_to_CM.send('RecieveCHCoeff_' + str(CM_ID),
                                               value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder),
                                                           'utf-8'))
            except Exception as e:
                print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
                print('Error in producer_coeff_to_CM')
                continue


        # break

    def CH_CM_RecieveHelloPacket(self):
        self.vib_neighbor = {}
        while(True):
            try:

                rnd = random.randint(1, 3)
                time.sleep(rnd)

                try:
                    self.dic_vehicl_times[int(self.SourceID)][self.timeChangePosition]
                    self.position_x = float(self.dic_vehicl_times[int(self.SourceID)][self.timeChangePosition]['x'])
                    self.position_y = float(self.dic_vehicl_times[int(self.SourceID)][self.timeChangePosition]['y'])
                    self.speed=float(self.dic_vehicl_times[int(self.SourceID)][self.timeChangePosition]['speed'])
                    self.timeChangePosition = self.timeChangePosition + 1
                except:
                    print('')
                    ''
                # ===============================================
                self.hello_packet = self.create_Hello_packet()
                try:
                    self.hello_packet['coefficients'].shape[0]
                except:
                    self.hello_packet['coefficients'] = ''

                # =============
                self.producer.send(public_topicc, value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder), 'utf-8'))

                time1 = datetime.datetime.now()
                # ==========

                try:
                    date2 = datetime.datetime.now()
                    self.consumer = KafkaConsumer(public_topicc, bootstrap_servers=[Server_Address])
                    #====================================
                    msg = self.consumer.poll(self.In_TIMER * 1000)

                    if not msg:
                        time.sleep(0.1)
                    else:
                        for topic_data, consumer_records in  msg.items():
                            for message in consumer_records:  # each message
                                mess = json.loads(message.value)
                                if (mess['SourceID']==self.SourceID):
                                    continue
                                #=======

                                point1 = numpy.array((self.position_x, self.position_y))
                                point2 = numpy.array((mess['position_x'], mess['position_y']))
                                distance = float(numpy.linalg.norm(point1 - point2))
                                if (float(distance) <= float(self.TRANMISSION_RANGE)):  # is Neighbor
                                    if (mess['SourceID'] not in self.vib_neighbor.keys()):  # ghablan nabashad
                                        self.vib_neighbor[mess['SourceID']] = {}
                                        self.vib_neighbor[mess['SourceID']]['direction'] = mess['direction']
                                        self.vib_neighbor[mess['SourceID']]['speed'] = mess['speed']
                                        self.vib_neighbor[mess['SourceID']]['number_of_hops_to_CH'] = mess[
                                            'number_of_hops_to_CH']
                                        self.vib_neighbor[mess['SourceID']]['current_clustering_state'] = mess[
                                            'current_clustering_state']
                                        self.vib_neighbor[mess['SourceID']]['distance'] = distance
                                        self.vib_neighbor[mess['SourceID']]['SourceID'] = mess['SourceID']
                                        self.vib_neighbor[mess['SourceID']]['avg_speed'] = float(mess['avg_speed'])
                                        self.vib_neighbor[mess['SourceID']]['V_state'] = str(mess['V_state'])
                                        self.vib_neighbor[mess['SourceID']]['TRY_CONNECT_CH'] = 0
                                        self.vib_neighbor[mess['SourceID']]['HOPE_CM'] = str(mess['HOPE_CM'])
                                        self.vib_neighbor[mess['SourceID']]['Member_CM'] = str(mess['Member_CM'])
                                        self.vib_neighbor[mess['SourceID']]['Member_CH'] = str(mess['Member_CH'])
                                        self.vib_neighbor[mess['SourceID']]['coefficients']=''
                                        self.vib_neighbor[mess['SourceID']]['intercept_']=''




                                    else:
                                        self.vib_neighbor[mess['SourceID']]['direction'] = mess['direction']
                                        self.vib_neighbor[mess['SourceID']]['speed'] = mess['speed']
                                        self.vib_neighbor[mess['SourceID']]['number_of_hops_to_CH'] = mess[
                                            'number_of_hops_to_CH']
                                        self.vib_neighbor[mess['SourceID']]['current_clustering_state'] = mess[
                                            'current_clustering_state']
                                        self.vib_neighbor[mess['SourceID']]['distance'] = distance
                                        self.vib_neighbor[mess['SourceID']]['SourceID'] = mess['SourceID']
                                        self.vib_neighbor[mess['SourceID']]['avg_speed'] = float(mess['avg_speed'])
                                        self.vib_neighbor[mess['SourceID']]['V_state'] = str(mess['V_state'])
                                        self.vib_neighbor[mess['SourceID']]['HOPE_CM'] = str(mess['HOPE_CM'])
                                        self.vib_neighbor[mess['SourceID']]['Member_CM'] = str(mess['Member_CM'])
                                        self.vib_neighbor[mess['SourceID']]['Member_CH'] = str(mess['Member_CH'])

                                    # for coefficients
                                    if (self.vib_neighbor[mess['SourceID']]['coefficients'] != ''):
                                        self.vib_neighbor[mess['SourceID']]['coefficients_tMinus1'] = str(
                                            self.vib_neighbor[mess['SourceID']]['coefficients'])
                                        self.vib_neighbor[mess['SourceID']]['intercept_tMinus1'] = str(
                                            self.vib_neighbor[mess['SourceID']]['intercept_'])

                                    self.vib_neighbor[mess['SourceID']]['coefficients'] = str(mess['coefficients'])
                                    self.vib_neighbor[mess['SourceID']]['intercept_'] = str(mess['intercept_'])






                except Exception as e:
                    print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
                    time.sleep(0.1)

                sum_minus_speed = 0
                N_i = 0
                if (len(list(self.vib_neighbor.keys())) > 1):

                for neigh in self.vib_neighbor.keys():
                    if (self.vib_neighbor[neigh]['direction'] == self.direction):  # dar yek direction bashad!!!
                        if (int(self.vib_neighbor[neigh]['number_of_hops_to_CH']) <= self.MAX_HOPs):  # within MAX_HOP
                            N_i = N_i + 1
                            speed_i = self.speed
                            speed_ij = self.vib_neighbor[neigh]['speed']
                            minus_speed = numpy.abs(speed_i - speed_ij)
                            sum_minus_speed = sum_minus_speed + minus_speed

                if (N_i != 0):
                    self.avg_speed = sum_minus_speed / N_i
                else:
                    self.avg_speed = self.speed

            except Exception as e:
                print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
                ''

    def CH_changeState(self):

        Join_Timer = 5
        try:
            Avg_CoSim_k= min(list(self.neighbor_Avg_CoSimCH.values()))   # minimum Avg_CoSimCH ghablihaa
        except:
            return 1
            Avg_CoSim_k=999999

        connectionCH_neighbor={}
        for neighbor in self.vib_neighbor.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'CH'):  # for all CH in vib


                avgSpeedCH=self.vib_neighbor[neighbor]['avg_speed']

                thetaCH = self.vib_neighbor[neighbor]['coefficients']
                try:
                    thetaCH_tminus1=self.vib_neighbor[neighbor]['coefficients_tMinus1']
                except:
                    print('Error in neighbor_Avg_CoSimCH')
                    thetaCH_tminus1=self.coefficients

                Avg_CoSimCH = calculate_Avg_CoSimCH_threeElements(self.alpha, avgSpeedCH,  thetaCH, thetaCH_tminus1)

                self.vib_neighbor[neighbor]['Avg_CoSimCH'] = Avg_CoSimCH
                connectionCH_neighbor[neighbor] = True
                self.neighbor_Avg_CoSimCH[neighbor] = Avg_CoSimCH

        self.neighbor_Avg_CoSimCH = dict(sorted(self.neighbor_Avg_CoSimCH.items(), key=lambda item: item[1]))

        for neighbor in self.neighbor_Avg_CoSimCH.keys():
            if (self.vib_neighbor[neighbor]['V_state'] == 'CH'):
                if (self.neighbor_Avg_CoSimCH[neighbor]<Avg_CoSim_k):
                    # ===============send connect_request ===========
                    if (connectionCH_neighbor[neighbor] == True):
                        self.consumer_respect_CH = KafkaConsumer('Connect_Response_' + str(self.SourceID),
                                                                 bootstrap_servers=[Server_Address])

                        self.producer_request_CH.send('Connect_Request_' + str(neighbor),
                                                      value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder),
                                                                  'utf-8'))
                        msg_respect = self.consumer_respect_CH.poll(timeout_ms=Join_Timer * 1000)
                        if not msg_respect:
                            ''
                        else:
                            for topic_data, consumer_records in msg_respect.items():
                                message_resp = consumer_records[0].value
                                mess_resp = json.loads(message_resp)

                                self.ID_to_CH = mess_resp['SourceID']
                                self.mess_resp = mess_resp
                                if (self.V_state == 'CH'):
                                    self.V_state = 'CM'
                                    return 1


    def CHstate_algorithm(self):
        while(True):
            try:
                self.CH_HFL()
                self.CH_changeState()
                if (self.V_state != 'CH'):
                    break
            except:
                continue


    def NoClustering_HFL(self):
        while(True):
            try:
                time.sleep(random.randint(1,5))

                self.Train_Update_local_SGD()

                # =========== send Coefficient to EPC
                wait_recieve_coff_from_EPC=5
                self.hello_packet = self.create_Hello_packet()
                self.hello_packet['coefficients']=self.coefficients
                self.hello_packet['intercept_']=self.intercept_
                self.producer_coeff_to_EPC.send('SendTOEPCCoeff_' + str(EPC_sourceID),
                                                value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder), 'utf-8'),
                                                )
                # recieve Coefficient from EPC
                self.comsumer_coeff_from_EPC = KafkaConsumer('RecieveFromEPCCoeff_' + str(self.SourceID),
                                                            bootstrap_servers=[Server_Address])
                msg_coff_fromEPC = self.comsumer_coeff_from_EPC.poll(timeout_ms=wait_recieve_coff_from_EPC * 1000)  # wait from now to timeout_ms for recieving all messages within time
                if not msg_coff_fromEPC:
                    ''
                else:
                    for topic_data, consumer_records in msg_coff_fromEPC.items():  # All messages
                        for message in consumer_records:  # each message
                            mess_coeff_EPC = json.loads(message.value)
                            self.coefficients=mess_coeff_EPC['coefficients']
                            self.intercept_=mess_coeff_EPC['intercept_']

            except:
                continue


    def CM_HFL(self):
        self.have_coef=0
        # recieve Coefficient from CH
        self.consumer_coeff_from_CH = KafkaConsumer('RecieveCHCoeff_' + str(self.SourceID),
                                                    bootstrap_servers=[
                                                        Server_Address])
        self.msg_coef = self.consumer_coeff_from_CH.poll(timeout_ms=20 * 1000)
        if not self.msg_coef:
            # print('Not Recieve Coeff from CH')
            ''
        else:
            for topic_data, consumer_records in self.msg_coef.items():
                for message in consumer_records:  # each message
                    if (self.have_coef==1):
                        break
                    self.mess_from_CH = json.loads(message.value)
                    try:
                        self.coefficients = numpy.asarray(self.mess_from_CH['coefficients'])
                        self.intercept_ = numpy.asarray(self.mess_from_CH['intercept_'])
                        self.have_coef = 1
                    except:
                        continue

                    print('EXITTTT')
                    # ====

        self.Train_Update_local_SGD()
        self.hello_packet = self.create_Hello_packet()
        self.producer_coeff_to_CH.send('SendCHCoeff_' + str(self.ID_to_CH),
                                       value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder), 'utf-8'))

    def CM_changeState(self):
        TIMER_CM_MAX=5
        if (self.have_coef==1):
            self.timer_CM=0
        else:
            self.timer_CM=self.timer_CM+1

        if (self.timer_CM>TIMER_CM_MAX):
            self.V_state='SE'



    def CMstate_algorithm(self):
        while (True):
            self.CM_HFL()
            self.CM_changeState()
            if (self.V_state != 'CM'):
             break
        pass

    # =====
    # Define EPC
    # =====

    def EPC(self):
        dic_EPCcoeffiecients = {}
        dic_EPCintercept = {}
        list_accuracy = list()
        while (True):
            try:
                list_CH_recieved=list()
                self.consumer_EPC = KafkaConsumer('SendTOEPCCoeff_' + str(EPC_sourceID),
                                             bootstrap_servers=[Server_Address])
                self.msg_coff_EPC = self.consumer_EPC.poll(
                    timeout_ms=5 * 1000)
                if not self.msg_coff_EPC:
                    ''
                else:
                    for topic_data, consumer_records in self.msg_coff_EPC.items():  # All messages
                        # ======== Reviece Coeff from CHs
                        for message in consumer_records:  # each message
                            self.mess_from_CH_EPC = json.loads(message.value)

                            dic_EPCcoeffiecients[self.mess_from_CH_EPC['SourceID']] = numpy.asarray(
                                self.mess_from_CH_EPC['coefficients'])
                            dic_EPCintercept[self.mess_from_CH_EPC['SourceID']] = numpy.asarray(
                                self.mess_from_CH_EPC['intercept_'])

                            list_CH_recieved.append(self.mess_from_CH_EPC['SourceID'])


                #==============
                # New ATTACK algorithm
                #================
                for vehicleCH in dic_EPCcoeffiecients.keys():
                    cosSim=cosineSimilarity(self.Dic_recieve_coeff_in_EPC_previous[vehicleCH]['coefficients'][-1],
                                            dic_EPCcoeffiecients[vehicleCH])

                    if (cosSim>0.5):
                        self.Dic_recieve_coeff_in_EPC_previous[vehicleCH]['Block_flag']=True
                        self.block_duration=self.block_duration+1
                        self.Dic_recieve_coeff_in_EPC_previous[vehicleCH]['state']='SE'
                    else:
                        self.Dic_recieve_coeff_in_EPC_previous[vehicleCH]['state'] = 'CH'




                    self.Dic_recieve_coeff_in_EPC_previous[vehicleCH]['coefficients'].append(
                        dic_EPCcoeffiecients[vehicleCH])


                avg_coeff_EPC = ''
                avg_intercept_EPC = ''

                for item in dic_EPCcoeffiecients.keys():

                    if (self.Dic_recieve_coeff_in_EPC_previous[item]['state']!='CH'):
                        continue

                    try:
                        if (avg_coeff_EPC == ''):
                            avg_coeff_EPC = numpy.array(dic_EPCcoeffiecients[item])
                        else:
                            avg_coeff_EPC = numpy.array(avg_coeff_EPC) + numpy.array(dic_EPCcoeffiecients[item])
                    except:
                        continue



                for item in dic_EPCintercept.keys():

                    if (self.Dic_recieve_coeff_in_EPC_previous[item]['state']!='CH'):
                        continue


                    try:
                        if (avg_intercept_EPC == ''):
                            avg_intercept_EPC = numpy.array(dic_EPCintercept[item])
                        else:
                            avg_intercept_EPC = avg_intercept_EPC + numpy.array(dic_EPCintercept[item])
                    except:
                        continue





                if (len(dic_EPCintercept.keys()) != 0):
                    average_avg_coeff_EPC = numpy.asarray(avg_coeff_EPC) / len(dic_EPCintercept.keys())
                    try:
                        avg_intercept_EPC = numpy.asarray(avg_intercept_EPC) / len(dic_EPCintercept.keys())
                    except:
                        avg_intercept_EPC=average_avg_coeff_EPC



                    sgd_classifier_EPC.coef_ = average_avg_coeff_EPC
                    sgd_classifier_EPC.intercept_ = avg_intercept_EPC

                    try:
                        sgd_classifier_EPC.partial_fit(self.X_data, self.Y_data, classes=numpy.unique(self.Y_data))
                    except:
                        sgd_classifier_EPC.fit(self.X_data, self.Y_data)

                    acc = sgd_classifier_EPC.score(self.X_data_test, self.Y_data_test)

                    if (len(list_accuracy) == 0 or list_accuracy[-1] != acc):
                        list_accuracy.append(acc)

                    lst_acc.append(str(acc))
                    self.coefficients=avg_coeff_EPC
                    self.intercept_=avg_intercept_EPC

                    hello_packet = self.create_Hello_packet()
                    hello_packet['coefficients'] = avg_coeff_EPC
                    hello_packet['intercept_'] = avg_intercept_EPC

                    if (hello_packet['coefficients']  == 0):
                        hello_packet['coefficients']  = ''
                    if (hello_packet['intercept_'] == 0):
                        hello_packet['intercept_'] = ''


                    for CHnode in list_CH_recieved:
                        self.producer_EPC_coeff_to_CH.send('RecieveFromEPCCoeff_' + str(CHnode),
                                                   value=bytes(dumps(self.hello_packet, cls=NumpyArrayEncoder), 'utf-8'),
                                                   )

            except Exception as e:
                print('Error on line {}'.format(sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
                continue

    def Main_Algorithm(self):
        while(True):

            try:
                if (self.V_state=='SE'):
                    print('CHsection')
                    try:
                        self.SA_State_CHsection()  # OK
                        time.sleep(random.randint(1,7))
                    except:
                        ''
                    print('CMsection')
                    try:
                        self.SA_State_CMsection()  # OK
                        time.sleep(random.randint(1, 7))
                    except:
                        ''

                    print('SEsection')
                    try:
                        self.SA_State_SEsection()  # OK
                        time.sleep(random.randint(1, 7))
                    except:
                        ''
                else:
                    if (self.V_state=='CH'):
                        try:
                            self.CHstate_algorithm()   # OK
                            time.sleep(random.randint(1, 2))
                        except:
                            ''
                    else:
                        if (self.V_state=='CM'):
                            try:
                                self.CMstate_algorithm()   #OK
                                time.sleep(random.randint(1, 2))
                            except:
                                ''
            except:
                continue

import time
import json
def save_result_Json(Dic_CH_accuracy,lst_acc,numberVehicles):
    dump_Dic_CH_accuracy=json.dumps(Dic_CH_accuracy)
    fp=open('dumpDic_CH_accuracy_NEW'+str(numberVehicles)+'.txt','w',encoding='utf8')
    fp.write(dump_Dic_CH_accuracy)
    fp.close()

    dump_lst_acc=json.dumps(lst_acc)
    fp=open('dumplst_acc_NEW'+str(numberVehicles)+'.txt','w',encoding='utf8')
    fp.write(dump_lst_acc)
    fp.close()

    return
def load_result_json(path='dumpDic_CH_accuracy.txt',path_lst_acc='dumplst_acc.txt'):
    fp1=open(path,'r',encoding='utf8')
    rr=fp1.read()
    diccc=json.loads(rr)

    fp2=open(path_lst_acc,'r',encoding='utf8')
    rr2=fp2.read()
    lst_acc_dic=json.loads(rr2)


    return diccc,lst_acc_dic

def plot_accuracy(Dic_CH_accuracy,lst_acc):
    for CH in Dic_CH_accuracy.keys():
        lst_ch = Dic_CH_accuracy[CH]
        x = [i for i in range(0, len(lst_ch))]
        y = lst_ch
        plt.plot(x, y, marker='o', label='CH_node' + CH)
        plt.xlabel('Communication Rounds')
        plt.ylabel('Accuracy')
        plt.title('Comparisons')
        leg = plt.legend()
    plt.show()


    # save as SVG
    image_format = 'svg'  # e.g .png, .svg, etc.
    image_name = 'Comparisons.svg'
    plt.savefig(image_name, format=image_format, dpi=2000)

    x = [i for i in range(0, len(lst_acc))]

    y = list()
    for j in lst_acc:
        y.append(round(float(j), 2))
    plt.plot(x, y, marker='o', label='EPC_Node')
    plt.xlabel('Communication Rounds')
    plt.ylabel('Accuracy')
    plt.title('EPC results')
    leg = plt.legend()
    plt.show()
    print('Finish Plot')

    image_format = 'svg'  # e.g .png, .svg, etc.
    image_name = 'EPC results.svg'
    plt.savefig(image_name, format=image_format)

import matplotlib.pyplot as plt

import random
list_vehicles = list()
def define_vehicles(list_vehicles,numberOfVehicles,dic_vehicl_times,haveClustering):
    from sklearn.model_selection import train_test_split
    list_data_vehicles_X_data, list_data_vehicles_y_data, all_X_data, all_y_data = load_minist_data(count_vehicles=numberOfVehicles+1)
    X_train, X_test, y_train, y_test = train_test_split(all_X_data, all_y_data, test_size=0.33, random_state=42)

    select_veh=int(numberOfVehicles*0.1)
    lst_veh=random.sample(range(0, numberOfVehicles), select_veh)

    for i in range(1,numberOfVehicles+1):
        try:
            change_x=float(dic_vehicl_times[i][0]['x'])
            change_y=float(dic_vehicl_times[i][0]['y'])
            change_speed=float(dic_vehicl_times[i][0]['speed'])
            change_delay = random.randint(1, 2)
        except:
            change_x = random.randint(10, 50)
            change_y = random.randint(10, 50)
            change_speed = random.randint(10, 50)
            change_delay = random.randint(1, 2)

        if (i in lst_veh): # select for noise
            list_vehicles.append(
                vehicle_VIB(change_x, change_y, change_speed, str(i), list_data_vehicles_X_data[i], list_data_vehicles_y_data[i], X_test, y_test,
                            100,dic_vehicl_times,'0',haveClustering,True))
        else:
            list_vehicles.append(
                vehicle_VIB(change_x, change_y, change_speed, str(i), list_data_vehicles_X_data[i], list_data_vehicles_y_data[i], X_test, y_test,
                            100,dic_vehicl_times,'0',haveClustering,False))
        time.sleep(change_delay)

    #========================
    # Create EPC

    vehicle_VIB(change_x, change_y, change_speed, '999', X_train, y_train, X_test,
                y_test,
                100, dic_vehicl_times, '1',haveClustering,False)

    #============================

    return X_train,y_train,X_test, y_test

#======
from Read_XML import get_dic_vehicl_times
numberOfVehicles=10
dic_vehicl_times=get_dic_vehicl_times()
EPC_sourceID = 999
haveClustering='1'
X_train,y_train,X_test, y_test=define_vehicles(list_vehicles,numberOfVehicles,dic_vehicl_times,haveClustering)
print('finish')

# ===================
# SHOW RESULTs
# =================

# Dic_CH_Coeff
from prettytable import PrettyTable

sgd_classifier_EPC = SGDClassifier(random_state=42, max_iter=100)
lst_str_CH_bodes = list()
lst_str_CH_bodes.append('')

lst_acc = list()
lst_acc.append(0)





def Plot_EPC_Accuracy_Table(X_train,y_train,X_test, y_test):
    my_table = PrettyTable()
    my_table.field_names = ["CH_Nodes", "Accuracy"]
    training_data = X_train
    training_y = y_train

    count_round = 100000000
    list_accuracy = list()
    while (True):
        try:
            avg_coeff_EPC = ''
            avg_intercept_EPC = ''
            Dic_CH_Coeff_temp=Dic_CH_Coeff.copy()
            for item in Dic_CH_Coeff_temp.keys():
                if (avg_coeff_EPC == ''):
                    avg_coeff_EPC = numpy.array(Dic_CH_Coeff_temp[item]['avg_coeff'])

                else:
                    avg_coeff_EPC = numpy.array(avg_coeff_EPC) + numpy.array(Dic_CH_Coeff_temp[item]['avg_coeff'])

                if (avg_intercept_EPC == ''):
                    avg_intercept_EPC = numpy.array(Dic_CH_Coeff_temp[item]['intercept_'])
                else:
                    avg_intercept_EPC = avg_intercept_EPC + numpy.array(Dic_CH_Coeff_temp[item]['intercept_'])

            if (len(Dic_CH_Coeff_temp.keys()) != 0):
                average_avg_coeff_EPC = numpy.asarray(avg_coeff_EPC) / len(Dic_CH_Coeff_temp.keys())
                avg_intercept_EPC = numpy.asarray(avg_intercept_EPC) / len(Dic_CH_Coeff_temp.keys())

                sgd_classifier_EPC.coef_ = average_avg_coeff_EPC
                sgd_classifier_EPC.intercept_ = avg_intercept_EPC

                try:
                    sgd_classifier_EPC.partial_fit(training_data, training_y, classes=numpy.unique(training_y))
                except:
                    sgd_classifier_EPC.fit(training_data, training_y)

                acc = sgd_classifier_EPC.score(X_test, y_test)

                if (len(list_accuracy) == 0 or list_accuracy[-1] != acc):
                    list_accuracy.append(acc)
                    count_round = count_round - 1

                lst_str_CH_bodes[0] = ','.join(list(Dic_CH_Coeff_temp.keys()))
                lst_acc.append(str(acc))

                if (count_round == 0):
                    break
            time.sleep(2)
        except:
            continue

def Plot_CH_Accuracy_Table(numberVehicles):
    Dic_CH_accuracy = {}
    count_round = 1000
    while (True):

        #=======
        # Write to File
        #======
        save_result_Json(Dic_CH_accuracy,lst_acc,numberVehicles)
        #==================




        my_table = PrettyTable()
        my_table.field_names = ["SourceID", "State", "CH_Node", "AssignedNodes", "Accuracy", "EPC_CHNodes",
                                "EPC_Accuracy"]

        for item in list_vehicles:
            try:
                AssignedNodes = ','.join(item.Dic_recieve_coeff_in_CH.keys())
            except:
                AssignedNodes = '-'
            my_table.add_row(
                [str(item.SourceID), str(item.V_state), str(item.ID_to_CH), str(AssignedNodes), str(item.accuracy),
                 str(lst_str_CH_bodes[0]), str(lst_acc[-1])])
            if (item.V_state == 'CH'):
                if (item.SourceID not in Dic_CH_accuracy.keys()):
                    Dic_CH_accuracy[item.SourceID] = list()
                    Dic_CH_accuracy[item.SourceID].append(0)
                if (Dic_CH_accuracy[item.SourceID][-1] != float(item.accuracy)):
                    Dic_CH_accuracy[item.SourceID].append(float(item.accuracy))
                    count_round = count_round - 1
        if (count_round == 0):
            break

        print(my_table)
        time.sleep(1)

    return Dic_CH_accuracy

Dic_CH_accuracy = Plot_CH_Accuracy_Table(numberOfVehicles)
plot_accuracy(Dic_CH_accuracy,lst_acc)