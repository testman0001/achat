#!/usr/bin/env python3
# -*- coding:UTF-8 -*-
import argparse
from serv import *
from common import MasterRecivePacketType, WorkerRecivePacketType
class Master(Server):
    """
    对比普通的server，多了以下改动：
    1.支持worker的注册、移除、心跳检测，以及worker负责的在线用户集合维护
    2.离线消息同步任务的转发，收到worker的同步任务转发请求时，将同步任务分配给负责的worker

    Args:
        Server ([type]): [description]
    """
    def __init__(self, config_file="./config.json"):
        """
        改写初始化函数
        """
        #{worker_id:{"socket":socket, "last_heartbeat_time_ms":time_ms,
        #            "server_ip":ip, "server_port":port, "user_set":{}
        #             "user_num":num}}
        self._worker_message = {}
        with open(config_file) as fp:
            cfg = json.load(fp)
            self._heartbeat_timeout_ms = cfg["master"]["heartbeat_timeout_ms"]        
        return super().__init__(config_file)

    def _worker_id_generate(self):
        """
        按当前socket生成worker_id
        """
        worker_addr = self._current_socket.getpeername()
        worker_id = '{}_{}'.format(worker_addr[0], worker_addr[1])
        return worker_id

    def _worker_register(self, ip , port):
        """
        worker注册

        Args:
            ip ([type]): [description]
            port ([type]): [description]
        """
        worker_id = self._worker_id_generate()
        if worker_id in self._worker_message.keys():
            self._logger.log_error("worker already registered")
        self._worker_message[worker_id] = {}
        self._worker_message[worker_id]["socket"] = self._current_socket
        self._worker_message[worker_id]["last_heartbeat_time_ms"] = current_time_ms()
        self._worker_message[worker_id]["server_ip"] = ip
        self._worker_message[worker_id]["server_port"] = port
        self._worker_message[worker_id]["user_set"] = set()
        self._worker_message[worker_id]["user_num"] = 0

    def _worker_get(self, username):
        """
        为用户分配worker，目前策略是返回用户最少的worker

        Args:
            username ([type]): [description]
        """
        return min(self._worker_message.keys(), key=(lambda k:self._worker_message[k]["user_num"]))

    def _worker_notify_to_user(self, username):
        """
        通知用户分配到的worker地址
        Args:
            username ([type]): [description]
        """
        if not self._worker_message:
            return
        worker = self._worker_get(username)
        self._response_to_client("server_update_req", {
            "server_ip":self._worker_message[worker]["server_ip"],
            "server_port":self._worker_message[worker]["server_port"]
            }, self._current_socket)

    def _worker_remove(self, worker_id):
        """
        移除worker

        Args:
            worker_id ([type]): [description]
        """
        # # 移除前，先为在线用户重新分配worker
        # user_set = self._worker_message[worker_id]["user_set"] 
        # for user in user_set:
        #     if self._redis.user_info_get(user, "state") == OFFLINE:
        #         continue
        #     self._worker_notify(user)
        self._worker_message.pop(worker_id)

    def _user_login(self, username):
        """
        改写用户登录函数，收到登录请求时直接通知用户新worker的地址
        Args:
            username ([type]): [description]
        """
        self._worker_notify_to_user(username)
        return SUCCESS

    def _login_success(self, username):
        """
        处理worker发送过来的用户成功登录事件

        Args:
            username ([type]): [description]
        """
        worker_id = self._worker_id_generate()
        self._worker_message[worker_id]["user_set"].add(username)
        self._worker_message[worker_id]["user_num"] += 1

    def _message_sync_forward(self, reciver_list):
        """
        处理worker转发过来离线消息同步请求，通知对应的worker去处理

        Args:
            reciver_list ([type]): [description]
        """
        reciver_set = set(reciver_list)
        for worker in self._worker_message.keys():
            reciver_to_worker = list(self._worker_message[worker]["user_set"] & reciver_set)
            self._response_to_client("message_sync_forward", {
                "version":0,
                "reciver_list":reciver_to_worker
                }, self._worker_message[worker]["socket"] )

    def _packet_process_flow(self, data):
        """
        改写master收包处理流程，主要是对处理请求类型作出限制
        """
        # 先解析数据包，获得字典形式的返回
        data_dict = json.loads(base64.b64decode(data).decode("utf-8"))
        self._logger.log_debug(data_dict)
        packet_type = data_dict['packet_type']
        # 获取事务id
        if packet_type in ServerRecivePacketType:
            self._current_transaction_id = data_dict['transaction_id']
        # 分发给具体函数处理，对应方法在枚举类中
        # 改写后限制master所能处理的类型
        ret = self._packet_dispatch(data_dict, MasterRecivePacketType)
        if ret == FAIL:
            self._logger.log_error("RequestHandlerError:packet type:{}".format(packet_type))
        # 没必要通知请求者，直接退出
        if ret == PROCESSING:
            return
        # worker的请求
        if packet_type not in ServerRecivePacketType:
            return
        # 发送回应给请求者
        self._response_to_client("simple_result_resp", {
            "transaction_id":self._current_transaction_id,
            "result":ret
            }, self._current_socket)

    def _heartbeat(self):
        """
        处理接收到的心跳包
        """
        worker_id = self._worker_id_generate()
        self._worker_message[worker_id]["last_heartbeat_time_ms"] = current_time_ms()

    def _heartbeat_timeout(self, worker_id, current_time):
        """
        判断worker是否心跳已经超时

        Args:
            worker_id ([type]): [description]
            current_time ([type]): [description]

        Returns:
            [type]: [description]
        """
        pass_time = current_time - self._worker_message[worker_id]["last_heartbeat_time_ms"]
        return pass_time > self._heartbeat_timeout_ms

    def _server_cron_task(self):
        """
        改写定时任务，判断worker是否还在线，判断worker的用户是否已经离线
        """
        current_time = current_time_ms()
        for worker in list(self._worker_message.keys()):
            # 检查心跳是否超时
            if self._heartbeat_timeout(worker, current_time):
                self._logger.log_error("worker:{} heartbeat detection timeout".format(worker))
                self._worker_remove(worker)
                continue
            # 检查用户是否离线
            for user in list(self._worker_message[worker]["user_set"]):
                if self._redis.user_info_get(user, "state") == OFFLINE:
                    self._worker_message[worker]["user_set"].remove(user)

class Worker(Server):
    """
    集群worker， 继承自Server
    对比普通的server，主要多了以下改动：
    1.支持worker自身的注册，
    2.定时向master发送心跳包
    3.支持离线消息同步任务的转发

    Args:
        Server ([type]): [description]
    """
    def _redis_init(self):
        """
        改写redis初始化函数，去除最大消息量初始化部分
        """
        self._redis = redis_db.RedisDb(self._redis_ip, self._redis_port)
        return True

    def worker_register(self, ip, port):
        """
        注册worker

        Args:
            ip ([type]): [description]
            port ([type]): [description]

        Returns:
            [type]: [description]
        """
        try:
            self._worker_socket = socket(AF_INET, SOCK_STREAM)
            self._worker_socket.connect((ip, port))
            self._response_to_client("worker_register",{
                "version":0,
                "ip":self._server_ip,
                "port":self._server_port
                },self._worker_socket)
        except error as e:
             return False
        return True

    def _offline_message_sync(self, reciver_list):
        """
        改写同步离线消息，添加转发同步任务到master
        注意：只处理本服务端上登录的用户的同步任务，登录在其他服务端上的由其他服务端自己处理，避免创建大量tcp连接
        Args:
            reciver_list ([type]): [description]
        """
        reciver_set = set(reciver_list)
        # 获取本服务端上已登录的用户
        login_list = list(self._user_to_socket.keys())
        forward_reciver_set = set()
        for user in reciver_set:
            # 不在线
            if self._redis.user_info_get(user, "state") != ONLINE:
                continue
            # 在线，但用户已经不在本服务端上登录，这是集群模式考虑的场景
            if user not in login_list:
                forward_reciver_set.add(user)
                continue
            # 用户在本机登录，且处于在线状态，本机尝试将离线信息信息同步给用户
            recive_id_list = self._redis.message_all(user)
            user_socket = self._socket_get_by_user(user)
            for recive_id in recive_id_list:
                message = self._redis.message_get(user, recive_id)
                message["recive_message_id"] = recive_id
                self._response_to_client("message_recive", message, user_socket)
        if not forward_reciver_set:
            return
        # 告知master自己不能处理的用户列表
        self._response_to_client("message_sync_forward", {
                "version":0,
                "reciver_list":list(forward_reciver_set)
                }, self._worker_socket)

    def _message_sync_forward(self, reciver_list):
        """
        处理master转发过来的离线消息同步任务

        Args:
            reciver_list ([type]): [description]
        """
        self._offline_message_sync(reciver_list)

    def _user_login(self, username, password):
        """
        改写用户登录，主要是通知master登录成功
        发送给用户一个token，有过期时间，用户放到消息里免登录
        Args:
            username ([type]): [description]
            password ([type]): [description]
        """
        if not self._redis.user_exists(username):
            return FAIL
        if password != self._redis.user_info_get(username, "password"):
            return FAIL
        # 更新用户状态
        self._redis.user_info_set(username, "state", ONLINE)
        self._socket_to_user[self._current_socket] = username
        self._user_to_socket[username] = self._current_socket
        # 设置token
        token = str(uuid4())
        self._redis.token_set(username, token, self._redis_expire_s)
        # 暂时策略是登录时清空发送消息buffer，后续可能改成强制处理完
        self._send_message_buffer[username] = {}
        self._send_message_buffer[username]["expect_id"] = 0
        self._response_to_client("login_success",{
            "version":0,
            "username":username
            },self._worker_socket)
        # 通知用户更新，告知token
        self._response_to_client("token_update_req", {
            "token":token
        }, self._current_socket)
        # 登录时触发离线消息同步
        self._offline_message_sync([username])
        return SUCCESS

    def _packet_process_flow(self, data):
        """
        改写收包解析函数，主要是限制worker处理的请求类型
        """
        # 先解析数据包，获得字典形式的返回
        data_dict = json.loads(base64.b64decode(data).decode("utf-8"))
        self._logger.log_debug(data_dict)
        packet_type = data_dict['packet_type']
        # 获取事务id
        if packet_type in [e.name for e in ServerRecivePacketType]:
            self._current_transaction_id = data_dict['transaction_id']
        # 分发给具体函数处理，对应方法在枚举类中
        # 改写后限制master所能处理的类型：用户注册、登录、worker注册
        ret = self._packet_dispatch(data_dict, WorkerRecivePacketType)
        if ret == FAIL:
            self._logger.log_error("RequestHandlerError:packet type:{}".format(packet_type))
        # 没必要通知请求者，直接退出
        if ret == PROCESSING:
            return
        # 来自master的请求
        if packet_type not in [e.name for e in ServerRecivePacketType]:
            return
        # 发送回应给请求者
        self._response_to_client("simple_result_resp", {
            "transaction_id":self._current_transaction_id,
            "result":ret
            }, self._current_socket)

    def _server_cron_task(self):
        """
        改写定时任务，额外向master发送心跳
        """
        self._response_to_client("heartbeat", {"version":0}, self._worker_socket)
        super()._server_cron_task()

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode','-m', default="master", choices=["master", "worker"])
    parser.add_argument('--config', default="./config.json")
    parser.add_argument('--ip', default="127.0.0.1")
    parser.add_argument('--port', default="58771")
    input_args = parser.parse_args()
    if input_args.mode == "master":
        server = Master(input_args.config)
    else:
        server = Worker(input_args.config)
        if not server.worker_register(input_args.ip, int(input_args.port)):
            print("failed to connect ({}:{})".format(input_args.ip, input_args.port))
            exit()
    server.run()