#!/usr/bin/env python3
# -*- coding:UTF-8 -*-
import json
import sys
import select
import inspect
import argparse
import struct
import base64
import signal
import time
from socket import *
from socket import error as socket_error
from uuid import uuid4

#导入其他模块
import redis_db
import mysql_db
from log import *
from common import *

def current_time_ms():
    return int(round(time.time() * 1000))

class Server(object):
    def __init__(self, config_file="./config.json"):
        with open(config_file) as fp:
            cfg = json.load(fp)
            self._server_port = cfg["server"]["server_port"]
            self._server_ip= cfg["server"]["server_ip"]
            self._max_connect_num = cfg["server"]["max_connect_num"]
            self._max_send_message = cfg["server"]["max_send_message"]
            self._checktime_s = cfg["server"]["checktime_s"]
            self._send_message_timeout_ms = cfg["server"]["send_message_timeout_ms"]
            self._log_path = cfg["server"]["log_path"]
            self._log_level = cfg["server"]["log_level"]
            self._redis_ip = cfg["redis"]["ip"]
            self._redis_port = cfg["redis"]["port"]
            self._redis_expire_s = cfg["redis"]["expire_s"]
            self._mysql_ip = cfg["mysql"]["ip"]
            self._mysql_user = cfg["mysql"]["user"]
            self._mysql_password = cfg["mysql"]["password"]
            self._max_offline_message = cfg["server"]["max_offline_message"]
        self._current_socket = None
        self._current_transaction_id = None
        # 存放发送消息列表, {sender_name : {"expect_id":id, id: data, id: data},...],..}
        self._send_message_buffer = {}
        self._socket_to_user = {}
        self._user_to_socket = {}

    def _log_init(self):
        """
        日志初始化

        Returns:
            [type]: [description]
        """
        self._logger = Logger(self._log_path, self._log_level)
        return self._logger

    def _server_socket_init(self):
        """
        服务端socket初始化

        Returns:
            [type]: [description]
        """
        try:
            self._server_socket = socket(AF_INET, SOCK_STREAM)
            self._server_socket.bind((self._server_ip, self._server_port))
            # 禁用nagle，降低延迟
            # self._server_socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, True)
            self._server_socket.listen(self._max_connect_num)
            self.fd_set = [self._server_socket]
        except Exception as socket_erro:
            self._logger.log_error("ERROR: {}".format(repr(socket_erro)))
            return False
        return True

    def _redis_init(self):
        """
        redis初始化
        """
        try:
            self._redis = redis_db.RedisDb(self._redis_ip, self._redis_port)
            self._redis.max_offline_message_set(self._max_offline_message)
        except Exception as e:
            self._logger.log_error("ERROR:{}".format(repr(e)))
            return False
        return True

    def _mysql_init(self):
        """
        mysql初始化
        """
        try:
            self._mysql = mysql_db.Mysqldb(self._mysql_ip, self._mysql_user, self._mysql_password)
        except Exception as e:
            self._logger.log_error("ERROR:{}".format(repr(e)))
            return False
        return True

    def _server_init(self):
        if not self._log_init():
            self._logger.log_error("log init failed")
            exit()
        if not self._server_socket_init():
            self._logger.log_error("socket init failed")
            exit()
        if self._redis_init() == None:
            self._logger.log_error("redis init failed")
            exit()
        if self._mysql_init() == None:
            self._logger.log_error("mysql init failed")
            exit()
        self._logger.log_info("server initialized successfully")

    def _socket_get_by_user(self, username):
        """
        由用户名获取socket

        Args:
            username ([type]): [description]
        """
        # 已经在本服务器上与目标用户建立了连接
        if username in self._user_to_socket.keys():
            return self._user_to_socket[username]
        return None

    def _response_to_client(self, packet_type, args_dict, socket_fd):
        """
        生成发往客户端的响应
        格式为json字符串+base64编码
        Args:
            packet_type ([type]): [description]
            args_dict ([type]): [description]
            socket_fd ([type]): [目标客户端socket]

        Returns:
            [type]: [description]
        """
        args_dict['packet_type'] = packet_type
        data = base64.b64encode(json.dumps(args_dict).encode("utf-8"))
        socket_fd.sendall(struct.pack('i', len(data)))
        socket_fd.sendall(data)

    def _user_register(self, username, password):
        """
        用户注册
        Args:
            username ([type]): [description]
            password ([type]): [description]
        """
        if self._redis.user_exists(username):
            return FAIL
        self._redis.user_add(username)
        self._redis.user_info_set(username, "state", OFFLINE)
        self._redis.user_info_set(username, "password", password)
        self._redis.user_info_set(username, "next_recive_id", 0)
        return SUCCESS

    def _user_login(self, username, password):
        """
        用户登录
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
        # 通知用户更新，告知token
        self._response_to_client("token_update_req", {
            "token":token
        }, self._current_socket)
        # 登录时触发离线消息同步
        self._offline_message_sync([username])
        return SUCCESS

    def _user_logout(self, req_user, username):
        """
        用户登出，用户自身或者管理员都可让用户登出
        Args:
            req_user ([type]): [请求发起用户]
            username ([type]): [要登出的用户名]
        """
        if not self._redis.user_exists(username):
            return FAIL
        if req_user == "admin" or req_user == username:
            # 删除username对应的token
            self._redis.token_delete(username)
            # 设为离线
            self._redis.user_info_set(username, "state", OFFLINE)
            if username in self._user_to_socket.keys():
                self._user_to_socket.pop(username)
            return SUCCESS
        return FAIL

    def _user_logout_by_socket(self, socket_fd):
        """
        使用对应socket的用户登出
        主要当socket关闭时触发
        Args:
            socket_fd ([type]): [description]
        """
        if socket_fd not in self._socket_to_user.keys():
            return
        username = self._socket_to_user[socket_fd]
        # 登出用户
        if not self._user_logout(username, username):
            self._logger.log_error("user:{} logout failed".format(username))

    def _user_delete(self, req_user, username):
        """
        用户删除，只允许用户自身或者管理员删除
        Args:
            req_user ([type]): [请求发起用户]
            username ([type]): [要删除的用户名]
        """
        if not self._redis.user_exists(username):
            return FAIL
        if req_user == "admin" or req_user == username:
            # 删除某个用户的全部相关信息
            self._redis.user_remove(username)
            # 发送消息buffer删除
            self._send_message_buffer.pop(username)
            return SUCCESS
        return FAIL

    def _friend_add(self, req_user, friend_name):
        """
        好友添加
        Args:
            req_user ([type]): [请求发起用户]
            friend_name ([type]): [好友名]
        """
        if not self._redis.user_exists(friend_name):
            return FAIL
        # 暂时不做验证，直接实现添加好友
        self._redis.friend_add(req_user, friend_name)
        self._redis.friend_add(friend_name, req_user)
        return SUCCESS
    
    def _friend_delete(self, req_user, friend_name):
        """
        好友删除
        Args:
            req_user ([type]): [请求发起用户]
            friend_name ([type]): [好友名]
        """
        if not self._redis.user_exists(friend_name):
            return FAIL
        self._redis.friend_delete(req_user, friend_name)
        self._redis.friend_delete(friend_name, req_user)
        return SUCCESS

    def _friend_list(self, req_user):
        """
        好友列表
        Args:
            req_user ([type]): [请求发起用户]
        """
        friend_list = self._redis.friend_all(req_user)
        # 直接返回好友列表给请求者
        self._response_to_client("friend_list_resp", {
            "friend_list":friend_list
            }, self._current_socket)
        return SUCCESS

    def _group_create(self, req_user, group_name):
        """
        组创建，请求发起者成为管理员
        Args:
            req_user ([type]): [请求发起用户]
            group_name ([type]): [description]
        """
        if self._redis.group_exists(group_name):
            return FAIL
        self._redis.group_add(group_name)
        self._redis.group_admin_add(group_name, req_user)
        self._redis.group_member_add(group_name, req_user)
        return SUCCESS

    def _group_delete(self, req_user, group_name):
        """
        组删除，该操作只有管理员能执行
        Args:
            req_user ([type]): [请求发起用户]
            group_name ([type]): [description]
        """
        if not self._redis.group_exists(group_name):
            return FAIL
        if req_user not in self._redis.group_admin(group_name):
            return FAIL
        self._redis.group_delete(group_name)
        return SUCCESS

    def _group_add(self, req_user, group_name, username):
        """
        组成员添加，该操作只有管理员能执行
        Args:
            req_user ([type]): [请求发起用户]
            group_name ([type]): [description]
            username ([type]): [description]
        """
        if not self._redis.group_exists(group_name):
            return FAIL
        if username not in self._redis.user_exists(username):
            return FAIL
        if req_user not in self._redis.group_admin(group_name):
            return FAIL
        self._redis.group_member_add(group_name, username)
        return SUCCESS

    def _group_remove(self, req_user, group_name, username):
        """
        组成员移除，该操作只有管理员能执行
        Args:
            req_user ([type]): [请求发起用户]
            group_name ([type]): [description]
            username ([type]): [description]
        """
        if not self._redis.group_exists(group_name):
            return FAIL
        if req_user not in self._redis.group_admin(group_name):
            return FAIL
        self._redis.group_member_remove(group_name, username)
        return SUCCESS

    def _group_info(self, req_user, group_name):
        """
        获取群组信息, 请求者必须属于群组
        Args:
            req_user ([type]): [请求发起用户]
            group_name ([type]): [description]
        """
        if not self._redis.group_exists(group_name):
            return FAIL
        member_set = self._redis.group_members(group_name)
        if req_user not in member_set:
            return FAIL
        admin_set = self._redis.group_admin(group_name)
        self._response_to_client("group_info_resp", {
            "group_name":group_name,
            "group_info":{"admin":admin_set,"members":member_set}
            }, self._current_socket)
        return SUCCESS

    def _admin_add(self, req_user, group_name, username):
        """
        组管理员添加，该操作只有管理员能执行
        Args:
            req_user ([type]): [请求发起用户]
            group_name ([type]): [description]
            username ([type]): [description]
        """
        if not self._redis.group_exists(group_name):
            return FAIL
        if username not in self._redis.group_members(group_name):
            return FAIL
        if req_user not in self._redis.group_admin(group_name):
            return FAIL
        self._redis.group_admin_add(group_name, username)
        return SUCCESS

    def _admin_remove(self, req_user, group_name, username):
        """
        组管理员删除，该操作只有管理员能执行
        Args:
            req_user ([type]): [请求发起用户]
            group_name ([type]): [description]
            username ([type]): [description]
        """
        if not self._redis.group_exists(group_name):
            return FAIL
        admin_set = self._redis.group_admin(group_name)
        if username not in admin_set:
            return FAIL
        if req_user not in admin_set:
            return FAIL
        self._redis.group_admin_remove(group_name, username)
        return SUCCESS

    @staticmethod
    def _recive_message_trans(message):
        """
        发送消息转换成待接收消息

        Args:
            message ([type]): [description]

        Returns:
            [type]: [description]
        """
        recive_message = {
            "session_type": message["session_type"],
            "sender_name": message["sender_name"],
            "content" : message["content"]
        }
        return recive_message

    def _offline_message_add(self, message):
        """
        将消息写入同步库，如果是群组消息采用扩散写的方式
        Args:
            message ([type]): [description]

        Returns:
            [type]: [description]
        """
        target_name = message["target_name"]
        recive_message = self._recive_message_trans(message)

        # 发送给用户
        if recive_message["session_type"] == "user":
            next_recive_id = self._redis.user_info_get(target_name, "next_recive_id")
            print(target_name, next_recive_id, message)
            self._redis.message_add(target_name, next_recive_id, message)
            self._redis.user_info_set(target_name, "next_recive_id", int(next_recive_id) + 1)
            return [target_name]

        #发送给群组，扩散写
        reciver_list =  self._redis.group_members(target_name)
        for user in reciver_list:
            next_recive_id = self._redis.user_info_get(user, "next_recive_id")
            self._redis.message_add(user, next_recive_id, message)
            self._redis.user_info_set(user, "next_recive_id", int(next_recive_id) + 1)
        return reciver_list

    def _offline_message_remove(self, req_user, recive_message_id):
        """
        移除指定的消息
        当成功同步时触发
        Args:
            req_user ([type]): [description]
            recive_message_id ([type]): [description]
        """
        if recive_message_id not in self._redis.message_all(req_user):
            return FAIL
        self._redis.message_delete(req_user, recive_message_id)
        return SUCCESS

    def _offline_message_sync(self, reciver_list):
        """
        向列表里的在线用户同步离线消息
        注意：只处理本服务端上登录的用户的同步任务，登录在其他服务端上的由其他服务端自己处理，避免创建大量tcp连接
        Args:
            reciver_list ([type]): [description]
        """
        reciver_set = set(reciver_list)
        # 获取本服务端上已登录的用户
        login_list = list(self._user_to_socket.keys())
        for user in reciver_set:
            # 不在线
            if self._redis.user_info_get(user, "state") != ONLINE:
                continue
            # 在线，但用户已经不在本服务端上登录，这是集群模式考虑的场景
            if user not in login_list:
                #TODO：通知对应worker去处理
                continue
            # 用户在本机登录，且处于在线状态，本机尝试将离线信息信息同步给用户
            recive_id_list = self._redis.message_all(user)
            user_socket = self._socket_get_by_user(user)
            for recive_id in recive_id_list:
                message = self._redis.message_get(user, recive_id)
                message["recive_message_id"] = recive_id
                self._response_to_client("message_recive", message, user_socket)

    def _offline_message_check(self):
        """
        检查消息同步库是否达到上限，丢弃旧的消息
        这个任务交给一个服务器做就够了
        """
        max_offline_message  = int(self._redis.max_offline_message_get())
        for user in self._redis.user_all():
            message_id_list = list(self._redis.message_all(user))
            message_id_list.sort()
            remove_num = len(message_id_list) - max_offline_message 
            while remove_num > 0:
                message_id = message_id_list.pop(0)
                self._redis.message_delete(user, message_id)
                remove_num -= 1

    def _first_complete_message(self, message_buffer):
        """
        获取发送消息buffer中第一个完整的消息，返回消息id列表

        Args:
            message_buffer ([type]): [description]

        Returns:
            [type]: [description]
        """
        message_id_list = list(message_buffer.keys())
        message_id_list.remove("expect_id")
        if not message_id_list:
            return []
        pos = min(message_id_list)
        # 第一个消息的事务id
        transaction_id = message_buffer[pos]["transaction_id"]
        # 第一个消息的碎片数目
        frag_num = message_buffer[pos]["frag_num"] 
        # 当前已统计的数据包数目
        packet_num = 0
        compelete_flag = False
        id_list = []
        # 截止到期望id
        while True:
            if pos not in message_id_list:                       
                compelete_flag = (packet_num == frag_num)
                break
            if message_buffer[pos]["transaction_id"] != transaction_id:
                compelete_flag = (packet_num == frag_num)
                break
            if message_buffer[pos]["transaction_id"] == transaction_id:
                id_list.append(pos)
                packet_num += 1
                pos += 1
                continue
        # 第一个消息已经完整
        if compelete_flag:
            return id_list
        return []

    def _expect_id_adjust(self, username):
        """
        调整发送消息期望id
        触发条件：
        1.等待消息达到阈值
        2.定时任务发现最小id已经超时

        Args:
            username ([type]): [description]
        """
        expect_id = self._send_message_buffer[username]["expect_id"]
        message_id_list = list(self._send_message_buffer[username].keys())
        message_id_list.remove("expect_id")
        if not message_id_list:
            return
        min_id = min(message_id_list)
        pos = min_id
        # 最小id小于期望id，那么清空期望id前的所有消息
        if min_id < expect_id:
            while pos != expect_id:
                self._send_message_buffer[username].pop(pos)
                pos += 1
            # 期望id至少后移一位
            expect_id += 1
        # 最小id对应长文本，必须检查是否完整，不完整则需要全部丢弃
        elif self._send_message_buffer[username][min_id]["frag_num"] > 1:
            message_buffer = self._send_message_buffer[username]
            message_buffer["expect_id"] = sys.maxint
            # 消息不完整，清空碎片消息
            if not self._first_complete_message(message_buffer):
                transaction_id = message_buffer[pos]["transaction_id"]
                while pos in message_id_list:
                    if message_buffer[pos]["transaction_id"] != transaction_id:
                        break
                    if message_buffer[pos]["transaction_id"] == transaction_id:
                        self._send_message_buffer[username].pop(pos)
                        pos += 1
                        continue
            # 期望id至少在pos后
            expect_id = pos
        # 最小id对应普通长度消息，期望id至少在最小id后
        else:
            expect_id  = min_id
        # 统一更新期望id    
        while expect_id in message_id_list:
            expect_id += 1
        self._send_message_buffer[username]["expect_id"] = expect_id
 
    def _send_message_buffer_process(self, username):
        """
        处理用户的发送消息
        需要说明几点：
        1.buffer中期望id前的消息id必定是连续的
        2.不会存在重复消息id
        3.长文本消息的事务id一致，消息id不同
        Args:
            username ([type]): [description]
        """
        message_id_list = list(self._send_message_buffer[username].keys())
        message_id_list.remove("expect_id")
        # 没有需要处理的消息
        if not message_id_list:
            return
        # 剩余待处理消息数目,按照最大最小id的差值计算，包含缺失信息在内
        message_num = max(message_id_list) - min(min(message_id_list), self._send_message_buffer[username]["expect_id"])
        # 待处理发送消息达到上限
        if message_num >= self._max_send_message:
            # 后移期望id，舍弃第一个未接收完的长文本消息（如果是长文本消息）
            self._expect_id_adjust(username)

        # 取出连续且完整的消息进行处理
        reciver_list = []
        while True:
            id_list = self._first_complete_message(self._send_message_buffer[username])
            if not id_list:
                break
            # 消息发送请求已经完整接收，告知用户成功
            self._response_to_client("simple_result_resp", {
                "transaction_id":self._send_message_buffer[username][id_list[0]]["transaction_id"],
                "result":SUCCESS
                }, self._current_socket)
            for message_id in id_list:
                message = self._send_message_buffer[username][message_id]
                # 放到存储库
                self._mysql.message_save(message)
                # 放到离线消息同步库
                reciver_list.extend(self._offline_message_add(message))
                # 删除已处理的消息
                self._send_message_buffer[username].pop(message_id)
        # 触发同步
        self._offline_message_sync(reciver_list)
   
    def _message_send(self, req_user, target_type, target_name, content, send_message_id, frag_num):
        """
        向好友、群组发送消息
        Args:
            req_user ([type]): [请求发起用户]
            target_type ([type]): [对象类型， 分用户和群组]
            target_name ([type]): [对象名称]
            content ([type]): [实际数据]
            send_message_id ([type]): [发送消息的id]
            frag_num ([type]): [消息碎片数目，大于1代表长文本]
        """
        if target_type == "user":
            # 不是目标的好友，无权发送
            if not self._redis.firend_exists(target_name, req_user):
                self._logger.log_error("authority error:{} is not {} 's friend".format(target_name, req_user))
                return FAIL
        elif target_type == "group":
            if not self._redis.group_exists(target_name):
                self._logger.log_error("group:{} not exist!".format(target_name))
                return FAIL
            if req_user not in self._redis.group_members(target_name):
                self._logger.log_error("authority error:user:{} is not in group:{}".format(req_user, target_name))
                return FAIL

        expect_id = self._send_message_buffer[req_user]["expect_id"]
        # 不是期望的id，要求客户端重发期望消息
        if send_message_id != expect_id:
            self._response_to_client("message_resend_req", {
                "send_message_id":expect_id
                }, self._current_socket)
        # 小于期望id, 无效重复消息, 返回一个无关紧要的状态       
        if send_message_id < expect_id:
            return PROCESSING
        # 其余情况都存入消息发送列表
        message = {
            "transaction_id": self._current_transaction_id,
            "session_type": target_type,
            "sender_name": req_user,
            "target_name": target_name,
            "frag_num": frag_num,
            "content": content,
            "time": current_time_ms()
            }
        self._send_message_buffer[req_user][send_message_id] = message
        # 如果恰好是期望的消息id，需要将期望id后移到下一个孔洞
        if send_message_id == expect_id:
            message_id_list = list(self._send_message_buffer[req_user].keys())
            message_id_list.remove("expect_id")
            while (expect_id in message_id_list):
                expect_id += 1
            self._send_message_buffer[req_user]["expect_id"] = expect_id
        # 处理消息发送列表
        self._send_message_buffer_process(req_user)
        # 返回一个无关紧要的状态，退出即可，由上面处理函数告知客户是否成功
        return PROCESSING
 
    def _message_offline_pull(self, req_user):
        """
        拉取最新几条消息
        Args:
            req_user ([type]): [请求发起用户]
        """
        self._offline_message_sync([req_user])
        return SUCCESS

    def _message_withdraw(self, req_user, type, target_name,  transaction_id):
        """
        消息撤回，需要传入消息id，该id在发送时自动生成
        Args:
            req_user ([type]): [请求发起用户]
            type ([type]): [对象类型]
            target_name ([type]): [对象名称]
            transaction_id ([type]): [事务的id，发送请求时自动生成并携带，用来区分]
        """
        #TODO

    def _packet_dispatch(self, data_dict, packet_type_enum_class):
        """
        分发到具体函数处理数据包

        Args:
            data_dict ([type]): [description]
            packet_type_enum_class ([type]): [description]

        Returns:
            [type]: [description]
        """
        # 获取事务类型
        packet_type = data_dict['packet_type']
        if packet_type not in [e.name for e in packet_type_enum_class]:
            self._logger.log_error('packet type error:{}'.format(packet_type))
            return FAIL
        # 获取对应处理函数
        method = getattr(self, packet_type_enum_class[packet_type].value)
        args = inspect.getargspec(method).args
        # 构造参数列表
        args_list = []
        # 处理函数如果需要请求者，那么数据中必须包含合法的token
        if "req_user" in args: 
            if "token" not in data_dict.keys():
                self._logger.log_error("token missed in packet type:{}".format(packet_type))
                return FAIL
            req_user = self._redis.user_get_by_token(data_dict["token"])
            if not req_user:
                self._logger.log_error('token error:please login')
                return FAIL
            args_list.append(req_user)
        # 参数完整性检查
        for arg in args[1:]:
            if arg == "req_user":
                continue
            if arg not in data_dict.keys():
                #  参数缺失，退出
                self._logger.log_error("arg:{} missed in packet type:{}".format(packet_type, arg))
                return FAIL
            # 参数用字节类型
            args_list.append(data_dict[arg])
        # 调用对应函数处理请求
        return method(*args_list)

    def _packet_process_flow(self, data):
        """
        收包解析函数，将来自服务端的数据包按类型进行解析处理
        """
        # 先解析数据包，获得字典形式的返回
        data_dict = json.loads(base64.b64decode(data).decode("utf-8"))
        self._logger.log_debug(data_dict)
        # 获取事务id
        self._current_transaction_id = data_dict['transaction_id']
        # 分发给具体函数处理，对应方法在枚举类中
        ret = self._packet_dispatch(data_dict, ServerRecivePacketType)
        if ret == FAIL:
            self._logger.log_error("RequestHandlerError:packet type:{}".format(data_dict['packet_type']))
        # 没必要通知请求者，直接退出
        if ret == PROCESSING:
            return
        # 发送回应给请求者
        self._response_to_client("simple_result_resp", {
            "transaction_id":self._current_transaction_id,
            "result":ret
            }, self._current_socket)

    def _server_cron_task(self):
        """
        定时任务
        """
        # 检查发送消息列表是否有消息快超时
        current_time = current_time_ms()
        for key in self._send_message_buffer.keys():
            message_id_list = list(self._send_message_buffer[key].keys())
            message_id_list.remove("expect_id")
            if not message_id_list:
                return
            min_id = min(message_id_list)
            pass_time_ms = current_time - self._send_message_buffer[key][min_id]["time"]
            # 致命时钟错误
            if pass_time_ms < 0:
                self._logger.log_critical("the clock is wrong and the program cannot run correctly")
                exit()
            #  如果最小消息id对应的消息超时了，立刻后移期望id并开始处理后续消息
            if pass_time_ms > self._send_message_timeout_ms:
                self._expect_id_adjust(key)
                self._send_message_buffer_process(key)
        # 检查消息同步库是否达到上限，丢弃旧的消息
        # TODO:集群模式该任务交给一个worker即可
        self._offline_message_check()

    @staticmethod
    def _signal_handler(signal_num, frame):
        """
        中断信号处理函数
        """
        exit()

    def _signal(self):
        """
        绑定信号处理函数
        """
        for sig in [signal.SIGINT, signal.SIGHUP, signal.SIGTERM]:
            signal.signal(sig, self._signal_handler)

    def run(self):
        """
        主体函数，使用io复用模型（select），收取数据包、执行定时任务
        """
        self._server_init()
        self._signal()

        inputs = self.fd_set
        data_dict = {}
        while True:
            try:
                readable, writable, expectional = select.select(inputs, [], inputs, self._checktime_s)
                # 触发定时任务
                if not readable and not expectional:
                    self._server_cron_task()
                    continue
                # 存在可读事件
                for each in readable:
                    # 监听端口，收到建立连接请求
                    if each is self._server_socket:
                        connect_fd, request_addr = each.accept()
                        self._logger.log_info("connection from {}".format(request_addr))
                        inputs.append(connect_fd)
                        data_dict[connect_fd] = {"data_len": 0, "data_bytes": b''}
                    # 客户端发送过来的其他数据
                    else:
                        read_flag = False
                        # data_len为0, 先获得待读取数据包总长度
                        if not data_dict[each]["data_len"]:
                            header = each.recv(HEADER_LEN)
                            header_len = len(header)
                            # 不等于头部长度属于致命错误，无法判断后续内容是什么
                            if header_len == HEADER_LEN:
                                data_dict[each]["data_len"] = struct.unpack('i', header)[0]
                                read_flag = True
                            elif header_len > 0:
                                each.close()
                        # 按数据包长度尽量读取数据
                        while data_dict[each]["data_len"]:
                            recv_len = min(data_dict[each]["data_len"], BUFFER_SIZE)
                            data = each.recv(recv_len)
                            # 先退出，可能还没有到达，等待下一轮继续读取
                            if not data:
                                break
                            data_dict[each]["data_bytes"] += data
                            data_dict[each]["data_len"] = data_dict[each]["data_len"] - recv_len
                            read_flag = True
                        # 没有读取到任何实际数据，说明客户端关闭连接
                        if not read_flag:
                            self._logger.log_error("disconnect from {}".format(each.getpeername()))
                            # 强制使用该socket的用户下线
                            self._user_logout_by_socket(each)
                            inputs.remove(each)
                            data_dict.pop(each)
                            continue
                        # 读取完毕，交给对应函数处理
                        if not data_dict[each]["data_len"]:
                            self._current_socket = each
                            self._packet_process_flow(data_dict[each]["data_bytes"])
                            data_dict[each]["data_bytes"] = b''
                for each in expectional:
                    self._logger.log_error("expection on client:{}".format(each.getpeername()))
                    inputs.remove(each)
                    each.close()                   
            except Exception as e:
                self._logger.log_error("ERROR:{}".format(repr(e)))
                
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default="./config.json")
    config = parser.parse_args().config
    ser = Server(config)
    ser.run()