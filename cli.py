#!/usr/bin/env python3
# -*- coding:UTF-8 -*-
import time
import json
import sys
import signal
import struct
import select
import shlex
import inspect
import argparse
import base64
from socket import *

#导入其他模块
from log import *
from common import *

def current_time_ms():
    return int(round(time.time() * 1000))

class Client(object):
    _cmd = {
        'help': {'func': '_help',                   'info': 'help                                           help information'},
        'register': {'func': '_user_register',      'info': 'register username password                      user login'},
        'login': {'func': '_user_login',            'info': 'login  username password                        user register'},
        'logout': {'func': '_user_logout',          'info': 'logout  username                                user logout'},
        'user_delete': {'func': '_user_delete',     'info': 'user_delete  username                           user delete'},
        'friend_add': {'func': '_friend_add',       'info': 'friend_add friend_name                          add friend'},
        'friend_delete': {'func': '_friend_delete', 'info': 'friend_delete friend_name                       add friend'},
        'friend_list': {'func': '_friend_list',    'info': 'friend_list                                     get friend list'},
        'group_create': {'func': '_group_create',   'info': 'group_create group_name                         create group'},
        'group_delete': {'func': '_group_delete',   'info': 'group_delete group_name                         delete group'},
        'group_info': {'func': '_group_info',       'info': 'group_info group_name                           get group information'},
        'admin_add': {'func': '_admin_add',         'info': 'admin_add group_name username                   add admin'},
        'admin_remove': {'func': '_admin_remove',   'info': 'admin_remove group_name username                remove admin'},
        'message_send': {'func': '_message_send',   'info': 'message_send target_type target_name content    send message'}
        # 'history': {'func': '_history',             'info': 'history target_type target_name                 pull history message'}
    }

    def __init__(self, config_file="./config.json"):
        with open(config_file) as fp:
            cfg = json.load(fp)
            self._checktime_s = cfg["client"]["checktime_s"]
            self._timeout_ms = cfg["client"]["timeout_ms"]
            self._max_message_len = cfg["client"]["max_message_len"]
            self._message_len_divided = cfg["client"]["message_len_divided"]
            self._log_path = cfg["client"]["log_path"]
            self._log_level = cfg["client"]["log_level"]
            self._server_port = cfg["server"]["server_port"]
            self._server_ip= cfg["server"]["server_ip"]

        self._current_user = None
        self._password = None
        self._user_state = OFFLINE
        # 事务id格式：user_clientip_type_tsc
        self._current_transaction_id = None
        # 消息id使用单调递增整数, 为避免用户在其他主机重新登陆时引发乱序，登录时服务端会处理完先前的发送消息列表
        self._current_send_message_id = 0
        self._token = None
        # 存放事务id,请求发起的时间和全部参数，完成请求时从中删除，定时检查请求是否超时失败
        self._transaction_table = {}
        # 存放接收消息列表
        self._message_recive_list = {}
        # 存放会话名和对应的消息列表
        self._session_table = {}

    def _log_init(self):
        """
        日志初始化

        Returns:
            [type]: [description]
        """
        self._logger = Logger(self._log_path, self._log_level)
        return self._logger

    def _client_socket_init(self):
        """
        客户端socket初始化

        Returns:
            [type]: [description]
        """
        try:
            self._client_socket = socket(AF_INET, SOCK_STREAM)
            # 禁用nagle，降低延迟
            # self._client_socket.setsockopt(IPPROTO_TCP, TCP_NODELAY, True)
            self._client_socket.connect((self._server_ip, self._server_port))
            self._inputs.append(self._client_socket)
        except error as e:
            self._logger.log_error("Failed to establish connection with server:{}, error:{}".format(
                (self._server_ip, self._server_port), repr(e)))
            return False
        return True

    def _client_init(self):
        self._inputs = [sys.stdin]
        if not self._log_init():
            self._logger.log_error("log init failed")
            exit()
        if not self._client_socket_init():
            self._logger.log_error("socket init failed")
            exit()
        self._logger.log_info("client initialized successfully")

    def _request_send(self, event_type, args_dict):
        """
        为各种指令生成发往服务器的数据
        格式为json字符串+utf8编码

        Args:
            event_type ([type]): [description]
            args_dict ([type]): [description]

        Returns:
            [type]: [description]
        """
        args_dict['packet_type'] = event_type
        args_dict['transaction_id'] = self._current_transaction_id
        # 补充token
        if event_type != "user_register" and event_type != "user_login":
            args_dict['token'] = self._token
        data = base64.b64encode(json.dumps(args_dict).encode("utf-8"))
        # 额外发送4个字节，代表下一个数据包长度，用来处理粘包、分包
        self._client_socket.sendall(struct.pack('i', len(data)))
        self._client_socket.sendall(data)
        return True

    def _user_register(self, username, password):
        """
        用户注册

        Args:
            username ([type]): [description]
            password ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("user_register", args)

    def _user_login(self, username, password):
        """
        用户登录

        Args:
            username ([type]): [description]
            password ([type]): [description]
        """
        args = locals()
        args.pop('self')
        self._current_send_message_id = 0
        self._current_user = username
        self._password = password
        return self._request_send("user_login", args)

    def _user_logout(self, username):
        """
        用户登出

        Args:
            username ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("user_logout", args)

    def _user_delete(self, username):
        """
        用户删除

        Args:
            username ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("user_delete", args)

    def _friend_add(self, friend_name):
        """
        好友添加

        Args:
            friend_name ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("friend_add", args)

    def _friend_delete(self, friend_name):
        """
        好友删除

        Args:
            friend_name ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("friend_delete", args)

    def _friend_list(self):
        """
        好友列表获取
        """
        args = locals()
        args.pop('self')
        return self._request_send("friend_list", args)

    def _group_create(self, group_name):
        """
        组创建，创建者成为管理员

        Args:
            group_name ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("group_create", args)

    def _group_delete(self, group_name):
        """
        组删除，该操作只有管理员能执行

        Args:
            group_name ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("group_delete", args)

    def _group_add(self, group_name, username):
        """
        组成员添加，该操作只有管理员能执行

        Args:
            group_name ([type]): [description]
            username ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("group_add", args)

    def _group_remove(self, group_name, username):
        """
        组成员移除，该操作只有管理员能执行

        Args:
            group_name ([type]): [description]
            username ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("group_remove", args)

    def _group_info(self, group_name):
        """获取群组信息

        Args:
            group_name ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("group_info", args)

    def _admin_add(self, group_name, username):
        """
        组管理员添加，该操作只有管理员能执行

        Args:
            group_name ([type]): [description]
            username ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("admin_add", args)

    def _admin_remove(self, group_name, username):
        """
        组管理员移除，该操作只有管理员能执行

        Args:
            group_name ([type]): [description]
            username ([type]): [description]
        """
        args = locals()
        args.pop('self')
        return self._request_send("admin_remove", args)

    def _message_send(self, target_type, target_name, content):
        """
        向好友、群组发送消息
        最大支持消息长度，受到服务器维护buffer数目及客户端每个数据包实际消息长度的限制
        例如客户端配置中每个消息数据包中最多发送800字节消息，服务器配置成每个用户维护100条消息
        那么最大消息长度为800*100，超过这个长度必定失败
        Args:
            target_type ([type]): [对象类型， 分用户和群组]
            target_name ([type]): [对象名称]
            content ([type]): [实际数据]
        """
        args = locals()
        args.pop('self')
        message_len = len(content)
        if message_len > self._max_message_len:
            self._logger.log_error("length exceeds the upper limit")
            return False
        # 分片数目
        frag_num = int(message_len / self._message_len_divided) + 1
        args["frag_num"] = frag_num
        while message_len:
            # 分片发送
            if message_len > self._message_len_divided:
                args["content"] = content[:self._message_len_divided]
                content = content[self._message_len_divided:]
                message_len -= self._message_len_divided
            else:
                args["content"] = content
                message_len = 0
            args['send_message_id'] = self._current_send_message_id
            self._current_send_message_id += 1
            if not self._request_send("message_send", args):
                return False
        return True

    def _message_offline_pull(self):
        """
        请求拉取用户的离线待接收消息
        """
        args = locals()
        args.pop('self')
        return self._request_send("message_offline_pull", args)

    def _message_withdraw(self, target_type, target_name,  transaction_id):
        """
        消息撤回，需要传入事务id

        Args:
            target_type ([type]): [对象类型]
            target_name ([type]): [对象名称]
            transaction_id ([type]): [事务的id，发送请求时自动生成并携带，用来区分]
        """
        args = locals()
        args.pop('self')
        return self._request_send("message_withdraw", args)

    def _help(self):
        """
        打印帮助信息到终端
        """
        print ('############################# help  information ####################################')
        for each in self._cmd.keys():
            print (self._cmd[each]['info'])
        return True

    def session_view(self, target_type, target_name):
        """
        获取会话内容，首先会用message_offline_pull拉取一下离线消息，然后读取客户端的session_table

        Args:
            target_type ([type]): [description]
            target_name ([type]): [description]
        """

    def _transaction_id_update(self, event_type):
        """
        更新当前事务id
        格式：user_clientip_type_tsc

        Args:
            event_type ([type]): [事件类型]
        """
        if self._current_user:
            user = self._current_user
        else:
            user = "null"
        time_ms = current_time_ms()
        tsc = str(time_ms)
        #更新当前事务id
        self._current_transaction_id = '_'.join([user, self._client_socket.getsockname()[0], event_type, tsc])
        #添加进待执行事务表
        self._transaction_table[self._current_transaction_id] = {}
        self._transaction_table[self._current_transaction_id]['type'] = event_type
        self._transaction_table[self._current_transaction_id]['time_ms'] = time_ms

    def _client_packet_dispatch(self, data):
        """
        收包解析函数，将来自服务端的数据包按类型进行解析处理

        Args:
            data ([type]): [description]
        """
        # 先解析数据包，获得字典形式的返回
        data_dict = json.loads(base64.b64decode(data).decode("utf-8"))
        self._logger.log_debug(data_dict)
        event_type = data_dict['packet_type']
        if event_type not in [e.name for e in ClientRecivePacketType]:
            self._logger.log_error('packet type error:{}'.format(data_dict['packet_type']))
            return
        # 简单的结果响应
        if event_type == "simple_result_resp":
            if data_dict['transaction_id'] not in self._transaction_table.keys():
                return
            if data_dict["result"] == FAIL:
                self._logger.log_error("{} failed".format(
                    self._transaction_table[data_dict['transaction_id']]['type']))
            self._transaction_table.pop(data_dict['transaction_id'])
            return
        # 更新token
        if event_type == "token_update_req":
            self._token = data_dict["token"]
            return
        # 更新服务端ip和port，重新建立tcp连接
        if event_type == "server_update_req":
            if self._server_ip == data_dict["server_ip"] and self._server_port == data_dict["server_port"]:
                return
            self._server_ip = data_dict["server_ip"]
            self._server_port = data_dict["server_port"]
            self._client_socket.close()
            self._inputs.remove(self._client_socket)
            if not self._client_socket_init():
                self._logger.log_error("failed to connect new worker")
            # 尝试重新登录用户
            self._user_login(self._current_user, self._password)
            return
        # 收到消息
        if event_type == "message_recive":
            # 存放到接收列表中
            self._message_recive_list[data_dict["recive_message_id"]] = {
                "session_type":data_dict["session_type"],
                "sender_name":data_dict["sender_name"],
                "content":data_dict["content"]
            }
            self._transaction_id_update("message_recive_ack")
            self._request_send("message_recive_ack", {
                "recive_message_id":data_dict["recive_message_id"]
            })
            self._logger.log_info("\n {}:{} message:{}".format(
                data_dict["session_type"], data_dict["sender_name"], data_dict["content"]))
            return
        # 收到好友列表
        if event_type == "friend_list_resp":
            self._logger.log_info("friend list:{}".format(data_dict["friend_list"]))
            return
        # 收到群组信息
        if event_type == "group_info_resp":
            self._logger.log_info("group_info:{}".format(data_dict["group_info"]))
            return
        self._logger.log_error("recive packet type:{}".format(event_type))

    def _cmd_parse(self, cmd_str):
        """
        指令解析
        指令格式为：type -arg_name arg_content...

        Args:
            cmd_str ([type]): [description]
        """
        cmd_list = shlex.split(cmd_str.rstrip('\n'))
        args_num = len(cmd_list)
        if args_num == 0:
            return None
        cmd_dict = {}
        # 先获取指令的类型
        cmd_dict['packet_type'] = cmd_list[0]
        index = 1
        # 解析出其他参数
        while index < args_num:
            key = cmd_list[index].lstrip('-')
            index = index + 1
            if index == args_num:
                return cmd_dict
            # message后面的全部视为待发送信息
            if key == "message":
                cmd_dict[key] = ''.join(cmd_list[index:])
                return cmd_dict
            cmd_dict[key] = cmd_list[index]
            index = index + 1
        return cmd_dict

    def _client_cmd_dispatch(self, cmd_str):
        """
        处理直接来自用户的指令

        Args:
            cmd_str ([type]): [description]
        """
        # 将cmd的参数解析成字典
        cmd_dict = self._cmd_parse(cmd_str)
        if not cmd_dict:
            return
        packet_type = cmd_dict['packet_type'] 
        if packet_type not in self._cmd.keys():
            self._logger.log_error('cmd type error:{}'.format(packet_type))
            return
        # 获取对应的处理函数
        method = getattr(self, self._cmd[packet_type]['func'])
        args = inspect.getargspec(method).args
        args_list = []
        # 参数完整性检查
        for arg in args[1:]:
            if arg == 'self':
                continue
            if arg not in cmd_dict.keys():
                self._logger.log_error('arg:{} missed in packet type:{}'.format(
                    arg, cmd_dict['packet_type']))
                return 
            args_list.append(cmd_dict[arg])
        # 更新当前事务id 
        if packet_type != "help":
            self._transaction_id_update(cmd_dict['packet_type'])
        if not method(*args_list):
            self._logger.log_error('CmdHandlerError:packet type:{}'.format(cmd_dict['packet_type']))

    def _client_cron_task(self):
        """
        定时任务：检查事务表，判断是否有请求超时
        """
        current_time = current_time_ms()
        id_list = list(self._transaction_table.keys())
        for key in id_list:
            pass_time_ms = current_time - self._transaction_table[key]['time_ms']
            # 可能要考虑时钟错误，一直为负数
            if pass_time_ms < 0:
                self._logger.log_critical("the clock is wrong and the program cannot run correctly")
                exit()
            if pass_time_ms > self._timeout_ms:
                self._logger.log_error("{} timeout".format(
                    self._transaction_table[key]['type']))
                self._transaction_table.pop(key)

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
        主体函数，使用io复用模型（select），收取数据包、获取用户终端输入、执行定时任务
        """
        self._client_init()
        self._signal()

        data_bytes = b''
        data_len = 0
        while True:
            try:
                readable, writable, exceptional = select.select(self._inputs, [], self._inputs, self._checktime_s)
                # 触发定时任务, 检查之前的操作是否已经超时
                if not readable and not exceptional:
                    self._client_cron_task()
                    continue
                # 存在可读事件
                for each in readable:
                    # 来自socket
                    if each is self._client_socket:
                        read_flag = False
                        # data_len为0, 先获得待读取数据包总长度
                        if not data_len:
                            header = each.recv(HEADER_LEN)
                            header_len = len(header)
                            # 只要不等于头部长度直接关闭与服务端的连接，属于致命错误，无法判断后续内容是什么
                            if header_len == HEADER_LEN:
                                data_len = struct.unpack('i', header)[0]
                                read_flag = True
                            elif header_len > 0:
                                each.close()
                                self._inputs.remove(each)
                        
                        # 按数据包长度尽量读取数据
                        while data_len:
                            recv_len = min(data_len, BUFFER_SIZE)
                            data = each.recv(recv_len)
                            # 先退出，可能还没有到达，等待下一轮继续读取
                            if not data:
                                break
                            data_bytes += data
                            data_len = data_len - recv_len
                            read_flag = True
                        # 没有读取到任何实际数据，说明服务端已关闭连接
                        if not read_flag:
                            self._inputs.remove(each)
                            data_bytes = b''
                            data_len = 0
                            self._logger.log_error("the connection to server is disconnected")
                            continue
                        # 读取完毕，交给对应函数处理
                        if not data_len:
                            self._client_packet_dispatch(data_bytes)
                            data_bytes = b''
                    # 来自终端的输入
                    if each is sys.stdin:
                        cmd = each.readline()
                        self._client_cmd_dispatch(cmd)
                for each in exceptional:
                    each.close()
                    self._inputs.remove(each)
                    self._logger.log_error("exception on server:{}".format(each.getpeername()))
            except (ImportError, IOError, KeyError, error)  as e:
                self._logger.log_error("ERROR:{}".format(repr(e)))

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', default="./config.json")
    config_file = parser.parse_args().config
    cli = Client(config_file)
    cli.run()