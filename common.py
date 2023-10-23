#!/usr/bin/env python3
# -*- coding:UTF-8 -*-
from enum import *
HEADER_LEN = 4
BUFFER_SIZE = 1024
OFFLINE    = "offline"
ONLINE     = "online"
FAIL       = -1
PROCESSING = 0
SUCCESS    = 1

@unique
class ClientRecivePacketType(Enum):
    #大部分服务端发送的响应数据包只需要带事务id和状态码即可，状态码后续补充
    #整体数据包结构: packet_type + transaction_id + result(success:1, fail:-1) 
    simple_result_resp  = 1

    #好友列表数据
    #整体数据包结构:packet_type + friend_list
    friend_list_resp   = 2

    #服务端转发给客户端的消息
    #后续考虑改进或者拆分
    #整体数据包结构: packet_type + recive_message_id + session_type + sender_name + （group_name） + content
    message_recive  = 3

    #更新客户端的token
    #整体数据包结构:packet_type + token
    token_update_req  = 4

    #更新客户端使用的服务器ip和port
    #整体数据包结构:packet_type + token + server_ip + server_port
    server_update_req  = 5

    #整体数据包结构:packet_type + group_name + group_info
    group_info_resp  = 6

    #重发消息请求
    #整体数据包结构:packet_type + send_message_id
    message_resend_req = 7


@unique
class ServerRecivePacketType(Enum):
    #整体数据包结构: version + packet_type + transaction_id + username + password
    user_register       = '_user_register' 
    #整体数据包结构: version + packet_type + transaction_id + token + username(to_delete)
    user_delete         = '_user_delete'
    #整体数据包结构: version + packet_type + transaction_id + username + password
    user_login          = '_user_login'
    #整体数据包结构: version + packet_type + transaction_id + token + username(to_logout)
    user_logout         = '_user_logout' 

    #整体数据包结构: version + packet_type + transaction_id + token + username_to_add
    friend_add          = '_friend_add'
    #整体数据包结构: version + packet_type + transaction_id + token + username_to_delete
    friend_delete       = '_friend_delete'
    #整体数据包结构: version + packet_type + transaction_id + token
    friend_list         = '_friend_list'

    #整体数据包结构: version + packet_type + transaction_id + token + group_name
    group_create        = '_group_create'
    #整体数据包结构: version + packet_type + transaction_id + token + group_name
    group_delete        = '_group_delete'
    #整体数据包结构: version + packet_type + transaction_id + token + group_name + username
    group_add           = '_group_add'
    #整体数据包结构: version + packet_type + transaction_id + token + group_name + username
    group_remove        = '_group_remove'
    #整体数据包结构: version + packet_type + transaction_id + token + group_name
    group_info          = '_group_info'
    #整体数据包结构: version + packet_type + transaction_id + token + group_name + username
    admin_add        = '_admin_add'
    #整体数据包结构: version + packet_type + transaction_id + token + group_name + username
    admin_remove       = '_admin_remove'

    #整体数据包结构: version + packet_type + transaction_id + token + send_message_id + target_type +
    #               target_name(username/group_name) + frag_num(消息碎片数目，大于1代表长文本) + content
    message_send         = '_message_send'
    #整体数据包结构: version + packet_type + transaction_id + token
    message_offline_pull = '_message_offline_pull'
    #整体数据包结构: version + packet_type + transaction_id + token
    message_withdraw     = '_message_withdraw '

    #整体数据包结构: version + packet_type + transaction_id + token + recive_message_id
    message_recive_ack   = "_offline_message_remove"
    # #整体数据包结构: version + packet_type + transaction_id + token + recive_message_id
    # message_recive_sack      = 
    # #整体数据包结构: version + packet_type + transaction_id + token + recive_message_id
    # message_recive_nack      = 

    #TODO：
    # 如果要支持图片或大文件发送再补充其他的数据包类型

@unique
class MasterRecivePacketType(Enum):
    #整体数据包结构: version + packet_type + transaction_id + username + password
    user_register         = '_user_register' 
    #整体数据包结构: version + packet_type + transaction_id + username + password
    user_login             = '_user_login'
    #整体数据包结构: version + packet_type + username
    login_success         = '_login_success'
    #整体数据包结构: version + packet_type
    worker_register       = '_worker_register'  
    #整体数据包结构: version + packet_type + reciver_list
    message_sync_forward  = '_message_sync_forward'
    #整体数据包结构: version
    heartbeat             = '_heartbeat'

@unique
class WorkerRecivePacketType(Enum):
    #整体数据包结构: version + packet_type + reciver_list
    message_sync_forward   = '_message_sync_forward'

    user_register          = '_user_register' 
    user_delete            = '_user_delete'
    user_login             = '_user_login'
    user_logout            = '_user_logout' 
    friend_add             = '_friend_add'
    friend_delete          = '_friend_delete'
    friend_list            = '_friend_list'
    group_create           = '_group_create'
    group_delete           = '_group_delete'
    group_add              = '_group_add'
    group_remove           = '_group_remove'
    group_info             = '_group_info'
    admin_add              = '_admin_add'
    admin_remove           = '_admin_remove'
    message_send           = '_message_send'
    message_offline_pull   = '_message_offline_pull'
    message_withdraw       = '_message_withdraw '
    message_recive_ack     = "_offline_message_remove"