#!/usr/bin/env python3
# -*- coding:UTF-8 -*-
from base64 import decode
import redis
'''
redis数据结构如下

用户名集合（set）：
achat:user
(username,...)
用户信息（hash）：
achat:{username}:user_info
{"password":, "state":, "next_recive_id":,"token":，"worker":}
用户好友（set)：
achat:{username}:friends
(username,...)

离线消息上限（string):
achat:max_offline_message
"num"
用户待接收离线消息（set）：
achat:{username}:message_set
(message_id,...)
消息：
achat:{username}:{message_id}
{"session_type":, "sender_name":, "content":}

群组集合（set）：
achat:group 
(group_name,...)
群组管理员集合（set）：
achat:{group_name}:admin 
(username,...)
群组成员集合（set）：
achat:{group_name}:members
(username,...)

token=>user映射（string）：
achat:{token} 
user=>token映射（string）：
achat:{username} 
'''
class RedisDb(object):
    def __init__(self, host, port):
        self._redis = redis.StrictRedis(host, port)

    def max_offline_message_set(self, num):
        """
        设置每个用户的离线消息上限

        Args:
            num ([type]): [description]
        """
        self._redis.set("achat:max_offline_message", num)

    def max_offline_message_get(self):
        """
        获取每个用户的离线消息上限

        """
        ret = self._redis.get("achat:max_offline_message")
        if not ret:
            return None
        return ret.decode('utf-8')

    def user_all(self):
        """
        获取所有用户

        Returns:
            [type]: [description]
        """
        return list(user.decode('utf-8') for user in self._redis.smembers("achat:user"))

    def user_exists(self, username):
        """
        检查用户是否存在

        Args:
            username ([type]): [description]
        """
        return (username in self.user_all())

    def user_add(self, username):
        """
        往用户集合添加用户

        Args:
            username ([type]): [description]
        """
        self._redis.sadd("achat:user", username)

    def user_info_set(self, username, key, value):
        """
        修改用户信息

        Args:
            username ([type]): [description]
            key ([type]): [description]
            value ([type]): [description]
        """
        redis_key = "achat:{}:user_info".format(username)
        self._redis.hset(redis_key, key, value)

    def user_info_get(self, username, key):
        """
        获取用户信息

        Args:
            username ([type]): [description]
            key ([type]): [description]
        """
        redis_key = "achat:{}:user_info".format(username)
        ret = self._redis.hget(redis_key, key)
        if not ret:
            return None
        return ret.decode('utf-8')

    def token_set(self, username, token, expire_s):
        """
        生成并设置token到用户的映射

        Args:
            username ([type]): [description]
            tomeout ([type]): [description]
        """
        self._redis.set("achat:{}".format(token), username, ex=expire_s)
        self._redis.set("achat:{}".format(username), token, ex=expire_s)

    def user_get_by_token(self, token):
        """
        由token获取请求发起者的用户名
        Args:
            token ([type]): [用户身份标志]
        """
        redis_key = "achat:{}".format(token)
        ret = self._redis.get(redis_key)
        if not ret:
            return None
        return ret.decode('utf-8')

    def token_get_by_user(self, username):
        """
        由请求发起者获取token
        Args:
            token ([type]): [用户身份标志]
        """
        redis_key = "achat:{}".format(username)
        ret = self._redis.get(redis_key)
        if not ret:
            return None
        return ret.decode('utf-8')

    def token_delete(self, username):
        """
        删除某个用户的登录token

        Args:
            username ([type]): [description]
        """
        token = self.token_get_by_user(username)
        if not token:
            self._redis.delete("achat:{}".format(username))
            self._redis.delete("achat:{}".format(token))

    def friend_all(self, username):
        """
        获取好友列表

        Args:
            username ([type]): [description]
        """
        redis_key = "achat:{}:friends".format(username)
        return list(user.decode('utf-8') for user in self._redis.smembers(redis_key))

    def firend_exists(self, username, friend_name):
        """
        判断是否是用户的好友

        Args:
            username ([type]): [description]
            friend_name ([type]): [description]
        """
        return (friend_name in self.friend_all(username))

    def friend_add(self, req_user, friend_name):
        """
        往请求者的好友列表添加好友

        Args:
            req_user ([type]): [description]
            friend_name ([type]): [description]
        """
        redis_key = "achat:{}:friends".format(req_user)
        self._redis.sadd(redis_key, friend_name)

    def friend_delete(self, req_user, friend_name):
        """
        往请求者的好友列表删除好友

        Args:
            req_user ([type]): [description]
            friend_name ([type]): [description]
        """
        redis_key = "achat:{}:friends".format(req_user)
        self._redis.srem(redis_key, friend_name)

    def group_all(self):
        """
        获取所有群组

        Returns:
            [type]: [description]
        """
        return list(group.decode('utf-8') for group in self._redis.smembers("achat:group"))

    def group_exists(self, group_name):
        """
        检查群组是否存在

        Args:
            group_name ([type]): [description]
        """
        return (group_name in self.group_all())

    def group_add(self, group_name):
        """
        添加新群组

        Args:
            group_name ([type]): [description]
        """
        self._redis.sadd("achat:group", group_name)

    def group_delete(self, group_name):
        """
        删除群组

        Args:
            group_name ([type]): [description]
        """
        self._redis.delete("achat:{}:admin".format(group_name))
        self._redis.delete("achat:{}:members".format(group_name))
        self._redis.srem("achat:group", group_name)

    def group_admin_add(self, group_name, username):
        """
        为群组添加管理员

        Args:
            group_name ([type]): [description]
            username ([type]): [description]
        """
        redis_key = "achat:{}:admin".format(group_name)
        self._redis.sadd(redis_key, username)

    def group_admin_remove(self, group_name, username):
        """
        往群组删除管理员

        Args:
            group_name ([type]): [description]
            username ([type]): [description]
        """
        redis_key = "achat:{}:admin".format(group_name)
        self._redis.srem(redis_key, username)

    def group_admin(self, group_name):
        """
        群组所有管理员

        Args:
            group_name ([type]): [description]
        """
        redis_key = "achat:{}:admin".format(group_name)
        return list(admin.decode('utf-8') for admin in self._redis.smembers(redis_key))

    def group_member_add(self, group_name, username):
        """
        为群组添加成员

        Args:
            group_name ([type]): [description]
            username ([type]): [description]
        """
        redis_key = "achat:{}:members".format(group_name)
        self._redis.sadd(redis_key, username)

    def group_member_remove(self, group_name, username):
        """
        往群组删除成员

        Args:
            group_name ([type]): [description]
            username ([type]): [description]
        """
        redis_key = "achat:{}:members".format(group_name)
        self._redis.srem(redis_key, username)

    def group_members(self, group_name):
        """
        群组所有成员

        Args:
            group_name ([type]): [description]
        """
        redis_key = "achat:{}:members".format(group_name)
        return list(user.decode('utf-8') for user in self._redis.smembers(redis_key))

    def user_remove(self, username):
        """
        删除某个用户的全部相关信息

        Args:
            username ([type]): [description]
        """
        # 删除token映射
        self.token_delete(username)
        # 删除用户信息
        self._redis.delete("achat:{}:user_info".format(username))
        # 离线消息id集合
        message_set = self._redis.smembers("achat:{}:message_set".format(username))
        # 删除未处理的离线消息
        for id in message_set:
            self._redis.delete("achat:{}:{}".format(username, id))
        # 删除消息id集合
        self._redis.delete("achat:{}:message_set".format(username))
        # TODO:群组相关,暂不处理
        # 从用户集合中去除
        self._redis.srem("achat:user", username)

    def message_add(self, username, message_id, message):
        """
        添加message

        Args:
            username ([type]): [description]
            message_id ([type]): [description]
            message ([type]): [description]
        """
        self._redis.sadd("achat:{}:message_set".format(username), message_id)
        redis_key = "achat:{}:{}".format(username, message_id)
        self._redis.hmset(redis_key, message)

    def message_delete(self, username, message_id):
        """
        删除某一条message

        Args:
            username ([type]): [description]
            message_id ([type]): [description]
        """
        redis_key = "achat:{}:{}".format(username, message_id)
        self._redis.delete(redis_key)
        self._redis.srem("achat:{}:message_set".format(username), message_id)

    def message_all(self, username):
        """
        获取所有离线消息

        Args:
            username ([type]): [description]
        """
        redis_key = "achat:{}:message_set".format(username)
        return list(message.decode('utf-8') for message in self._redis.smembers(redis_key))

    def message_get(self, username, message_id):
        """
        添加message

        Args:
            username ([type]): [description]
            message_id ([type]): [description]
        """
        redis_key = "achat:{}:{}".format(username, message_id)
        message = {}
        message["session_type"] = self._redis.hget(redis_key, "session_type").decode('utf-8')
        message["sender_name"] = self._redis.hget(redis_key, "sender_name").decode('utf-8')
        message["content"] = self._redis.hget(redis_key, "content").decode('utf-8')
        return message