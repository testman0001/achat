#!/usr/bin/env python3
# -*- coding:UTF-8 -*-
import pymysql

class Mysqldb(object):
    def __init__(self, host, user, password):
        db_name = "achat"
        self._conn = pymysql.connect(host=host, user=user, password=password)
        self._cursor = self._conn.cursor()
        sql_list = []
        # 数据库不存在则创建
        sql_list.append("CREATE DATABASE IF NOT EXISTS `%s`;" % db_name)
        # 使用数据库
        sql_list.append("USE `%s`;" % db_name)
        # 表不存在则创建
        sql_list.append(
            """
            CREATE TABLE IF NOT EXISTS `user_message_table`(
                `transaction_id` CHAR(50) NOT NULL,
                `frag_num` INT NOT NULL,
                `sender_name` CHAR(20) NOT NULL,
                `reciver_name` CHAR(20) NOT NULL,
                `content` TEXT(1024) NOT NULL,
                `time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(`transaction_id`, `frag_num`)
            );
            """
        )
        sql_list.append(
            """
            CREATE TABLE IF NOT EXISTS `group_message_table`(
                `transaction_id` CHAR(50) NOT NULL,
                `frag_num` INT NOT NULL,
                `sender_name` CHAR(20) NOT NULL,
                `group_name` CHAR(20) NOT NULL,
                `content` TEXT(1024) NOT NULL,
                `time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY(`transaction_id`, `frag_num`)
            );
            """
        )
        for sql in sql_list:
            self._cursor.execute(sql)

    def close(self):
        self._cursor.close()
        self._conn.close()

    def _user_message_save_sql(self, transaction_id, frag_num, sender_name, reciver_name, content):
        """
        生成点对点聊天的信息存储sql指令

        Args:
            transaction_id ([type]): [description]
            frag_num ([type]): [description]
            sender_name ([type]): [description]
            reciver_name ([type]): [description]
            content ([type]): [description]

        Returns:
            [type]: [description]
        """
        sql = "INSERT INTO `user_message_table` \
            (`transaction_id`, `frag_num`, `sender_name`, `reciver_name`, `content`) \
            VALUES ('%s', '%d', '%s', '%s', '%s');" % \
            (transaction_id, frag_num, sender_name, reciver_name, content)
        return sql

    def _group_message_save_sql(self, transaction_id, frag_num, sender_name, group_name, content):
        """
        生成群聊的信息存储sql指令

        Args:
            transaction_id ([type]): [description]
            frag_num ([type]): [description]
            sender_name ([type]): [description]
            group_name ([type]): [description]
            content ([type]): [description]

        Returns:
            [type]: [description]
        """
        sql = "INSERT INTO `group_message_table` \
            (`transaction_id`, `frag_num`, `sender_name`, `group_name, `content`) \
            VALUES ('%s', '%d', '%s', '%s', '%s');" % \
            (transaction_id, frag_num, sender_name, group_name, content)
        return sql

    def message_save(self, message):
        """
        存储消息

        Args:
            message ([type]): [description]

        Returns:
            [type]: [description]
        """
        content = self._conn.escape_string(message["content"])
        if message["session_type"] == "user":
            sql = self._user_message_save_sql(message["transaction_id"], message["frag_num"],
                                              message["sender_name"], message["target_name"], content)
        else:
            sql = self._group_message_save_sql(message["transaction_id"], message["frag_num"],
                                               message["sender_name"], message["target_name"], content)
        try:
            self._cursor.execute(sql)
            self._conn.commit()
        except Exception as e:
            self._conn.rollback()
            return False
        return True
