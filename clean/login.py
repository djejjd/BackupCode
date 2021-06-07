#! /home/hadoop/.conda/envs/spark/bin python
# -*- encoding: utf-8 -*-
"""
@File    :   login.py 
@License :   (C)Copyright 2020-2021, 毕业设计

@Modify Time      @Author    @Version    @Desciption
------------      -------    --------    -----------
2021/6/3 下午4:59   zhl      1.0         None
"""
import pymysql


def getConn():
    db = pymysql.connect(host='localhost', user='warren', password='123456', db='spark',
                         port=3306, charset='utf8')
    return db


def getLoginInfo():
    name = 'root'
    password = '1234563'
    conn = getConn()
    cur = conn.cursor()

    sql = "SELECT * FROM loginInfo WHERE name=%s and password=%s"
    ret = cur.execute(sql, [name, password])
    if ret != 0:
        result = cur.fetchall()
        print(list(result[0]))
    else:
        print("none")


def printInfo():
    conn = getConn()
    cur = conn.cursor()

    sql = "SELECT Name, Password FROM loginInfo WHERE Type=%s"

    cur.execute(sql, 0)

    for i in cur.fetchall():
        print(i)
        print(i[0])
        print(i[1])


def addInfo():
    conn = getConn()
    cur = conn.cursor()

    name = "warren"
    password = "123456"

    try:
        sql = "INSERT INTO loginInfo (Name, Password, Type) VALUES (%s, %s, %s)"
        ret = cur.execute(sql, [name, password, 0])
        conn.commit()
        # 1 表示正常添加
        print(ret)
    except:
        # 0 表示出现重复
        ret = 0
        print(ret)
    cur.close()
    conn.close()


def deleteInfo():
    conn = getConn()
    cur = conn.cursor()

    name = "warren"
    password = "123456"

    try:
        sql = "DELETE FROM loginInfo WHERE Name=%s"
        ret = cur.execute(sql, name)
        conn.commit()
        # 1 表示正常添加
        print(ret)
    except:
        # 0 表示出现重复
        ret = 0
        print(ret)
    cur.close()
    conn.close()


def updateInfo():
    conn = getConn()
    cur = conn.cursor()

    name = "warren"
    password = "123456"
    print("更新")

    try:
        sql = "UPDATE loginInfo SET Password=%s WHERE Name=%s"
        ret = cur.execute(sql, [password, name])
        conn.commit()
        # 1 表示正常修改
        print(ret)
    except:
        # 0 表示出错
        ret = 0
        print(ret)
        conn.rollback()
    cur.close()
    conn.close()


if __name__ == '__main__':
    updateInfo()
