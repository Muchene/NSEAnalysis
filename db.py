# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import psycopg2
from matplotlib import pyplot
from matplotlib import dates as dt
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import numpy as np
import traceback
import json

class DB(object): 
    
    def __init__(self, fname):
        
        self.config = {}
        with open(fname) as f:
            self.config = json.load(f)
            
        
        self.server = SSHTunnelForwarder(
                                         (self.config["server"]["addr"]),
                                         ssh_password=self.config["server"]["pwd"],
                                         ssh_username=self.config["server"]["uname"],
                                         remote_bind_address=(self.config["server"]["bindAddr"][0],int(self.config["server"]["bindAddr"][1])),# this needs to be the port on the remote server,
                                         )
        self.server.start()
        self.settings = self.config["settings"] 
        self.settings["port"] = self.server.local_bind_port
        self.conn = None
        self.company_names = {}
        self.connect()
        
    def connect(self):
        if self.conn != None:
            self.conn.close()
        settings = self.settings
        conn_str = "host='{}' port='{}' dbname='{}' user='{}' password='123456'".format(settings["host"],settings["port"], settings["dbname"], settings["user"]) #,settings["password"]) 
        #print(conn_str)
        self.conn = psycopg2.connect(conn_str)
        
    def close(self):
        self.server.stop()
        self.conn.close()
            

    def grab_data(self, companies, num_points=1000000):
        """
        For each company in companies, return num_points datapoints representing
        the company stock price from [now-(num_points*30min), now]
        """
        
        comp_placeholder = ["%s" for i in range(len(companies))]
        comp_placeholder = "(" + ",".join(comp_placeholder) + ")"
        
        query = """
            SELECT T.company_id, T."time", T.price FROM (
                SELECT ROW_NUMBER() OVER (PARTITION BY price.company_id ORDER BY price.time DESC) as r,
                price.company_id, price.time, price.price 
                FROM price WHERE company_id in {} ) as T
            WHERE r <= {} """.format(comp_placeholder, num_points)
        
        cur = self.conn.cursor()
        try:
            cur.execute(query,tuple(companies))
        except:
            traceback.print_exc()
            cur.close()
            return None
            
        tmp = {}
        for i in range(num_points*len(companies)):
            point = cur.fetchone()
            if point == None: 
                break
            if point[0] not in tmp:
                tmp[point[0]] = [[],[]]
            tmp[point[0]][0].append(point[1])
            tmp[point[0]][1].append(point[2])
        ret = {}
        for company in tmp:
            ret[company] = pd.Series(tmp[company][1],tmp[company][0])
        cur.close()
        return ret
        
    def company_name(self, company_id):
        if company_id in self.company_names:
            return self.company_names[company_id]
        query = "SELECT company_id, company_name FROM companies"""
        curr = self.conn.cursor()
        try:
            curr.execute(query)
        except:
            traceback.print_exc()
            curr.close()
            return ""
            
        ret = str()
        for row in curr.fetchall():
            self.company_names[row[0]] = row[1]
            if row[0] == company_id:
                ret= row[1]
        return ret
    
def test():
    db = DB("config.json")
    data = db.grab_data([1,2],10000)
    data[1].plot()
    data[2].plot()
    
