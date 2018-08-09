# Hive
"""
Ce script gere le preprocessing des donnees brutes

Les donnees brutes sont chargees, les lignes de transaction sont filtrees et regroupees

Script specifique a DSC

* !pip3 install thrift==0.9.3
* !pip3 install thrift_sasl
* !pip3 install sasl
* !pip3 install impyla
* !pip3 install impala

* !pip install thrift==0.9.3
* !pip install thrift_sasl
* !pip install sasl
* !pip install impyla
* !pip install impala
"""

* import pandas as pd
* import numpy as np
* import logging
* import os
* from impala.dbapi import connect
* from impala.util import as_pandas
* from csv import QUOTE_NONE
* from datetime import datetime
* import json
* import time


class HiveInterface:
  
  def __init__(self):
      ####################
      # Parametres de connexion a Hive
      ####################
      # Serveur
      HIVE_HOST = 'acfsv817bed179p.bigdata4sg.saint-gobain.net'  
      # Port
      HIVE_PORT = 10000
      # Login
      HIVE_LOGIN = 'hive'
      # Mot de passe
      HIVE_PASSWORD = ''
      # Base de donnees
      HIVE_DATABASE = 'calc_shd_dev'
      # mode d'authenfication
      HVIE_AUTH_MECANISM = 'GSSAPI'
      #'GSSAPI'
      # Nom du service sous Kerberos
      HIVE_KERBEROS_SERVICE_NAME = 'hive'

      # Connexion a Hive sur un cluster Hadoop securise (Kerberos)
      print("Connexion a Hive sur un cluster Hadoop securise (Kerberos) ...")
      self.hive_conn = connect(host=HIVE_HOST,
                     port=HIVE_PORT,
                     auth_mechanism= HVIE_AUTH_MECANISM,
                     user=HIVE_LOGIN,
                     password=HIVE_PASSWORD,
                     database=HIVE_DATABASE,
                     kerberos_service_name=HIVE_KERBEROS_SERVICE_NAME)

      print("DONE ...")
      # creation d'un curseur
      print("creation d'un curseur + requete SQL ...")
      cursor = self.hive_conn.cursor()
      self.cursor = cursor


  def run_request(self, bm):
      # execution d'une requete SQL
      #la source de chargement de la table calc_shd_dev.dataset_calc est as400_nat_pp03_stanat_raw_dev.stnlfa_to_process
      self.cursor.execute(bm)
      #print("DONE ...")
      # transformer les donnees en dataFrame Panda
      raw = as_pandas(self.cursor)
      
      return raw
  
  
  def close_connection(self):
      # fermeture du cusreur
      self.cursor.close()
      # fermeture de la connexion
      self.hive_conn.close()   
      
