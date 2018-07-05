# -*- coding: utf-8 -*-
import os
import sys
import glob
import json
import calendar, time
from datetime import datetime
import csv
from ftplib import FTP
import pymssql
from lxml import etree
from xml.etree.ElementTree import ElementTree
import multiprocessing

from vtv_task import VtvTask, vtv_task_main
from vtv_db import get_mysql_connection
from data_schema import get_schema
from vtv_utils import VTV_DATAGEN_DIR, VTV_SERVER_DIR, copy_file

from delivery_base import DeliveryBase

DELIVER_FILE_PREFIX='orange_delivery'

class DeliverData(DeliveryBase):
    def __init__(self):
        VtvTask.__init__(self)
        self.init_config(self.options.config_file)
        self.section = self.options.section
        self.source = 'ORANGE'
        DeliveryBase.__init__(self, self.source, self.section)

        self.guid_map = {}
        self.sk_rovi_map = {}
        self.new_gids = []
        self.default_stats = {'old_base_file_record': 0, 'show': 0, 'series': 0, 'tvshow': 0, 'movie': 0, 'episode': 0, 'tvseries': 0, 'sports': 0, 'None': 0}
        self.typ_stats = self.default_stats.copy()
        self.prev_typ_stats = self.default_stats.copy()
        self.total_typ_stats = self.default_stats.copy()
        self.sk_where_matched = {}
        self.sks_in_paid = set()
        self.sks_in_paid_modified = set()

        self.db_ip = self.get_config_value('META_DB_IP',self.section)
        self.db_name = self.get_config_value('META_DB_NAME',self.section)
        self.cursor, self.db = get_mysql_connection(self.logger, self.db_ip, self.db_name,user='veveo',passwd='veveo123',cursorclass='ss')
        

    def set_options(self):
        config_file = os.path.join(self.system_dirs.VTV_ETC_DIR, 'catalogue_matching.cfg')
        self.parser.add_option('-c', '--config-file', metavar='FILE', default=config_file, help='config file for content data generation')
        self.parser.add_option('-i', '--input_file', default='', help='Give path of input file')
        self.parser.add_option('-s', '--section', default='TIVO', help='Source name')
        self.parser.add_option('', '--prune-geography', default='all_unbound,usa_bound,can_bound', help='prune if rovi id not from these extracts')
        self.parser.add_option('', '--no-upload', metavar='BOOL', help = "if specified, will not upload the files", action='store_true')

    def generate_output(self):
        self.service_provider = self.get_config_value('SERVICE_PROVIDER', self.section)
        self.idspace = self.get_config_value('IDSPACE', self.section)

        self.json_output_list = []
        pid_not_found = 0
        sk_count = 0
        print 'Size of original_guid_map %d' % len(self.original_guid_map)
        for guid_map_hash, list_to_populate in [(self.original_guid_map, self.json_output_list)]:
            for sk, rv_id in guid_map_hash.items():
                if not rv_id:
                    self.logger.info("sk->%s have no rv id"%sk)
                    print "sk %s have no Rovi Id" % sk

                sk_count = sk_count + 1
                id_name = self.gid_provider_map.get(sk)
                if not id_name:
                    query = "select content_provider from orange_metacontent where sk = '%s'" % sk
                    self.cursor.execute(query)
                    for row in self.cursor.fetchall():
                        id_name, = row
                        print 'id_name from db is %', id_name
                    if not id_name:
                        pid_not_found += 1
                        print "Provider ID not found for Sk ", sk
                        continue

                di = {"rovi_id": {"rovi_id_2.0": rv_id}, "service_provider": "Orange", "ref": {"idspace": "Asset_ID", "id": sk, "Provider_ID": id_name}}
                list_to_populate.append(di)

        print "Provider ID NOT FOUND COUNT : %s" % pid_not_found
        day = '%02d' % datetime.today().day
        month ='%02d' % datetime.today().month
        self.todays_date = '%s%s%s'%(datetime.today().year, month, day)

        self.json_file = '%s_%s.json'%(DELIVER_FILE_PREFIX, self.todays_date)
        fp = open(self.json_file, 'w')
        for json_obj in self.json_output_list:
            fp.write('%s\n'%(json.dumps(json_obj)))
        fp.close()
        print 'Count of records written to delivery file %s is %d' % (self.json_file, len(self.json_output_list))

    def get_latest_base_file(self, file_path, pattern = '201*json'):
        file_list = glob.glob(file_path+'/%s_%s' % (DELIVER_FILE_PREFIX, pattern))
        file_map = {}
        date_list = []
        for filename in file_list:
            date = filename.split('_')[-1].replace('.xml', '').replace('.json', '')
            date_list.append(int(date))
            file_map[date] = filename
        date_list.sort()
        if not date_list:
            return ''
        else:
            max_date = date_list[-1]
            return file_map[str(max_date)]

    def load_pre_matched_guid_pair(self):
        json_file = self.get_latest_base_file(self.json_path)
        print 'Latest base file:%s' %json_file

        if not json_file:
            print 'No JSON file found to load pre matched guid pair'
            return

        for line in open(json_file).readlines():
            json_obj = json.loads(line.strip())
            rv = json_obj['rovi_id']['rovi_id_2.0']
            sk = json_obj['ref']['id']
            provider_name = json_obj['ref']['Provider_ID']

            if not rv:
                self.logger.info("This sk->%s from Latest base file has no rovi id"%rv)
                continue
            self.gid_provider_map[sk] =  provider_name
            self.delivery_hash[sk] =  rv

        self.update_gid_provider_map()

    def update_gid_provider_map(self):
        self.stats['total_input_records'] = 0
        self.paid_sk_map = {}
        screwd_records = 0
        input_records = self.read_orange_xml()
        for row in input_records:
            provider_key = row.get('provider_key')
            source_uid = row.get('source_uid')
            entity_type = row.get('entity_type')
            self.gid_provider_map[source_uid] = provider_key
            self.stats['total_input_records'] += 1

    def read_orange_xml(self):
        input_dir = os.path.join(self.input_tgz_dir, '') # /home/veveo/datagen/FTP_FILES/ORANGE/TGZ/
        self.file_list = glob.glob('%s/*.xml'%input_dir)
        input_records = []
        for xml_file in self.file_list:
            doc=etree.parse(xml_file)
            root=doc.getroot()
            for program in root.findall('.//Program'):
                attrib = program.attrib
                program_id = attrib.get('Id')
                out_rec={}
                out_rec['source_uid'] = program_id
                program_type = attrib.get('Type').upper()
                if program_type == 'Movie'.upper():
                    out_rec['entity_type'] = 'movie'
                elif program_type == 'Episode'.upper():
                    out_rec['entity_type'] = 'episode'
                elif program_type == 'Master'.upper():
                    out_rec['entity_type'] = 'tvseries'
                else:
                    out_rec['entity_type'] = None
                out_rec['provider_key'] = program.find('./ContentProvider').text
                input_records.append(out_rec)
        return input_records

    def push_ftp_local(self):
        upload_filename = "%s.gz"%(self.json_file)
        print 'Push FTP %s to CPI' % upload_filename
        server = self.get_config_value('FTP_NAME', self.section.upper())
        port = self.get_config_value('FTP_PORT', self.section.upper())
        user = self.get_config_value('JSON_FTP_USER', self.section.upper())
        pwd = self.get_config_value('JSON_FTP_PWD', self.section.upper())
        remote_path = self.get_config_value('JSON_FTP_REMOTE_PATH', self.section.upper())
        upload_file = open(upload_filename, 'rb')

        ftp_conn = self.connect_ftp(server, port, user, pwd)
        ftp_conn.cwd(remote_path)
        ftp_conn.storbinary('STOR '+ upload_filename, upload_file)

    def load_cw_extract(self, guid_map_hash):
        ott_file = os.path.join(VTV_SERVER_DIR, 'catalogue_matching', 'orange', 'orange_ott_static_match.txt')
        if not os.path.exists(ott_file):
            print 'File %s not present... continuing!!!' %ott_file
            return

        #('ProgramType', 'ProviderName', 'RoviID', '3PP_ProgramID')
        dict_row_list = self.read_csv_file(ott_file, ',')
        dict_pgm_typ = {'Series Episode': 'episode', 'Series Master': 'series', 'Movie': 'movie'}
        for dict_row in dict_row_list:
            ProgramType = dict_row['ProgramType']
            ProviderName = dict_row['ProviderName']
            RoviID =  dict_row['RoviID']
            PP_ProgramID = dict_row['3PP_ProgramID']
            #this is for prefferred query which has only rovi and tivo id
            if not (ProgramType or ProviderName):
                guid_map_hash[PP_ProgramID] = RoviID
                continue
            if ProgramType not in dict_pgm_typ:
                continue
            mapped_typ = dict_pgm_typ[ProgramType].lower()
            sk = "%s-%s-%s"%(ProviderName, mapped_typ, PP_ProgramID)
            guid_map_hash[sk] = RoviID


    def run_main(self):
        self.stats = {}

        self.original_vod_extract_hash = {}
        self.ott_extract_hash = {}
        self.gid_provider_map = {}

        self.load_pre_matched_guid_pair()
        print "Pre Delete no of records in base delivery file: %s"%len(self.delivery_hash)

        # KG Matching
        print 'KG Matching:'
        self.kg_matching() # from DeliveryBase

        # KG PID PAID Matching
        print 'KG PID PAID Matching:'
        copy_delivery_hash = self.delivery_hash.copy()
        self.update_paid_map_from_verified_records(self.delivery_hash, self.vod_extract_hash)

        # Static Matching
        print 'Static Matching:'
        self.static_matching() # from DeliveryBase

        # MatchMaker (Content Warehouse)
        print 'MatchMaker (Content Warehouse):'
        self.load_cw_extract(self.ott_extract_hash)

        # PID PAID Matching
        print 'PID PAID Matching:'
        self.pid_paid_matching(self.ott_extract_hash, self.original_vod_extract_hash, catalogue_dir=None, match_file=None) # from DeliveryBase

        # Catalogue (Orange) specific functions
        self.original_guid_map = self.delivery_hash.copy()
        self.prune_guid_map_based_on_rovi_extract(self.original_guid_map, self.options.prune_geography)
        self.generate_output()

        # Deliver to CPI
        self.create_tar([(self.json_path, self.json_file)])
        if not self.options.no_upload:
            print "******* Uploading Files *******"
            self.push_ftp_local()


if __name__ == '__main__':
     vtv_task_main(DeliverData)
     sys.exit( 0 )

