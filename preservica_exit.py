import os
import re
import shutil
import time
import logging
from bs4 import BeautifulSoup
from bs4.formatter import XMLFormatter
from zipfile import ZipFile
from datetime import datetime
from pathlib import Path
from bdbag import bdbag_api

#TODO Update project paths as appropriate
pax_opex_path = 'M:/IDT/DAM/Preservica_Exit_Strategy/pax_opex'
bags_mets_path = 'M:/IDT/DAM/Preservica_Exit_Strategy/bags_mets'
stage_path = 'M:/IDT/DAM/Preservica_Exit_Strategy/staging'

def preservica_exit():
    project_start_time = time.time()
    log_time = datetime.now()
    log_time_str = log_time.strftime('%Y-%m-%d')
    logging.basicConfig(filename=log_time_str + '_preservica_exit.log', filemode='a', format='%(asctime)s %(levelname)s %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)
    pax_opex_all = os.listdir(path = pax_opex_path)
    convert_total = len(pax_opex_all)
    running_count = 0
    for pax_opex in pax_opex_all:
        try:
            start_time = time.time()
            os.mkdir(stage_path)
            current_pax_opex = os.path.join(pax_opex_path, pax_opex)
            with ZipFile(current_pax_opex, 'r') as zip_obj_outer:
                zip_obj_outer.extractall(path=stage_path)
            for entity in os.listdir(path=stage_path):
                if entity.endswith('.zip'):
                    zip_path = os.path.join(stage_path, entity)
                    with ZipFile(zip_path, 'r') as zip_obj_inner:
                        zip_obj_inner.extractall(path=stage_path)
                    os.remove(zip_path)
            opex = ''
            xip = ''
            for file in os.listdir(path=stage_path):
                file_path = os.path.join(stage_path, file)
                if file.endswith('.opex'):
                    fhand = open(file_path, 'r', encoding='utf8')
                    opex = fhand.read()
                    fhand.close()
                if file.endswith('.xip'):
                    ghand = open(file_path, 'r', encoding='utf8')
                    xip = ghand.read()
                    xip = xip.replace('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>', '')
                    ghand.close()
            preservica_id = re.findall('<SourceID>(.+?)</SourceID>', opex)[0]
            title = re.findall('<Title>(.+?)</Title>', opex)[0]
            mets_root = '''<?xml version="1.0" encoding="UTF-8"?>
            <mets:mets xmlns:mets="http://www.loc.gov/METS/v2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:premis="http://www.loc.gov/premis/v3" xmlns:dcterms="http://purl.org/dc/terms/" xsi:schemaLocation="http://www.loc.gov/METS/v2 https://raw.githubusercontent.com/mets/METS-schema/mets2/v2/mets.xsd http://www.loc.gov/premis/v3 https://www.loc.gov/standards/premis/v3/premis-v3-0.xsd http://purl.org/dc/elements/1.1/ https://www.dublincore.org/schemas/xmls/qdc/2008/02/11/dc.xsd http://purl.org/dc/terms/ https://www.dublincore.org/schemas/xmls/qdc/2008/02/11/dcterms.xsd" OBJID="{objid}" LABEL="{label}">'''.format(objid=preservica_id, label=title)
            now = datetime.now()
            date_time = now.strftime('%Y-%m-%dT%H:%M:%S')
            mets_hdr_open = '''<metsHdr CREATEDATE="{date_time}">
                <mets:agent ROLE="CREATOR" TYPE="INDIVIDUAL">
                    <mets:name>John Dewees, Senior Digital Asset Management Specialist, Digital Initiatives</mets:name>
                </mets:agent>
                <mets:agent ROLE="CREATOR" TYPE="ORGANIZATION">
                    <mets:name>University of Rochester, River Campus Libraries</mets:name>
                </mets:agent>'''.format(date_time=date_time)
            identifiers = re.findall('<Identifier type="(.+?)">(.+?)</Identifier>', opex)
            mets_hdr_ids = ''
            uri = ''
            for id in identifiers:
                if id[0] == 'uri':
                    uri = id[1]
                else:
                    mets_hdr_ids += '<mets:altRecordID TYPE="{id_type}">{id_value}</mets:altRecordID>'.format(id_type=id[0], id_value=id[1])
            mets_hdr_close = '</metsHdr>'
            mets_md_all_open = '<mets:mdSec>'
            dc_terms = re.findall(r'<ns0:dcterms xmlns:ns0="http://purl.org/dc/terms/" xmlns="http://preservica.com/XIP/v7.2" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" attributeFormDefault="unqualified" elementFormDefault="qualified" targetNamespace="http://purl.org/dc/terms/">.+?</ns0:dcterms>', opex, re.DOTALL)
            mets_md_desc_open = '<mets:mdGrp USE="DESCRIPTIVE">'
            mets_md_desc = ''
            if len(dc_terms) > 0:
                dc_terms = dc_terms[0]
                dc_terms = dc_terms.replace('ns0', 'dcterms')
                mets_md_desc = '''<mets:md ID="item_md">
                        <mets:mdWrap MDTYPE="DCQ">
                            <mets:xmlData>
                                {dc_terms}
                            </mets:xmlData>
                        </mets:mdWrap>
                    </mets:md>'''.format(dc_terms=dc_terms)
            if uri != '':
                mets_md_desc += '''<md ID="aspace"><mdRef LABEL="collection_md" LOCTYPE="URI" MDTYPE="EAD" LOCREF="{uri}"/></md>'''.format(uri=uri)
            mets_md_desc_close = '</mets:mdGrp>'
            mets_md_admin_open = '<mets:mdGrp USE="ADMINISTRATIVE">'
            premis_rights = re.findall(r'<ns0:rights>.+?</ns0:rights>', opex, re.DOTALL)
            mets_md_rights = ''
            if len(premis_rights) > 0:
                premis_rights = premis_rights[0]
                premis_rights = premis_rights.replace('ns0', 'premis')
                mets_md_rights = '''<mets:md ID="rights" USE="RIGHTS">
                    <mets:mdWrap MDTYPE="PREMIS:RIGHTS" MDTYPEVERSION="3.0">
                        <mets:xmlData>
                            {premis_rights}
                        </mets:xmlData>
                    </mets:mdWrap>
                </mets:md>'''.format(premis_rights=premis_rights)
            mets_md_events_list = re.findall(r'<ns0:event>.+?</ns0:event>', opex, re.DOTALL)
            mets_md_events = ''
            if len(mets_md_events_list) > 0:
                pos = 0
                for event in mets_md_events_list:
                    mets_md_events_list[pos] = event.replace('ns0', 'premis')
                    pos += 1
                event_id = 1
                for event_md in mets_md_events_list:
                    mets_md_events += '''<mets:md ID="event-{event_num}" USE="PROVENANCE">
                    <mets:mdWrap MDTYPE="PREMIS:EVENT" MDTYPEVERSION="3.0">
                        <mets:xmlData>
                            {event_md}
                        </mets:xmlData>
                    </mets:mdWrap>
                </mets:md>
                '''.format(event_num = str(event_id).zfill(3), event_md=event_md)
                    event_id += 1
            mets_md_tech = '''<mets:md ID="tech" USE="TECHNICAL"> 
                <mets:mdWrap MDTYPE="XIP">
                    <mets:xmlData>
                        {xip}
                    </mets:xmlData>
                </mets:mdWrap>
            </mets:md>
            '''.format(xip=xip)
            mets_md_admin_close = '</mets:mdGrp>'
            mets_md_all_close = '</mets:mdSec>'
            mets_footer = '</mets:mets>'
            mets_list = [mets_root, mets_hdr_open, mets_hdr_ids, mets_hdr_close, mets_md_all_open, mets_md_desc_open, mets_md_desc, mets_md_desc_close, mets_md_admin_open, mets_md_rights, mets_md_events, mets_md_tech, mets_md_admin_close, mets_md_all_close, mets_footer]
            mets_raw = ''.join(mets_list)
            mets_temp = BeautifulSoup(mets_raw, "xml") 
            formatter = XMLFormatter(indent=4)
            mets_final = mets_temp.prettify(formatter=formatter) 
            with open(os.path.join(stage_path, preservica_id + '_mets.xml'), 'w', encoding='utf8') as mets_md:
                mets_md.write(mets_final)
            scan_stage = Path(stage_path).rglob('*')
            bag_stage = os.path.join(stage_path, preservica_id + '_bag')
            os.mkdir(bag_stage)
            bag_size = 0
            for entity in scan_stage:
                if str(entity).endswith('.xip'):
                    continue
                elif str(entity).endswith('.opex'):
                    continue
                elif entity.is_file() == True:
                    bag_size += entity.stat().st_size
                    shutil.move(entity, os.path.join(bag_stage, entity.name))
            bag_size = str(round(bag_size / 1000000, 2)) + ' MB'
            date = now.strftime('%Y-%m-%d')
            source_organization = 'University of Rochester'
            contact_name = 'John Dewees'
            contact_email = 'john.dewees@rochester.edu'
            bdbag_api.make_bag(bag_stage, algs=['sha1'], metadata={'Source-Organization':source_organization, 'Contact-Name':contact_name, 'Contact-Email':contact_email, 'External-Identifier':preservica_id, 'Bagging-Date':date, 'Bag-Size':str(bag_size)})
            bdbag_api.archive_bag(bag_stage, 'zip')
            for bag in os.listdir(path = stage_path):
                if bag.endswith('_bag.zip'):
                    src_path = os.path.join(stage_path, bag)
                    dst_path = os.path.join(bags_mets_path, bag)
                    shutil.move(src_path, dst_path)
            shutil.rmtree(stage_path)
            os.remove(current_pax_opex)
            running_count += 1
            end_time = time.time()
            duration = int(end_time - start_time)
            print('{running_count}/{convert_total} - {title} - {preservica_id}_bag.zip - {duration} seconds'.format(running_count=running_count, convert_total=convert_total, title=title, preservica_id=preservica_id, duration=duration))
            logger.info('{running_count}/{convert_total} - {title}- {preservica_id}_bag.zip - {duration} seconds'.format(running_count=running_count, convert_total=convert_total, title=title, preservica_id=preservica_id, duration=duration))
        except:
            logger.error('ASSET CONVERSION FAILED {running_count}/{convert_total} - {preservica_id}_bag.zip - {duration} seconds'.format(running_count=running_count, convert_total=convert_total, preservica_id=preservica_id, duration=duration))
            continue
    project_end_time = time.time()
    total_time = int(project_end_time - project_start_time)
    str_time = time.strftime("%H:%M:%S", time.gmtime(total_time))
    print('-----------------------------------------------------------------\nPAX/OPEX conversion to Bag/METS complete\ntotal bags created: {running_count}\ntotal elapsed time: {str_time}'.format(running_count=running_count, str_time=str_time))
    logger.info('PAX/OPEX conversion to Bag/METS complete | total bags created: {running_count} | total elapsed time: {str_time}'.format(running_count=running_count, str_time=str_time))
# preservica_exit()
