import re
import pandas as pd
import numpy as np
import os, sys, glob

import json
import itertools
#from urllib.request import urlopen
#import requests, xmltodict
import time, datetime
import math
# from pprint import pprint
# import gc
from tqdm import tqdm
tqdm.pandas()
import pickle

from utils_common import np_unique_nan
from utils_io import save_df_lst_to_excel, save_df_to_excel
from utils_io import logger #, restore_df_from_pickle

if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    from utils_io import logger

from  local_dictionaries import sevice_sections, service_chapters, service_types_A, service_types_B, service_classes_A, service_classes_B
from  local_dictionaries import dict_ath_anatomy, dict_ath_therapy, dict_ath_pharm, dict_ath_chemical


AZ_lst = [chr(i) for i in range(ord('A'), ord('Z')+1)]
# print(AZ_lst)
AZ_ru_lst = ['А', 'В', 'С', 'D', 'Е', 'F', 'G', 'Н', 'I', 'J', 'К', 'L', 'М', 'N', 'О', 'Р', 'Q', 'R', 'S', 'Т', 'U', 'V', 'W', 'X', 'Y', 'Z']
cyr2lat_dict = dict(zip(AZ_ru_lst, AZ_lst))
def code_cyr2lat(s):
    if type(s)==str:
        s_tr = ''
        for ch in s:
            if ((( ord(ch) >= ord("A")) and (ord(ch) <= ord("Z"))) or ((ord(ch) >= ord('0')) and (ord(ch) <= ord('9')))):
                s_tr += ch
            else:
                ch_tr = cyr2lat_dict.get(ch)
                if ch_tr is not None:
                    s_tr += ch_tr
                else:return s
    else: return s
    return s_tr
def extract_groups_from_service_code(s, debug = False):
    global service_types_A, service_types_B, service_classes_A, service_classes_B
    # groups = None
    if s is None or (type(s)!=str): return None
    # кодировка всегда присутсвует до вида, подвила можетне быть
    code_A_mandatory_template = r"^A\d\d\.\d\d\.\d\d\d"
    code_B_mandatory_template = r"^B\d\d\.\d\d\d\.\d\d\d"
    if re.search(code_A_mandatory_template, s) is None and re.search(code_B_mandatory_template, s) is None:
        if debug: print("Неправильный формат кода услуги", s )
        return None
    groups = {}
    if s[0] =='A':
        groups['Тип'] = service_types_A.get(s[1:3])
        groups['Класс'] = service_classes_A.get(s[4:6])
    elif s[0] =='B':
        groups['Тип'] = service_types_B.get(s[1:3])
        groups['Класс'] = service_classes_B.get(s[4:7])
    return groups.values()

def read_tkbd(path_tkbd_source, fn_tk_bd):
    try:
        df_services = pd.read_excel(os.path.join(path_tkbd_source, fn_tk_bd), sheet_name = 'Услуги')
        print(df_services.shape)
        display(df_services.head(2))
    except Exception as err:
        logger.error(str(err))
        logger.error(f"Обработка перкращена: в Excel файле отсутсnвует лист 'Усулги'")
        sys.exit(2)
    try:
        df_LP = pd.read_excel(os.path.join(path_tkbd_source, fn_tk_bd), sheet_name = 'ЛП')
        print(df_LP.shape)
        display(df_LP.head(2))
    except Exception as err:
        logger.error(str(err))
        logger.error(f"Обработка перкращена: в Excel файле отсутсnвует лист 'ЛП'")
        sys.exit(2)
    try:
        df_RM = pd.read_excel(os.path.join(path_tkbd_source, fn_tk_bd), sheet_name = 'РМ')
        print(df_RM.shape)
        display(df_RM.head(2))
    except Exception as err:
        logger.error(str(err))
        logger.error(f"Обработка перкращена: в Excel файле отсутсnвует лист 'РМ'")
        sys.exit(2)

    return df_services, df_LP, df_RM

def extract_codes_groups(s, debug=False):
    service_section, service_type_code, service_type, service_class_code, service_class = None, None, None, None, None
    if type(s) is None or ((type(s)==float) and np.isnan(s)) or (type(s)!=str):
        return [None, None, None, None, None]
    code_A_mandatory_template = r"^A\d\d\.\d\d\.\d\d\d"
    code_B_mandatory_template = r"^B\d\d\.\d\d\d\.\d\d\d"
    if re.search(code_A_mandatory_template, s) is None and re.search(code_B_mandatory_template, s) is None:
        if debug: print("Неправильный формат кода услуги", s )
        return [None, None, None, None, None]
    service_type, service_class = list(extract_groups_from_service_code(s))
    service_section = s[0]
    service_type_code = s[0:3]
    if service_section == 'A':
        service_section_name = 'КОМПЛЕКС медицинских вмешательств'
        service_class_code = s[0:6]
    else: 
        service_section_name = 'ВИДЫ медицинских вмешательств'
        service_class_code = s[0:7]
    
    return service_section, service_section_name, service_type_code, service_type, service_class_code, service_class

def extract_codes_groups_ATH(s, debug = False):
    ath_anatomy_code, ath_anatomy, ath_therapy_code, ath_therapy, ath_pharm_code, ath_pharm, ath_chemical_code, ath_chemical = \
        None, None, None, None, None, None, None, None
    if type(s) is None or ((type(s)==float) and np.isnan(s)) or (type(s)!=str):
        return None, None, None, None, None, None, None, None
    code_mandatory_template = r"^[A-ZА-Я]{1}\d\d[A-ZА-Я]{2}"
    if re.search(code_mandatory_template, s) is None:
        if debug: print("Неправильный формат кода АТХ", s )
        if len(s) == 6: 
            if debug: print("Используем правильный формат кода АТХ", s, '->', s[:5])
            s = s[:5]
        elif (len(s) > 6) and '/' in s:
            if debug: print("Пытаемся использовать правильный формат кода АТХ", s, '->', s[:5])
            s = s.split('/')[0] # берем 1-й код пока
            if len(s)!=5:
                return None, None, None, None, None, None, None, None
        else:
            return None, None, None, None, None, None, None, None
    s = code_cyr2lat(s)
    if re.search(code_mandatory_template, s) is None:
        return None, None, None, None, None, None, None, None
    ath_anatomy_code = s[0]
    ath_anatomy = dict_ath_anatomy.get(ath_anatomy_code)
    ath_therapy_code = ath_anatomy_code + s[1:3]
    ath_therapy = dict_ath_therapy.get(ath_therapy_code)
    ath_pharm_code = ath_therapy_code + s[3]
    ath_pharm = dict_ath_pharm.get(ath_pharm_code)
    ath_chemical_code = ath_pharm_code + s[4]
    ath_chemical = dict_ath_chemical.get(ath_chemical_code)
    return ath_anatomy_code, ath_anatomy, ath_therapy_code, ath_therapy, ath_pharm_code, ath_pharm, ath_chemical_code, ath_chemical

def preprocess_services(df_services):
    
    service_name_col = 'Наименование услуги по Номенклатуре медицинских услуг (Приказ МЗ №804н)'
    df_services[service_name_col] = df_services[service_name_col].progress_apply(lambda x: x.strip() if x is not None else None)

    new_services_columns = ['Код раздела', 'Раздел', 'Код типа', 'Тип', 'Код класса', 'Класс' ]
    code_services_col = 'Код услуги по Номенклатуре медицинских услуг (Приказ МЗ № 804н)'
    df_services[new_services_columns] = df_services[code_services_col].progress_apply(lambda x: pd.Series(extract_codes_groups(x)))



    return df_services

def preprocess_LP(df_LP, smnn_list_df):
    # global smnn_list_df
    LP_name_col = 'Наименование лекарственного препарата (ЛП) (МНН)'
    df_LP[LP_name_col] = df_LP[LP_name_col].progress_apply(lambda x: x.strip() if x is not None else None)
    PhF_name_col = 'Форма выпуска лекарственного препарата (ЛП)'
    df_LP[PhF_name_col] = df_LP[PhF_name_col].progress_apply(lambda x: x.strip() if x is not None else None)

    mnn_col_name = 'Наименование лекарственного препарата (ЛП) (МНН)'
    df_LP['ФТГ'] = None
    for i_row, row in tqdm(df_LP.iterrows(), total = df_LP.shape[0]):
        mnn = row[mnn_col_name].replace('\n','').strip()
        mnn_upper = mnn.upper()
        rez_values = smnn_list_df.query(f"mnn_standard == '{mnn_upper}'")['ftg'].values
        ftg = None
        if rez_values.shape[0]>0:
            # if rez_values.shape[0]==0:
            #     ftg = rez_values[0]
            # else: ftg = str(list(rez_values))
            ftg = np_unique_nan(rez_values)
        else:
            rez_values = smnn_list_df[smnn_list_df["mnn_standard"].notnull() & smnn_list_df["mnn_standard"].str.contains(mnn, case=False)]['ftg'].values
            if rez_values.shape[0]>0:
                # if rez_values.shape[0]==0:
                #     ftg = rez_values[0]
                # else: ftg = str(list(rez_values))
                ftg = np_unique_nan(rez_values)
            else:
                print(i_row, f"Не найдено МНН: '{mnn}'")
        df_LP.loc[i_row, 'ФТГ'] = str(ftg) if ftg is not None else None
    print("Не найдено МНН:", df_LP[df_LP['ФТГ'].isnull()].shape[0])
    display(df_LP.head(2))

    new_ATH_cols = ['Код анатомического органа или системы', 'Наименование анатомического органа или системы',
       'Код терапевтической группы', 'Наименование терапевтической группы',
       'Код фармакологической группы', 'Наименование фармакологической группы',
       'Код химической группы', 'Наименование химической группы',]
    ATH_code_col_name = 'Код группы ЛП (АТХ)'
    df_LP[new_ATH_cols] = None
    df_LP[new_ATH_cols] = df_LP[ATH_code_col_name].progress_apply(lambda x: pd.Series(extract_codes_groups_ATH(x, True)))
    print(df_LP[df_LP['Наименование химической группы'].isnull()].shape, df_LP[df_LP['Код химической группы'].isnull()].shape)
    display(df_LP.head(2))
    # print(df_LP[df_LP['Наименование химической группы'].isnull()]['Код группы ЛП (АТХ)'].unique,
    #       df_LP[df_LP['Код химической группы'].isnull()]['Код группы ЛП (АТХ)'].unique)
    # display(df_LP[df_LP['Код химической группы'].isnull()])
    # display(df_LP[df_LP['Код группы ЛП (АТХ)']=='V070AB'])
    
    return df_LP

def preprocess_RM(df_RM):
    
    RM_name_col = 'Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании медицинской услуги'
    df_RM[RM_name_col] = df_RM[RM_name_col].progress_apply(lambda x: x.strip() if x is not None else None)
    return df_RM

from utils_io_spec import load_check_dictionaries_services

def preprocess_tkbd(
    path_tkbd_source, fn_tk_bd,
    path_tk_models_processed,
    supp_dict_dir
    ):
    
    # upload_files_services()
    df_services_MGFOMS, df_services_804n, smnn_list_df = load_check_dictionaries_services(supp_dict_dir)
    
    df_services, df_LP, df_RM = read_tkbd(path_tkbd_source, fn_tk_bd)
    df_services = preprocess_services(df_services)
    df_LP = preprocess_LP(df_LP, smnn_list_df)
    df_RM = preprocess_RM(df_RM)

    total_sheet_names = ['Услуги', 'ЛП', 'РМ']
    fn =  'tkbd_enriched.xlsx'
    fn_save = save_df_lst_to_excel([df_services, df_LP, df_RM], total_sheet_names, path_tk_models_processed, fn)
    logger.info(f"Файл '{fn_save}' сохранен в директорию '{path_tk_models_processed}'")

    return df_services, df_LP, df_RM
