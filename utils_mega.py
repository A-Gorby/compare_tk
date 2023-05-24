import os, sys
import tarfile
from mega import Mega
import json
from utils_io import logger
if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    from utils_io import logger

def tar_esklp_dictionaries(esklp_date, fn_smnn_list_df, fn_klp_list_dict_df, data_esklp_processed_dir, data_tmp_dir):
    smnn_fn_tar_gz = f'smnn_{esklp_date}.tar.gz'
    klp_fn_tar_gz = f'klp_{esklp_date}.tar.gz'
    logger.info(f"Упаковка файла '{fn_smnn_list_df}' - начало...")
    with tarfile.open(os.path.join(data_tmp_dir, smnn_fn_tar_gz), 'w:gz') as tar:
        tar.add(os.path.join(data_esklp_processed_dir, fn_smnn_list_df), arcname=fn_smnn_list_df)
    tar.close()
    logger.info(f"Упаковка файла '{fn_smnn_list_df}' - завершено!")
    logger.info(f"Упаковка файла '{fn_klp_list_dict_df}' - начало...")
    with tarfile.open(os.path.join(data_tmp_dir, klp_fn_tar_gz), 'w:gz') as tar:
        tar.add(os.path.join(data_esklp_processed_dir, fn_klp_list_dict_df), arcname=fn_klp_list_dict_df)
    tar.close()
    logger.info(f"Упаковка файла '{fn_klp_list_dict_df}' - завершено!")

    return smnn_fn_tar_gz, klp_fn_tar_gz

def save_esklp_dictionaries_to_mega(m, esklp_date, fn_smnn_list_df, fn_klp_list_dict_df, data_esklp_processed_dir, data_tmp_dir):
    if esklp_date is None or (esklp_date is not None and (len(esklp_date)==0)):
        logger.error(f"Не определена переменная esklp_date")
        sys.exit(2)
    if 'fn_smnn_list_df' not in globals() or 'fn_klp_list_dict_df' not in globals():
        logger.error(f"Не определены переменные: 'fn_smnn_list_df' или 'fn_klp_list_dict_df'")
        sys.exit(2)
    smnn_fn_tar_gz, klp_fn_tar_gz = tar_esklp_dictionaries(
        esklp_date, fn_smnn_list_df, fn_klp_list_dict_df, data_esklp_processed_dir, data_tmp_dir)
    
    file = m.upload(os.path.join(data_tmp_dir, smnn_fn_tar_gz))
    smnn_link = m.get_upload_link(file)
    file = m.upload(os.path.join(data_tmp_dir, klp_fn_tar_gz))
    klp_link = m.get_upload_link(file)

    return smnn_link, klp_link

def restore_saved_esklp_dictionaries_json(ma, saved_esklp_dictionaries_json_link, data_tmp_dir):
    fn_downloaded = ma.download_url(saved_esklp_dictionaries_json_link, data_tmp_dir)
    fn_saved_esklp_dictionaries_json = fn_downloaded.parts[-1]
    print(fn_saved_esklp_dictionaries_json)
    with open(os.path.join(data_tmp_dir, fn_saved_esklp_dictionaries_json), 'r') as f:
        saved_esklp_dictionaries = json.load( f)
    return saved_esklp_dictionaries

def save_saved_esklp_dictionaries_json(m, saved_esklp_dictionaries, data_tmp_dir):
    fn_saved_esklp_dictionaries_json = 'saved_esklp_dictionaries.json'
    with open(os.path.join(data_tmp_dir, fn_saved_esklp_dictionaries_json), 'w') as f:
        json.dump(saved_esklp_dictionaries, f)
    file = m.upload(os.path.join(data_tmp_dir, fn_saved_esklp_dictionaries_json))
    saved_esklp_dictionaries_json_link = m.get_upload_link(file)
    saved_esklp_dictionaries_json_dict = {'link': saved_esklp_dictionaries_json_link}
    with open (os.path.join(data_tmp_dir, 'saved_esklp_dictionaries_link.json'), 'w') as f:
        json.dump(saved_esklp_dictionaries_json_dict, f)
    logger.info(f"Файл 'saved_esklp_dictionaries_link.json' сохранен в '{data_tmp_dir}'")
    logger.info(f"Его необходимо сохранить на https://github.com/A-Gorby/parse_esklp.git")
    return saved_esklp_dictionaries_json_link

def restore_saved_esklp_dictionaries(ma, esklp_date, saved_esklp_dictionaries, data_esklp_processed_dir, data_tmp_dir):
    if esklp_date is None or saved_esklp_dictionaries.get(esklp_date) is None:
        logger.error(f"Ошибка в описании сохраненных справочников. Обратитесь к разработчику")
        sys.exit(2)
    smnn_link = saved_esklp_dictionaries.get(esklp_date)['smnn_link']
    klp_link = saved_esklp_dictionaries.get(esklp_date)['klp_link']
    logger.info(f"Скачивание smnn - начало...")
    fn_downloaded = ma.download_url(smnn_link, data_tmp_dir)
    smnn_fn_tar_gz = fn_downloaded.parts[-1]
    logger.info(f"Скачивание smnn - завершено! '{smnn_fn_tar_gz}'")
    logger.info(f"Скачивание klp - начало...")
    fn_downloaded = ma.download_url(klp_link, data_tmp_dir)
    klp_fn_tar_gz = fn_downloaded.parts[-1]
    logger.info(f"Скачивание klp - завершено! '{klp_fn_tar_gz}'")

    with tarfile.open(os.path.join(data_tmp_dir, smnn_fn_tar_gz), 'r:gz') as tar:  
        fn_smnn_pickle = tar.getmembers()[0].name
        tar.extractall(path=data_esklp_processed_dir)
    tar.close()    
    logger.info(f"Распаковка smnn - завершена! '{fn_smnn_pickle}'")
    with tarfile.open(os.path.join(data_tmp_dir, klp_fn_tar_gz), 'r:gz') as tar:  
        fn_klp_pickle = tar.getmembers()[0].name
        tar.extractall(path=data_esklp_processed_dir)
    tar.close()    
    logger.info(f"Распаковка klp - завершена! '{fn_klp_pickle}'")

    return fn_smnn_pickle, fn_klp_pickle

def restore_saved_esklp_dictionaries_smnn(ma, esklp_date, saved_esklp_dictionaries, data_esklp_processed_dir, data_tmp_dir):
    if esklp_date is None or saved_esklp_dictionaries.get(esklp_date) is None:
        logger.error(f"Ошибка в описании сохраненных справочников. Обратитесь к разработчику")
        sys.exit(2)
    smnn_link = saved_esklp_dictionaries.get(esklp_date)['smnn_link']
    logger.info(f"Скачивание smnn - начало...")
    fn_downloaded = ma.download_url(smnn_link, data_tmp_dir)
    smnn_fn_tar_gz = fn_downloaded.parts[-1]
    logger.info(f"Скачивание smnn - завершено! '{smnn_fn_tar_gz}'")
    
    with tarfile.open(os.path.join(data_tmp_dir, smnn_fn_tar_gz), 'r:gz') as tar:  
        fn_smnn_pickle = tar.getmembers()[0].name
        tar.extractall(path=data_esklp_processed_dir)
    logger.info(f"Распаковка smnn - завершена! '{fn_smnn_pickle}'")
    
    return fn_smnn_pickle
