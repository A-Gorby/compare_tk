import requests
import os
from urllib.parse import urlencode
import os
from utils_io import logger
if len(logger.handlers) > 1:
    for handler in logger.handlers:
        logger.removeHandler(handler)
    from utils_io import logger
def upload_files_services(supp_dict_dir = '/content/data/supp_dict'):
    base_url = 'https://cloud-api.yandex.net/v1/disk/public/resources/download?'
    # public_key = link #'https://yadi.sk/d/UJ8VMK2Y6bJH7A'  # Сюда вписываете вашу ссылку
    links = [('Коды МГФОМС и 804н.xlsx', 'https://disk.yandex.ru/i/lX1fVnK1J7_hfg', ('МГФОМС', '804н')),
    # ('serv_name_embeddings.pk1', 'https://disk.yandex.ru/d/8UTwZg5jKOhxXQ'),
    ('smnn_list_df_esklp_active_20230321_2023_03_24_1238.pickle', 'https://disk.yandex.ru/d/ZU318jcBw85pUg'),
    # ('НВМИ_РМ.xls', 'https://disk.yandex.ru/i/_RotfMJ_cSfeOw', 'Sheet1'),
    # ('МНН.xlsx', 'https://disk.yandex.ru/i/0rMKBimIKbS7ig', 'Sheet1'),
    # ('df_mi_national_release_20230201_2023_02_06_1013.zip', 'https://disk.yandex.ru/d/pfgyT_zmcYrHBw' ),
    # ('df_mi_org_gos_release_20230129_2023_02_07_1331.zip', 'https://disk.yandex.ru/d/Zh-5-FG4uJyLQg' ),
    # ('Специальность (унифицированный).xlsx', 'https://disk.yandex.ru/i/au5M0xyVDW2mtQ', None),
    ]

    # Получаем загрузочную ссылку
    for link_t in links:
        final_url = base_url + urlencode(dict(public_key=link_t[1]))
        response = requests.get(final_url)
        download_url = response.json()['href']

        # Загружаем файл и сохраняем его
        download_response = requests.get(download_url)
        # with open('downloaded_file.txt', 'wb') as f:   # Здесь укажите нужный путь к файлу
        with open(os.path.join(supp_dict_dir, link_t[0]), 'wb') as f:   # Здесь укажите нужный путь к файлу
            f.write(download_response.content)
            logger.info(f"File '{link_t[0]}' uploaded!")
            if link_t[0].split('.')[-1] == 'zip':
                fn_unzip = unzip_file(os.path.join(supp_dict_dir, link_t[0]), '', supp_dict_dir)
                logger.info(f"File '{fn_unzip}' upzipped!")


def load_check_dictionaries_services(path_supp_dicts):
    # global df_services_MGFOMS, df_services_804n, df_RM, df_MNN, df_mi_org_gos, df_mi_national
    # if not os.path.exists(supp_dict_dir):
    #     os.path.mkdir(supp_dict_dir)

    fn = 'Коды МГФОМС.xlsx'
    fn = 'Коды МГФОМС и 804н.xlsx'
    sheet_name = 'МГФОМС'
    df_services_MGFOMS = pd.read_excel(os.path.join(path_supp_dicts, fn), sheet_name = sheet_name)
    df_services_MGFOMS.rename (columns = {'COD': 'code', 'NAME': 'name'}, inplace=True)
    df_services_MGFOMS['code'] = df_services_MGFOMS['code'].astype(str)
    # print("df_services_MGFOMS", df_services_MGFOMS.shape, df_services_MGFOMS.columns)
    logger.info(f"Загружен справочник 'Услуги по реестру  МГФОМС': {str(df_services_MGFOMS.shape)}")

    sheet_name = '804н'
    df_services_804n = pd.read_excel(os.path.join(path_supp_dicts, fn), sheet_name = sheet_name, header=1)
    df_services_804n.rename (columns = {'Код услуги': 'code', 'Наименование медицинской услуги': 'name'}, inplace=True)
    # print("df_services_804n", df_services_804n.shape, df_services_804n.columns)
    logger.info(f"Загружен справочник 'Услуги по приказу 804н': {str(df_services_804n.shape)}")

    # fn_pickle = 'serv_name_embeddings.pk1'
    # serv_name_embeddings = restore_df_from_pickle(path_supp_dicts, fn_pickle) 
    fn_pickle = 'smnn_list_df_esklp_active_20230321_2023_03_24_1238.pickle'
    smnn_list_df = restore_df_from_pickle(path_supp_dicts, fn_pickle)
    
    return df_services_MGFOMS, df_services_804n, smnn_list_df
