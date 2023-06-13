import ipywidgets as widgets
import pandas as pd
import os
from ipywidgets import Layout, Box, Label

def form_param(fn_list):
    
    fn_check_file1_drop_douwn = widgets.Dropdown( options=fn_list, value=None) 
    fn_check_file2_drop_douwn = widgets.Dropdown( options=fn_list, value=None) 
    
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')

    check_box1 = Box([Label(value="Выберите файл со сводом данных из ТК: 'Услуги', 'ЛП', 'РМ'"), fn_check_file1_drop_douwn], layout=form_item_layout) 
    check_box2 = Box([Label(value="Выберите файл с описанием моделей:"), fn_check_file2_drop_douwn], layout=form_item_layout) 
    
    form_items = [check_box1, check_box2] 
    
    form = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form_01, fn_check_file1_drop_douwn, fn_check_file2_drop_douwn, sections_drop_douwn
    return form, fn_check_file1_drop_douwn, fn_check_file2_drop_douwn


def form_param_cmp_01(fn_list):
    profile, tk_code, tk_name, models = 'profile_test', '7777777', 'tk_test', ['План', 'Факт']
    profile_enter = widgets.Text(placeholder=profile, value=profile) #'Введите профиль ТК') # description='String:',    disabled=False
    tk_code_enter = widgets.Text(placeholder=tk_code, value=tk_code) # 'Введите Код ТК', value=tk_code) 
    tk_name_enter = widgets.Text(placeholder=tk_name, value=tk_name) #'Введите Название ТК', value=tk_name) 
    model_01_enter = widgets.Text( placeholder="План", value=models[0]) # description='Введите Название Модели 1',
    model_02_enter = widgets.Text( placeholder="Факт", value=models[1])  # description='Введите Название Модели 2'
    
    fn_check_file1_drop_douwn = widgets.Dropdown( options=fn_list, value=None) 
    fn_check_file2_drop_douwn = widgets.Dropdown( options=fn_list, value=None) 
    sections = ['Услуги', 'ЛП', 'РМ']
    sections_drop_douwn = widgets.SelectMultiple( options=sections, value=[sections[0]]) #, tips='&&&' ) 
    # cols_name_corr_drop_down = widgets.SelectMultiple( options=corr_cols_name, value= [corr_cols_name[0]], disabled=False) 
    # sheet_name_drop_douwn = widgets.Dropdown( options= [None], value= None, disabled=False)
    # col_name_drop_douwn = widgets.Dropdown( options= [None], value= None, disabled=False)
    # fn_dict_file_drop_douwn = widgets.Dropdown( options= [None] + fn_list, value= None, disabled=False, )
    # radio_btn_big_dict = widgets.RadioButtons(options=['Р”Р°', 'РќРµС‚'], value= 'Р”Р°', disabled=False) # description='Check me',    , indent=False
    # radio_btn_prod_options = widgets.RadioButtons(options=['Р”Р°', 'РќРµС‚'], value= 'РќРµС‚', disabled=False if radio_btn_big_dict.value=='Р”Р°' else True )
    # similarity_threshold_slider = widgets.IntSlider(min=1,max=100, value=90)
    # max_entries_slider = widgets.IntSlider(min=1,max=5, value=4)
    # max_out_values_slider = widgets.IntSlider(min=1,max=10, value=4)

    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')

    profile_enter_box = Box([Label(value="Введите Профиль ТК"), profile_enter], layout=form_item_layout)
    tk_code_enter_box = Box([Label(value='Введите Код ТК'), tk_code_enter], layout=form_item_layout) 
    tk_name_enter_box = Box([Label(value='Введите Название ТК'), tk_name_enter], layout=form_item_layout) 
    model_01_enter_box = Box([Label(value='Введите Название Модели 1'), model_01_enter], layout=form_item_layout) 
    model_02_enter_box = Box([Label(value='Введите Название Модели 2'), model_02_enter], layout=form_item_layout) 
    check_box1 = Box([Label(value="Выберите 1-й Excel-файл (лучше ТК) с данными: 'Услуги', 'ЛП', 'РМ'"), fn_check_file1_drop_douwn], layout=form_item_layout) 
    check_box2 = Box([Label(value="Выберите 2-й Excel-файл (ТК или факт из КИС) со данными 'Услуги', 'ЛП', 'РМ':"), fn_check_file2_drop_douwn], layout=form_item_layout) 
    multi_select = Box([Label(value="Выберите разделы (Ctrl для мнж выбора) для сравнения: 'Услуги', 'ЛП', 'РМ':"), sections_drop_douwn], layout=form_item_layout) #, tips='&&&') 
    # sheet_box = Box([Label(value='Выберите лист Excel-файла:'), sheet_name_drop_douwn], layout=form_item_layout) 
    # column_box = Box([Label(value='Р—Р°РіРѕР»РѕРІРѕРє РєРѕР»РѕРЅРєРё:'), col_name_drop_douwn], layout=form_item_layout) 
    # big_dict_box = Box([Label(value='РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ Р±РѕР»СЊС€РёРµ СЃРїСЂР°РІРѕС‡РЅРёРєРё:'), radio_btn_big_dict], layout=form_item_layout) 
    # prod_options_box = Box([Label(value='РСЃРєР°С‚СЊ РІ Р’Р°СЂРёР°РЅС‚Р°С… РёСЃРїРѕР»РЅРµРЅРёСЏ (+10 РјРёРЅ):'), radio_btn_prod_options], layout=form_item_layout) 
    # similarity_threshold_box = Box([Label(value='РњРёРЅРёРјР°Р»СЊРЅС‹Р№ % СЃС…РѕРґСЃС‚РІР° РїРѕР·РёС†РёР№:'), similarity_threshold_slider], layout=form_item_layout) 
    # max_entries_box = Box([Label(value='РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РєРѕР»-РІРѕ РЅР°Р№РґРµРЅРЅС‹С… РїРѕР·РёС†РёР№:'), max_entries_slider], layout=form_item_layout) 
    # max_out_values_box = Box([Label(value='РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РєРѕР»-РІРѕ РІС‹РІРѕРґРёРјС‹С… РїРѕР·РёС†РёР№:'), max_out_values_slider], layout=form_item_layout) 
    
    # form_items = [check_box, dict_box, big_dict_box, prod_options_box, similarity_threshold_box, max_entries_box]
    form_items = [profile_enter_box, tk_code_enter_box, tk_name_enter_box, model_01_enter_box, model_02_enter_box, check_box1, check_box2, multi_select ] 
    
    form_01 = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form_01, fn_check_file1_drop_douwn, fn_check_file2_drop_douwn, sections_drop_douwn
    return form_01, fn_check_file1_drop_douwn, fn_check_file2_drop_douwn, sections_drop_douwn, profile_enter, tk_code_enter, tk_name_enter, model_01_enter, model_02_enter

def get_col_names_from_excel(path, fn, sheets):
    cols_file = []
    for sheet in sheets:
        try:
            df = pd.read_excel(os.path.join(path, fn), sheet_name=sheet, nrows=5, header=0)
            cols_file.append(list(df.columns))
            print(sheet, list(df.columns))
        except Exception as err:
            print(err)
    
    return cols_file

def form_param_cmp_02(selected_sections, cols_file_01, cols_file_02):
    
    sections = ['Услуги', 'ЛП', 'РМ']
    tk_serv_cols = ['Код услуги по Номенклатуре медицинских услуг (Приказ МЗ № 804н)', 'Наименование услуги по Номенклатуре медицинских услуг (Приказ МЗ №804н)', #'Код услуги по Реестру МГФОМС', 
             'Усредненная частота предоставления', 'Усредненная кратность применения', 'УЕТ 1', 'УЕТ 2']
    tk_serv_cols_short = ['Код услуги', 'Наименование услуги', #'Код услуги по Реестру МГФОМС', 
             'Частота', 'Кратность', 'УЕТ 1', 'УЕТ 2']
    tk_lp_cols = ['Наименование лекарственного препарата (ЛП) (МНН)', 'Код группы ЛП (АТХ)', 'Форма выпуска лекарственного препарата (ЛП)',
              'Усредненная частота предоставления', 'Усредненная кратность применения', 'Единицы измерения', 'Кол-во']             
    tk_lp_cols_short = ['МНН', 'Код АТХ', 'Форма выпуска ЛП',
              'Частота', 'Кратность', 'Ед. измерения', 'Кол-во']
    tk_rm_cols = ['Изделия медицинского назначения и расходные материалы, обязательно используемые при оказании медицинской услуги', 'Код МИ из справочника (на основе утвержденного Перечня НВМИ)',
              'Усредненная частота предоставления', 'Усредненная кратность применения', 'Ед. измерения', 'Кол-во']
    tk_rm_cols_short = ['Код МИ/РМ', 'Название МИ/РМ',
              'Частота', 'Кратность', 'Ед. измерения', 'Кол-во']
    tk_cols = [tk_serv_cols, tk_lp_cols, tk_rm_cols]
    tk_cols_short = [tk_serv_cols_short, tk_lp_cols_short, tk_rm_cols_short]
    col_titles = ['Короткое название колонки', 'Колонка из файла 1', 'Колонка из файла 2']
    pre_cols_01, pre_cols_02 = [], []
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    form_items = []
    subforms = []
    for i, section in enumerate(selected_sections):
        pre_cols_01.append([])
        pre_cols_02.append([])
        pre_cols_01w = [widgets.Dropdown( options=cols_file_01[i], value=col if (col in cols_file_01[i]) else None) for col in tk_cols[i]] 
        pre_cols_02w = [widgets.Dropdown( options=cols_file_02[i], value=col if (col in cols_file_02[i]) else None) for col in tk_cols[i]] 
        pre_cols_01[i].extend(pre_cols_01w) #.append(pre_cols_01w)
        pre_cols_02[i].extend(pre_cols_02w)
        
        labels_w = [Label(value=col_sh) for col_sh in tk_cols_short[i]]
        form_items_w = list(zip(labels_w, pre_cols_01w, pre_cols_02w))
        # to flat list
        form_items_flat = [v for r in form_items_w for v in r]
        grid_box = widgets.GridBox([Label(s) for s in col_titles] + form_items_flat, layout=widgets.Layout(grid_template_columns="repeat(3, 30%)"))
        subforms.append(grid_box)
    # form_02 = widgets.Accordion(children=subforms, titles=tuple(selected_sections)) # v8 ipywidgets
    form_02 = widgets.Accordion(children=subforms) # v7.7.0 ipywidgets
    for i, section in enumerate(selected_sections):
        form_02.set_title(i, section)
    
    return form_02, subforms
#form_02, subforms = form_param_cmp_02(selected_sections, cols_file_01, cols_file_02)


def form_param_esklp_exist_dicts(esklp_dates):
    esklp_dates_dropdown = widgets.Dropdown( options=esklp_dates) #, value=None)
    
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Выберите дату сохраненного справочника ЕСКЛП:"), esklp_dates_dropdown], layout=form_item_layout) 
    form_items = [check_box]
    
    form_esklp_exist_dicts = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='auto')) #width='90%')) #
    # return form, fn_check_file_drop_douwn, fn_dict_file_drop_douwn, radio_btn_big_dict, radio_btn_prod_options, similarity_threshold_slider, max_entries_slider
    return form_esklp_exist_dicts, esklp_dates_dropdown 
