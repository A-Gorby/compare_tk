import ipywidgets as widgets
from ipywidgets import Layout, Box, Label
def form_param(fn_list):
    fn_check_file_drop_douwn = widgets.Dropdown( options=fn_list, value=None) #fn_list[0] if len(fn_list) > 0 else None, disabled=False)
    fn_desc_file_drop_douwn = widgets.Dropdown( options=fn_list, value=None)
    # sheet_name_drop_douwn = widgets.Dropdown( options= [None], value= None, disabled=False)
    # col_name_drop_douwn = widgets.Dropdown( options= [None], value= None, disabled=False)
    # fn_dict_file_drop_douwn = widgets.Dropdown( options= [None] + fn_list, value= None, disabled=False, )
    # radio_btn_big_dict = widgets.RadioButtons(options=['Р”Р°', 'РќРµС‚'], value= 'Р”Р°', disabled=False) # description='Check me',    , indent=False
    # radio_btn_prod_options = widgets.RadioButtons(options=['Р”Р°', 'РќРµС‚'], value= 'РќРµС‚', disabled=False if radio_btn_big_dict.value=='Р”Р°' else True )
    # similarity_threshold_slider = widgets.IntSlider(min=1,max=100, value=90)
    # max_entries_slider = widgets.IntSlider(min=1,max=5, value=4)
    # max_out_values_slider = widgets.IntSlider(min=1,max=10, value=4)

    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Выберите Excel-файл со сводными данными 'Услуги', 'ЛП', 'РМ':"), fn_check_file_drop_douwn], layout=form_item_layout) 
    desc_box = Box([Label(value='Выберите Excel-файл с описанием моделей:'), fn_desc_file_drop_douwn], layout=form_item_layout) 
    # sheet_box = Box([Label(value='Выберите лист Excel-файла:'), sheet_name_drop_douwn], layout=form_item_layout) 
    # column_box = Box([Label(value='Р—Р°РіРѕР»РѕРІРѕРє РєРѕР»РѕРЅРєРё:'), col_name_drop_douwn], layout=form_item_layout) 
    # big_dict_box = Box([Label(value='РСЃРїРѕР»СЊР·РѕРІР°С‚СЊ Р±РѕР»СЊС€РёРµ СЃРїСЂР°РІРѕС‡РЅРёРєРё:'), radio_btn_big_dict], layout=form_item_layout) 
    # prod_options_box = Box([Label(value='РСЃРєР°С‚СЊ РІ Р’Р°СЂРёР°РЅС‚Р°С… РёСЃРїРѕР»РЅРµРЅРёСЏ (+10 РјРёРЅ):'), radio_btn_prod_options], layout=form_item_layout) 
    # similarity_threshold_box = Box([Label(value='РњРёРЅРёРјР°Р»СЊРЅС‹Р№ % СЃС…РѕРґСЃС‚РІР° РїРѕР·РёС†РёР№:'), similarity_threshold_slider], layout=form_item_layout) 
    # max_entries_box = Box([Label(value='РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РєРѕР»-РІРѕ РЅР°Р№РґРµРЅРЅС‹С… РїРѕР·РёС†РёР№:'), max_entries_slider], layout=form_item_layout) 
    # max_out_values_box = Box([Label(value='РњР°РєСЃРёРјР°Р»СЊРЅРѕРµ РєРѕР»-РІРѕ РІС‹РІРѕРґРёРјС‹С… РїРѕР·РёС†РёР№:'), max_out_values_slider], layout=form_item_layout) 
    
    # form_items = [check_box, dict_box, big_dict_box, prod_options_box, similarity_threshold_box, max_entries_box]
    form_items = [check_box, desc_box] #, column_box, similarity_threshold_box, max_entries_box, max_out_values_box]
    
    form = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form, fn_check_file_drop_douwn, fn_dict_file_drop_douwn, radio_btn_big_dict, radio_btn_prod_options, similarity_threshold_slider, max_entries_slider
    # return form, fn_check_file_drop_douwn, sheet_name_drop_douwn, col_name_drop_douwn, similarity_threshold_slider, max_entries_slider, max_out_values_slider
    return form, fn_check_file_drop_douwn, fn_desc_file_drop_douwn

def form_param_esklp_exist_dicts(esklp_dates):
    esklp_dates_dropdown = widgets.Dropdown( options=esklp_dates) #, value=None)
    
    form_item_layout = Layout(display='flex', flex_flow='row', justify_content='space-between')
    check_box = Box([Label(value="Выберите дату сохраненного справочника ЕСКЛП:"), esklp_dates_dropdown], layout=form_item_layout) 
    form_items = [check_box]
    
    form_esklp_exist_dicts = Box(form_items, layout=Layout(display='flex', flex_flow= 'column', border='solid 2px', align_items='stretch', width='50%')) #width='auto'))
    # return form, fn_check_file_drop_douwn, fn_dict_file_drop_douwn, radio_btn_big_dict, radio_btn_prod_options, similarity_threshold_slider, max_entries_slider
    return form_esklp_exist_dicts, esklp_dates_dropdown 
