import re
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
