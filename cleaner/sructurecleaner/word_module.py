import pandas as pd
kbk_stamp = (
    '2020306999',
    '2020705999',
)
dfo = (
    'увтс',
    'иэу',
    'унто',
    'умо',
    'умто',
)


def value_in_list(kbk):
    for control_kbk in kbk_stamp:
        if control_kbk in kbk:
            return True
    return False

def kbk_clean(kbk):
    """превращает кбк формата 20203069990092019214(225) унто ГОЗ
     в формат (20203069990092019214225, унто, ГОЗ)"""
    kbk = kbk.replace('(', '').replace(')', '').split(' ')    
    if len(kbk) < 3:
        kbk.append('')
    kbk[1] = kbk[1].replace(',','')
    kbk[2] = kbk[2].replace(',','')
    if kbk[2].lower() in dfo:
        print('correctirovka', kbk[1])
        kbk[1] = kbk[2]
    print('posle correctirovki', kbk[1], kbk[2])
    return (kbk[0][:23], kbk[1].lower(), kbk[2])


def find_kbk(frame):
    """словарь с кбк:
    {значение кбк: индексы фрейма которые относятся к этому кбк  верхняя и нижняя границы }"""
    kbk_dict = {'header':[0,0]}
    curent_kbk = 'header'
    for row in frame.__iter__():
        if value_in_list(str(row)):
            #присваиваем верхнее значение диапазона
            if curent_kbk != 'header':
                kbk_dict[curent_kbk].append(frame[frame==row].index.astype(int)[0]-1)
            curent_kbk = kbk_clean(row)
            #присваимавем нижнее значение диапазона
            if curent_kbk != 'header':
                kbk_dict[curent_kbk] = [frame[frame==row].index.astype(int)[0]+1]
            continue
    kbk_dict[curent_kbk].append(len(frame))
    return  kbk_dict

def make_new_frame(frame, names_dict):
    """добавляет к существующему фрейму столюцы КБК и ДФО в соответстис координатами содержащимися в словаре kbk_dict"""
    kbk_dict = find_kbk(frame['subject'])
    frame_dict = []
    frame['kbk'] = ['']*len(frame)
    frame['detalisation'] = ['']*len(frame)
    num = 0
    for name, kbk in kbk_dict.items():
        print(kbk)
        #frame_dict[name]=[]
        for i in range(kbk[0], kbk[1]):
            print(name[0], len(name[0]))
            frame['kbk'].iloc[i] = name[0]
            print('to grame', name[1])
            frame['detalisation'].iloc[i] = name[1].replace(',','')

            if frame['ikz'].iloc[i] != '':
                frame['ikz'].iloc[i] = frame['ikz'].iloc[i][:26]
                frame_dict.append(frame.iloc[i])

    new_frame = pd.DataFrame(frame_dict)
    for word_name, sql_name in names_dict.items():
        new_frame.rename(columns={word_name: sql_name}, inplace=True)

    print(names_dict)

    return new_frame

    #def remoove_rows():

