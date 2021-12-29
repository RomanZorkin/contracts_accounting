"""Модуль позволяет извлечеть комбинцию значений для заданной суммы
Сумма может быть задана, как интервалом, так и конкретным значением
"""

from itertools import combinations

energy = {    
    1: 0.436,
    2: 0.135,
    3: 0.0009,
    4: 0.019,
    5: 0.038,
    6: 0.615,
    7: 0.576,
    8: 0.081,
    9: 0.004,
    10: 0.016,
}

min_value = float(input(
    """Введите минимальное значение желаемого диапозона.\
    Если необходимо найти точное число, то введите его и как\
    минимальное и как максимальное значение 
    """
))
max_value = float(input('Введите максимальное значение '))
print(min_value, max_value)

# control_summ = 1.128
try:
    if control_summ:
        min_value = control_summ
        max_value = control_summ
except:
    pass
output_list = []

stuff = list(energy)
step = 0
for i in range(0, len(stuff)+1):
    for subset in combinations(stuff, i):
        step += 1
        new_summ = 0
        for punkt in subset:
            new_summ = new_summ + energy[punkt]
        if new_summ >= min_value and new_summ <= max_value:
            output_list.append([subset, new_summ])
print(output_list, step)