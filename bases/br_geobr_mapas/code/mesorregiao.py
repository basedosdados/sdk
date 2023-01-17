# -*- coding: utf-8 -*-
"""mesorregiao.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1feAA1KFrEZ-9zTluWaERLSxt8tZt-Xbh
"""

pip install geobr

import geobr
import pandas as pd
import numpy as np
from posixpath import sep

mesorregiao = geobr.read_meso_region()

mesorregiao['code_state'] = mesorregiao['code_state'].astype(np.int64)
mesorregiao['code_meso'] = mesorregiao['code_meso'].astype(np.int64)

#micro=micro.rename({'code_state':'id_uf', 'abbrev_state':'sigla_uf', 'code_micro':'id_microrregiao','geometry':'geometria', 'name_micro': 'nome_microrregiao'},axis='columns')
mesorregiao=mesorregiao.rename({'code_state':'id_uf', 'abbrev_state':'sigla_uf', 'code_meso':'id_mesorregiao','geometry':'geometria'}, axis='columns')

mesorregiao=mesorregiao.filter(items=['id_uf', 'sigla_uf', 'id_mesorregiao', 'geometria'])

mesorregiao.to_csv('mesorregiao.csv', sep=',', index=False, header=True)