# -*- coding: utf-8 -*-
"""semiarido.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1WFSb__v0hrec8qyvXM2PTmlqCsB4nmlH
"""

! pip install geobr -q
! pip install basedosdados -q
! pip install geobr -q
! pip install --upgrade shapely

import pandas as pd
import basedosdados as bd 
import numpy as np
import geobr
import shapely

x = geobr.read_semiarid()

x = x.rename({'code_muni': 'id_municipio', 'abbrev_state':'sigla_uf', 'geometry':'geometria'},
             axis = 'columns')

x = x.filter(items=['id_municipio',
      'sigla_uf',
      'geometria'])

x['id_municipio'] = x['id_municipio'].astype(float).astype(int).astype(str)
x.head()

x.to_csv('semiarido.csv', sep=',', index=False, header=True)