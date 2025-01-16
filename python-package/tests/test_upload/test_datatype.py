from basedosdados.upload.datatypes import Datatype
from google.cloud.bigquery.external_config import (
    ExternalConfig,
    HivePartitioningOptions,
)

csvPath = 'tests/test_upload/table/municipio.csv'
avroPath = 'tests/test_upload/table/municipio.avro'
parquetPath = 'tests/test_upload/table/municipio.parquet'

def test_header_avro():
  """
    Test if header is returned for avro format
  """

  dt = Datatype(source_format="avro")
  cols = dt.header(avroPath, csv_delimiter=1)

  assert cols == [
      "ano",
      "id_municipio",
      "pib",
      "impostos_liquidos",
      "va",
      "va_agropecuaria",
      "va_industria",
      "va_servicos",
      "va_adespss",
  ]
  assert len(cols) != 0
  assert isinstance(cols, list)

def test_header_csv():
  """
    Test if header is returned for csv format
  """

  dt = Datatype(source_format="csv")
  cols = dt.header(csvPath, csv_delimiter='1')

  assert ['ano,id_municipio,pib,impostos_liquidos,va,va_agropecuaria,va_industria,va_servicos,va_adespss'] == cols
  assert len(cols) != 0
  assert isinstance(cols, list)

def test_header_parquet():
  """
    Test if header is returned for parquet format
  """

  dt = Datatype(source_format="parquet")
  cols = dt.header(parquetPath, csv_delimiter='1')

  assert cols == [
      "ano",
      "id_municipio",
      "pib",
      "impostos_liquidos",
      "va",
      "va_agropecuaria",
      "va_industria",
      "va_servicos",
      "va_adespss",
  ]
  assert len(cols) != 0
  assert isinstance(cols, list)

def test_partition_avro():
  """
    Test if partition config is returned for avro format
  """

  dt = Datatype(source_format="avro")
  partition = dt.partition()
  assert isinstance(partition, HivePartitioningOptions)


def test_partition_parquet():
  """
    Test if partition config is returned for parquet form
  """

  dt = Datatype(source_format="parquet")
  partition = dt.partition()
  assert isinstance(partition, HivePartitioningOptions)


def test_partition_csv():
  """
    Test if partition config is returned for csv format
  """
  
  dt = Datatype(source_format="csv")
  partition = dt.partition()
  assert isinstance(partition, HivePartitioningOptions)

def test_external_config_parquet():
  """
    Test if external config is returned for parquet format
  """

  dt = Datatype(source_format="parquet")
  assert isinstance(dt.external_config, ExternalConfig)


def test_external_config_avro():
  """
    Test if external config is returned for avro format
  """

  dt = Datatype(source_format="avro")
  assert isinstance(dt.external_config, ExternalConfig)

def test_external_config_csv():
  """
    Test if external config is returned for csv format
  """

  dt = Datatype(source_format="csv")
  assert isinstance(dt.external_config, ExternalConfig)