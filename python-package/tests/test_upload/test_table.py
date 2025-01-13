from basedosdados.upload.table import Table
from basedosdados.exceptions import BaseDosDadosException
import pytest

DATASET_ID = 'pytest'
TABLE_ID = 'pytest'
csvPath = 'tests/test_upload/table/municipio.csv'
avroPath = 'tests/test_upload/table/municipio.avro'
parquetPath = 'tests/test_upload/table/municipio.parquet'
sqlPath = 'tests/test_upload/table/publish.sql'

table = Table(dataset_id=DATASET_ID, table_id=TABLE_ID)


def test_create():
  """
    Test create method
  """
  table.create(path=csvPath, if_table_exists='pass', if_dataset_exists='pass', if_storage_data_exists='pass')

  out = table.table_exists('staging')

  assert isinstance(out, bool)
  assert out is True

def test_create_table_exists():
  """
    Test create if table already exists
  """

  with pytest.raises(FileExistsError):
    table.create(path=csvPath, if_table_exists='raise', if_dataset_exists='pass', if_storage_data_exists='pass')

def test_create_dataset_and_storage_exists():
  """
    Test create if dataset and storage already exists
  """

  with pytest.raises(BaseDosDadosException):
    table.create(path=csvPath, if_table_exists='replace', if_dataset_exists='raise', if_storage_data_exists='raise')

def test_append(capsys):
  """
    Test append method
  """
  table.append(csvPath)
  _, out = capsys.readouterr()

  assert 'Table pytest was appended!' in out

def test_delete_all():
  """
    Test delete method with all modes
  """
  table.create(
    path=csvPath,
    if_table_exists="replace",
    if_storage_data_exists="replace",
    if_dataset_exists="replace",
    source_format="csv",
  )

  table.delete("all")

  assert not table.table_exists("staging")
  assert not table.table_exists("prod")

def test_delete_staging(capsys):
  """
    Test delete method with staging mode
  """
  table.create(
    path=csvPath,
    if_table_exists="replace",
    if_storage_data_exists="replace",
    if_dataset_exists="replace",
    source_format="csv",
  )

  table.delete("staging")
  _, out = capsys.readouterr()

  assert 'Table pytest_prod was deleted!' not in out
  assert not table.table_exists("staging")

def test_delete_prod():
  """
    Test delete method with prod mode
  """
  table.create(
    path=csvPath,
    if_table_exists="replace",
    if_storage_data_exists="replace",
    if_dataset_exists="replace",
    source_format="csv",
  )

  table.delete("prod")

  assert not table.table_exists("prod")
  assert table.table_exists("staging")
