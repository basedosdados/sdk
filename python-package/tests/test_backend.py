from basedosdados.backend import Backend

backend = Backend()


def test_get_dataset():
  """
    Test dataset output without parameters
  """

  out = backend.get_datasets()
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["page_total"] > 100


def test_get_dataset_with_input():
  """
    Test dataset output with input
  """

  out = backend.get_datasets(dataset_name="br_me_caged")
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["page_size"] > 1


def test_get_tables():
  """
    Test tables output without parameters
  """

  out = backend.get_tables()
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["page_size"] >= 10
  assert out["page_total"] > 79


def test_get_tables_with_input():
  """
    Test tables output with input
  """

  out = backend.get_tables(table_name="br_me_caged")
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["page_size"] >= 10
  assert out["page_total"] == 0


def test_get_columns():
  """
    Test columns output without parameters
  """

  out = backend.get_columns()
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["page_size"] > 1
  assert out["page_total"] > 3000


def test_get_columns_with_input():
  """
    Test columns output with input
  """

  out = backend.get_columns(column_name="description")
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["page_size"] >= 10
  assert out["page_total"] > 0


def test_search(capsys):
  """
    Test search output without parameters
  """

  out = backend.search()
  _, res = capsys.readouterr()
  print('TEST SEARCH WITH INPUT', res)
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["page_size"] > 1


def test_search_with_input(capsys):
  """
    Test search output with input
  """

  out = backend.search(q="description")
  _, res = capsys.readouterr()
  print('TEST SEARCH WITH INPUT', res)
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["page_size"] >= 10


def test_get_dataset_config():
  """
    Test get_dataset_config output
  """

  out = backend.get_dataset_config(dataset_id="br_me_rais")
  assert isinstance(out, dict)
  assert len(out) > 0
  assert out != ""
  assert out != "None" or "null" or "NaN" or "inf" or "-inf" or "nan"


def test_get_table_config():
  """
    Test get_dataset_config output
  """

  out = backend.get_table_config(
      dataset_id="br_me_rais", table_id="microdados_estabelecimentos"
  )
  assert isinstance(out, dict)
  assert len(out) > 0
  assert out != ""
  assert out != "None" or "null" or "NaN" or "inf" or "-inf" or "nan"


def test_get_dataset_id_from_name():
  """
    Test get dataset id from name
  """

  out = backend._get_dataset_id_from_name("br_me_rais")
  assert isinstance(out, str)
  assert len(out) > 30
  assert out == "3e7c4d58-96ba-448e-b053-d385a829ef00"


def test_get_table_id_from_name():
  """
    Test get table id from name
  """

  out = backend._get_table_id_from_name(
      "br_me_rais", "microdados_estabelecimentos"
  )
  assert isinstance(out, str)
  assert len(out) > 35
  assert out == "86b69f96-0bfe-45da-833b-6edc9a0af213"


def test_execute_query():
  """
    Test execute query
  """

  example = {"items": [], "page": 1, "page_size": 10, "page_total": 0}
  query = """
       query {
        allDataset(first: 5, offset: 10) {
          edges {
            node {
              slug
              name
              description
              organizations {
                edges {
                  node {
                    name
                  }
                }
              }
              tags {
                edges {
                  node {
                    name
                  }
                }
              }
              themes {
                edges {
                  node {
                    name
                  }
                }
              }
              createdAt
              updatedAt
            }
          }
          totalCount
        }
      }
    """
  out = backend._execute_query(query, example)
  assert isinstance(out, dict)
  assert len(out) != 0
  assert out["allDataset"]["page_total"] > 100


def test_simplify_response():
  """
    Test simplify query
  """

  response = {"items": [], "page": 1, "page_size": 10, "page_total": 0}
  out = backend._simplify_response(response)
  assert isinstance(out, dict)
  assert len(out) != 0
