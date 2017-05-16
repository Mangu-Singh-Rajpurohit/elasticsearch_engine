import requests

class QueryConstructor(object):

    @classmethod
    def construct(cls, type, **kwargs):
        constructor_cls = TermQueryConstructor if type == "term" else (
                            MatchQueryConstructor if type == "match" else (
                                MultiMatchQueryConstructor if type == "multi_match" else (
                                    NestedQueryConstructor if type == "nested" else (
                                        MustQueryConstructor if type == "must" else (
                                            ShouldQueryConstructor if type == "should" else (
                                                BoolQueryConstructor if type == "bool" else None
                                            )
                                        )
                                    )
                                )
                            )
                          )
        assert constructor_cls, "Unsupported type received as argument: {}".format(type)
        return constructor_cls(**kwargs)

    def get_query(self, index, mapping, **kwargs):
        raise NotImplementedError("Implement this method in child class.")

class TermQueryConstructor(QueryConstructor):

    def get_query(self, index, mapping, **values):
        assert values, "Invalid values received as argument: {}".format(values)
        query_params            = values.get("query_params", values)
        additional_query_params = values.get("additional_query_params", {})

        body = {
            "term": {col: val for col, val in query_params.items()}
        }
        body["term"].update(additional_query_params)

        return body

class MatchQueryConstructor(QueryConstructor):

    def get_query(self, index, mapping, **values):
        assert values, "Invalid values received as argument: {}".format(values)
        query_params            = values.get("query_params", values)
        additional_query_params = values.get("additional_query_params", {})
        body = {
            "match": {col: val for col, val in query_params.items()}
        }
        body["match"].update(additional_query_params)

        return body

class MultiMatchQueryConstructor(QueryConstructor):

    def get_query(self, index, mapping, query, fields, q_type="cross_fields", **kwargs):
        assert fields, "Invalid fields received as argument: {}".format(fields)

        additional_query_params         = kwargs.get("additional_query_params", {})

        dict_multi_match                = {
                                            "query": query,
                                            "fields": fields,
                                            "type": q_type
                                          }
        dict_multi_match.update(additional_query_params)
        body                            = {
                                            "multi_match": dict_multi_match
                                          }

        return body

class NestedQueryConstructor(QueryConstructor):

    def get_query(self, index, mapping, path, **values):
        assert path, "Invalid path received: {}".format(path)
        assert values, "Invalid values received as argument: {}".format(values)

        query_params            = values.get("query_params", values)
        additional_query_params = values.get("additional_query_params", {})

        body    = {
            "nested": {
                "path": path,
                "query": {"{}.{}".format(path, field): val for field, val in query_params.items()}
            }
        }
        body["nested"].update(additional_query_params)
        return body

class ConditionalQueryMixin(object):

    def get_query(self, index, mapping, **kwargs):
        assert kwargs, "Invalid kwargs received: {}".format(kwargs)
        query_types = kwargs.get("query_types", None)
        assert query_types, "Invalid query_types received as argument: {}".format(query_types)

        body = []
        for query_type, query_type_config in query_types.items():
            constructor = QueryConstructor.construct(query_type)
            query       = constructor.get_query(query_type_config)
            body.append(query)

        return body

class MustQueryConstructor(ConditionalQueryMixin, QueryConstructor):

    def get_query(self, index, mapping, **kwargs):
        body = super(MustQueryConstructor, self).get_query(index, mapping, **kwargs)
        return { "must": body }

class ShouldQueryConstructor(ConditionalQueryMixin, QueryConstructor):

    def get_query(self, index, mapping, **kwargs):
        body = super(ShouldQueryConstructor, self).get_query(index, mapping, **kwargs)
        return { "should": body }

class FilterQueryConstructor(ConditionalQueryMixin, QueryConstructor):

    def get_query(self, index, mapping, **kwargs):
        body = super(FilterQueryConstructor, self).get_query(index, mapping, **kwargs)
        return { "filter": body }

class BoolQueryConstructor(ConditionalQueryMixin, QueryConstructor):

    def get_query(self, index, mapping, **kwargs):
        body = super(FilterQueryConstructor, self).get_query(index, mapping, **kwargs)
        return { "bool": body }

class ElasticSearchQueryEngine(object):

    def __init__(self, host, port):
        self._host = host
        self._port = port

    def get_url(self, url):
        return "{}:{}/{}".format(self._host, self._port, url)

    def send_request(self, url, method, headers={}, body={}):
        method_cb = getattr(requests, method.lower())
        return method_cb(url=self.get_url(url), headers=headers, json=body)

    def _query(self, type, index, mapping, **kwargs):
        constructor     = QueryConstructor.construct(type)
        body = {
            "query": constructor.get_query(index, mapping, **kwargs)
        }
        other_params    = kwargs.get("other_params", {})
        body.update(other_params)

        return body

    def term_query(self, index, mapping, **values):
        body = self._query("term", index, mapping, **values)
        return self.send_request("{}/{}".format(index, mapping), "get", body=body)

    def match(self, index, mapping, **values):
        body = self._query("match", index, mapping, **values)
        return self.send_request("{}/{}".format(index, mapping), "get", body=body)

    def multi_match(self, index, mapping, query, fields, q_type="cross_fields", **kwargs):
        body = self._query(
                            "multi_match", index, mapping, query=query,
                            fields=fields, q_type=q_type, **kwargs
                          )

        return self.send_request("{}/{}".format(index, mapping), "get", body=body)

    def nested(self, index, mapping, path, **values):
        body = self._query("nested", index, mapping, **values)
        return self.send_request("{}/{}".format(index, mapping), "get", body=body)

    def must(self, index, mapping, **kwargs):
        body = self._query("must", index, mapping, **kwargs)
        return self.send_request("{}/{}".format(index, mapping), "get", body=body)

    def should(self, index, mapping, **kwargs):
        body = self._query("should", index, mapping, **kwargs)
        return self.send_request("{}/{}".format(index, mapping), "get", body=body)

    def filter(self, index, mapping, **kwargs):
        body = self._query("filter", index, mapping, **kwargs)
        return self.send_request("{}/{}".format(index, mapping), "get", body=body)

    def bool(self, index, mapping, **kwargs):
        body = self._query("bool", index, mapping, **kwargs)
        return self.send_request("{}/{}".format(index, mapping), "get", body=body)

if __name__ == "__main__":

    test_all    = False
    index       = "hinglish11"
    mapping_url = "student/_search?pretty=true&search_type=dfs_query_then_fetch"
    es_engine   = ElasticSearchQueryEngine(host="http://localhost", port=9200)

    if test_all:
        resp        = es_engine.term_query(index, mapping_url, marks=44)
        resp        = es_engine.term_query(index, mapping_url, marks=44, other_params={"size": 100})

    if test_all:
        resp = es_engine.match(index, mapping_url, name="Mangu Singh Gevar Singh")

    if test_all:
        resp    = es_engine.multi_match(index, mapping_url,
                                        "Mangu Singh(Best school)", fields=["name^2", "school^1.2"])

    resp        = es_engine.nested(index, mapping_url)