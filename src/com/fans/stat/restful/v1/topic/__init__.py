__author__ = 'ZENGFANGUI'

# !/usr/bin/python
from tornroutes import route
from config import *
import pyes
from pyes.aggs import DateHistogramAgg
import datetime, calendar
import tornado
import  json
from src.com.fans.common.util import JSONHandler
from src.com.fans.common.redis import getCachRequest, cachRequest
from src.com.fans.stat.restful.v1.common import es_connection, redispool

class CardinalityAgg(pyes.aggs.BucketAgg):

    _internal_name = "cardinality"

    def __init__(self, name, field=None, script=None, params=None, **kwargs):
        super(CardinalityAgg, self).__init__(name, **kwargs)
        self.field = field
        self.script = script
        self.params = params

    def _serialize(self):
        data = {}
        if self.field:
            data['field'] = self.field
        elif self.script:
            data['script'] = self.script
            if self.params:
                data['params'] = self.params
        return data

@route("/v1/count/topic/list")
class TopicReadCount(JSONHandler.JSONHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self):
        topicIds = self.get_argument("topicids", None)
        time_filter = pyes.BoolFilter(must=[pyes.RangeFilter(qrange=pyes.ESRange('createTime',from_value="1299964735115"))])
        #filter = pyes.FilteredQuery(pyes.QueryStringQuery(query=topicIds,default_field="refId",analyze_wildcard=True),)
        filter = pyes.QueryStringQuery(query=topicIds,default_field="refId",analyze_wildcard=True)
        term_agg=pyes.aggs.TermsAgg(name='topic_read_count',field='refId',sub_aggs=[CardinalityAgg('user_uniq_count',field='userip')])
        print term_agg
        query_string=filter.search()
        query_string.agg.add(term_agg)
        print query_string
        result_map={}
        for key in topicIds.split(","):
            result_map[key]=0
        #for key in
        result =   es_connection.search(query_string,indices="access",doc_types="access-comment-access-list",search_type="count")

        if result.aggs['topic_read_count'].has_key('buckets'):
          for item in result.aggs['topic_read_count']['buckets']:
            print(item)
            result_map[item["key"]]=item["user_uniq_count"]["value"]
        else:
          return None
        print result_map
        self.finish(result_map)

