__author__ = 'ZENGFANGUI'
#!/usr/bin/python
from tornroutes import route
from config import *
import  pyes
from pyes.aggs import DateHistogramAgg
import  datetime,calendar
import  tornado
from src.com.fans.common.util import  ThreadPool


from src.com.fans.common.util import JSONHandler
from src.com.fans.common.redis import  getCachRequest,cachRequest
from src.com.fans.stat.restful.v1.common import es_connection,redispool


circle_stat_conn=es_connection
pool = redispool


def query_circle_specify_date_by_interval(cid,category,beginTime,endTime,interval):
    b_q=pyes.BoolQuery()
    q=pyes.MatchAllQuery()
    q=pyes.TermQuery("circleId",cid)
    range_query={}
    range_query['gte']=beginTime.strftime("%Y-%m-%d")
    range_query['lte']=endTime.strftime("%Y-%m-%d")
    range_query['format']="yyyy-MM-dd"
    es_range=pyes.ESRange("createTime",range_query['gte'],range_query['lte'],include_lower=True,include_upper=True,format="yyy-MM-dd")

    print range_query
    timerange=pyes.RangeQuery(es_range)

    b_q.add_must(q)
    b_q.add_must(timerange)
    q=b_q.search()


    #count_agg = pyes.aggs.ValueCountAgg()

    date_time_agg = DateHistogramAgg(name="circle-stat",field="createTime",interval=interval)

    q.agg.add(date_time_agg)
    print q
    result =   circle_stat_conn.search(query=q,indices=category,doc_types=category,search_type="count")
    if result.aggs['circle-stat'].has_key('buckets'):
        return result.aggs['circle-stat']['buckets']
    else:
        return None

@route("/v1/circle/(\d+)/month/(\S+)/category/(\S+)/detail")
class CircleStasticDetail(JSONHandler.JSONHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self,circleId,month,category):
        print circleId,month,category
        print self.request.uri
        data=getCachRequest(pool,self.request.uri)
        if data is not None:
            self.finish(data)
            return
        last_day=datetime.date.today()
        if month!='now':
            try:
                last_day=datetime.datetime.strptime(month,"%Y-%m")
                if last_day is None:
                    last_day=datetime.date.today()
                else:
                    last_month_day= calendar.monthrange(last_day.date().year, last_day.date().month)[1]
                    last_day=datetime.date(last_day.date().year,last_day.date().month,last_month_day)
            except Exception,e:
                print month
                print e
                last_day=datetime.date.today()

        first_day=datetime.date(last_day.year,last_day.month,1)
        if category=='up_action':
            category='comment_action,topic_action'
        print first_day
        result_map={}
        total=0
        max=0
        size = (last_day-first_day).days+1
        for i in range(0,size):
            tmp_day=first_day+datetime.timedelta(days=i)
            result_map[tmp_day.strftime("%Y-%m-%d")]=0
        print result_map
        query_result= yield ThreadPool.run_async(query_circle_specify_date_by_interval,circleId,category,first_day,last_day,'day')
        for i in query_result:
            total+=i['doc_count']
            print i
            if i['doc_count']>max:
                max=i['doc_count']

            result_map[i['key_as_string'].split('T')[0]]=i['doc_count']

        average=total/size

        dic=cachRequest(pool,self.request.uri,request_expire_time,total=total,max=max,average=average,data=result_map)
        self.finish(dic)


@route("/v1/circle/(\d+)/latest")
class CircleStatLatest(JSONHandler.JSONHandler):
    @tornado.web.asynchronous
    @tornado.gen.engine
    def get(self,circleid):
        print circleid

        data=getCachRequest(pool,self.request.uri)
        if data is not None:
            self.finish(data)
            return
        query_parameters={}
        b_q=pyes.BoolQuery()
        q=pyes.MatchAllQuery()
        q=pyes.TermQuery("circleId",circleid)
        stat_metri=['circle_user_sign_record','up_action','comment_info','post_info','circle_follow']
        begin_time=datetime.datetime.now()
        result_map={}
        for i in stat_metri:
            category=i
            if i=='up_action':
                category='comment_action,topic_action'
            adddata= yield ThreadPool.run_async(query_circle_specify_date_by_interval,circleid,category,begin_time,begin_time,'day')
            print adddata
            if None is adddata or len(adddata)==0:
                result_map[i]=0
            else:
                result_map[i]=adddata[-1:][0]['doc_count']
        cachRequest(pool,self.request.uri,request_expire_time,result_map)
        self.finish(result_map)
