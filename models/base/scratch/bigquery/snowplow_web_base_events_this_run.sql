{{
  config(
    tags=["this_run"]
  )
}}

{%- set lower_limit, upper_limit = snowplow_utils.return_limits_from_model(ref('snowplow_web_base_sessions_this_run'),
                                                                          'start_tstamp',
                                                                          'end_tstamp') %}

-- without downstream joins, it's safe to dedupe by picking the first event_id found.
select
  array_agg(e order by e.collector_tstamp limit 1)[offset(0)].*

from (

  select
    --a.contexts_com_snowplowanalytics_snowplow_web_page_1_0_0[safe_offset(0)].id as page_view_id,
    null as page_view_id,
    b.domain_userid, -- take domain_userid from manifest. This ensures only 1 domain_userid per session.
    a.* except(--contexts_com_snowplowanalytics_snowplow_web_page_1_0_0, 
    domain_userid,
    collector_tstamp,		
    derived_tstamp,			
    dvce_created_tstamp,			
    dvce_sent_tstamp,	
    etl_tstamp,		
    refr_dvce_tstamp,		
    true_tstamp),

    timestamp(collector_tstamp) collector_tstamp,		
    timestamp(derived_tstamp) derived_tstamp,			
    timestamp(dvce_created_tstamp) dvce_created_tstamp,			
    timestamp(dvce_sent_tstamp) dvce_sent_tstamp,	
    timestamp(etl_tstamp) etl_tstamp,		
    timestamp(refr_dvce_tstamp) refr_dvce_tstamp,		
    timestamp(true_tstamp) true_tstamp

  from {{ var('snowplow__events') }} as a
  inner join {{ ref('snowplow_web_base_sessions_this_run') }} as b
  on a.domain_sessionid = b.session_id

  where timestamp(a.collector_tstamp) <= {{ snowplow_utils.timestamp_add('day', var("snowplow__max_session_days", 3), 'timestamp(b.start_tstamp)') }}
  and timestamp(a.dvce_sent_tstamp) <= {{ snowplow_utils.timestamp_add('day', var("snowplow__days_late_allowed", 3), 'timestamp(a.dvce_created_tstamp)') }}
  and timestamp(a.collector_tstamp) >= {{ lower_limit }}
  and timestamp(a.collector_tstamp) <= {{ upper_limit }}
  {% if var('snowplow__derived_tstamp_partitioned', true) and target.type == 'bigquery' | as_bool() %}
    and timestamp(a.derived_tstamp) >= {{ snowplow_utils.timestamp_add('hour', -1, lower_limit) }}
    and timestamp(a.derived_tstamp) <= {{ upper_limit }}
  {% endif %}
  and {{ snowplow_utils.app_id_filter(var("snowplow__app_id",[])) }}

) e
group by e.event_id
