{{
  config(
    tags=["this_run"],
    post_hook=[
    "{{ snowplow_utils.quarantine_sessions('snowplow_web', var('snowplow__max_session_days')) }}"
    ],
    sql_header=snowplow_utils.set_query_tag(var('snowplow__query_tag', 'snowplow_dbt'))
  )
}}

{%- set lower_limit,
        upper_limit,
        session_start_limit = snowplow_utils.return_base_new_event_limits(ref('snowplow_web_base_new_event_limits')) %}

select
  s.session_id,
  s.domain_userid,
  timestamp(s.start_tstamp) start_tstamp,
  -- end_tstamp used in next step to limit events. When backfilling, set end_tstamp to upper_limit if end_tstamp > upper_limit.
  -- This ensures we don't accidentally process events after upper_limit
  case when timestamp(s.end_tstamp) > {{ upper_limit }} then {{ upper_limit }} else timestamp(s.end_tstamp) end as end_tstamp

from {{ ref('snowplow_web_base_sessions_lifecycle_manifest')}} s

where
-- General window of start_tstamps to limit table scans. Logic complicated by backfills.
-- To be within the run, session start_tstamp must be >= lower_limit - max_session_days as we limit end_tstamp in manifest to start_tstamp + max_session_days
timestamp(s.start_tstamp) >= {{ session_start_limit }}
and timestamp(s.start_tstamp) <= {{ upper_limit }}
-- Select sessions within window that either; start or finish between lower & upper limit, start and finish outside of lower and upper limits
and not (timestamp(s.start_tstamp) > {{ upper_limit }} or timestamp(s.end_tstamp) < {{ lower_limit }})
