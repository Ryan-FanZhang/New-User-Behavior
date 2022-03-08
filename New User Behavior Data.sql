SELECT t.dt,
       t.groupid,
       groupname,
       count(DISTINCT t.uid) NEW ,
                             count(DISTINCT t3.uid) chat,
                             sum(t3.pv) chat_pv,
                             count(DISTINCT t8.uid) READ ,
                                                    count(DISTINCT t9.uid) tick,
                                                    sum(t9.tick_pv) tick_pv,
                                                    round(sum(t3.pv) /count(DISTINCT t3.uid),2) avg_chat_num,
                                                    round(sum(t6.duration)/count(DISTINCT t6.uid)/60,2)avg_group_time,
                                                    round(count(DISTINCT t3.uid)/count(DISTINCT t.uid),2) chat_rate
FROM (--新注册并加入群组
 --加入群组

      SELECT a.dt,
             groupid,
             b.uid
      FROM
        (SELECT substr(CAST(ds AS string),1,8) dt,
                groupid,
                uid
         FROM beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
         WHERE substr(CAST(ds AS string),1,8) >=cast(substring(from_unixtime(unix_timestamp(now() - interval 15 DAY), 'yyyyMMdd'),1,8) AS string)
           AND operid IN ('1103000120601')
           AND groupid != '100141'
           AND groupid != '0'
         GROUP BY substr(CAST(ds AS string),1,8),
                  groupid,
                  uid) a
      JOIN
        (SELECT regtime dt,
                uid
         FROM beacon_olap.t_updw_dwd_kk_user_info_h
         WHERE substr(CAST(ds AS string),1,8) >=cast(substring(from_unixtime(unix_timestamp(now() - interval 1 DAY), 'yyyyMMdd'),1,8) AS string)
           AND usertype='1'
           AND  channel_app='CHNoknok'
           AND substr(CAST(ds AS string),9,2) = '23'
         GROUP BY regtime,
                  uid) b ON a.dt =b.dt
      AND a.uid = b.uid
      GROUP BY a.dt,
               groupid,
               b.uid) t
LEFT JOIN
  (SELECT substr(CAST(ds AS string),1,8) dt ,
          uid,
          groupid,
          count(DISTINCT messageid) pv
   FROM beacon_olap.ieg_gameplus_gameplus_noknok_group_im_msg_kk
   WHERE substr(CAST(ds AS string),1,8) >=cast(substring(from_unixtime(unix_timestamp(now() - interval 15 DAY), 'yyyyMMdd'),1,8) AS string)
     AND user_type=1
     AND  channel_app='CHNoknok'
     AND opertype IN ('1',
                      '2',
                      '3')
   GROUP BY substr(CAST(ds AS string),1,8),
            uid,
            groupid) t3 ON t.uid=t3.uid
AND t.dt=t3.dt
AND t.groupid=t3.groupid
LEFT JOIN
  (select 
			dt,
          groupid,
          uid,
		  sum(duration) duration
from 
(
SELECT substr(CAST(ds AS string),1,8) dt,
          groupid,
		duration,
          uid,
		  ftime,
           eventtime  
   FROM beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
   WHERE substr(CAST(ds AS string),1,8) >=cast(substring(from_unixtime(unix_timestamp(now() - interval 15 DAY), 'yyyyMMdd'),1,8) AS string)
     AND substr(operid,1,2)='11'
     AND substr(operid,-4,2)='03'
     AND operid<>'1101000110301'
	 and substr(cast(ds as string),9,2) = '23'
     AND uid<>'0'
     AND length(groupid)>2
     AND duration between 0 and 3600
   GROUP BY substr(CAST(ds AS string),1,8) ,
            uid ,
            groupid,
			duration,
			ftime,
           eventtime 
)a
group by 
	dt,
 groupid,
 uid) t6 ON t6.uid=t.uid
AND t6.dt=t.dt
AND t.groupid=t3.groupid
LEFT JOIN
  (SELECT groupid,
          groupname
   FROM beacon_olap.t_updw_ods_kk_channel_info_base_h
   WHERE substr(CAST(ds AS string),1,8) =cast(substring(from_unixtime(unix_timestamp(now() - interval 1 DAY), 'yyyyMMdd'),1,8) AS string)
     AND substr(CAST(ds AS string),9,2)='23'
   GROUP BY groupid,
            groupname) t5 ON t.groupid=t5.groupid
LEFT JOIN
  (SELECT substr(CAST(ds AS string),1,8) dt ,
          groupid,
          uid
   FROM beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
   WHERE substr(CAST(ds AS string),1,8) >=cast(substring(from_unixtime(unix_timestamp(now() - interval 7 DAY), 'yyyyMMdd'),1,8) AS string)
     AND operid IN ('1714000110201',
                    '1709000110601',
                    '1112000110101',
                    '1113000110101')
   GROUP BY substr(CAST(ds AS string),1,8),
            groupid,
            uid) t8 ON t.dt=t8.dt
AND t.uid=t8.uid
AND t.groupid=t8.groupid
LEFT JOIN --群组话题参与用户数

  (SELECT substr(CAST(ds AS string),1,8) dt,
          groupid,
          uid,
          count(DISTINCT messageid) tick_pv
   FROM beacon_olap.ieg_gameplus_gameplus_user_action_report_kk
   WHERE substr(CAST(ds AS string),1,8) >=cast(substring(from_unixtime(unix_timestamp(now() - interval 15 DAY), 'yyyyMMdd'),1,8) AS string)
     AND operid='1117008020601'
     AND uid<>'0'
   GROUP BY substr(CAST(ds AS string),1,8) ,
            groupid,
            uid) t9 ON t.dt=t9.dt
AND t.uid=t9.uid
AND t.groupid=t9.groupid --where t.groupid<>'100141'
GROUP BY t.dt,
         t.groupid,
         groupname HAVING count(DISTINCT t.uid)>5
ORDER BY t.dt DESC ,
         NEW DESC