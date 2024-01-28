-- repo_with_feature：带初始特征的 repo (共 9478 ，其中 9377 带 topic or repo_language)
-- 注意 repo_topics.name 的信息只存在于（PullRequestEvent，PullRequestReviewCommentEvent，PullRequestReviewEvent）这类事件信息中

with repo_id_5000 as (select distinct repo_id
                      from (SELECT repo_id, repo_name, AVG(openrank) AS avg_openrank
                            FROM global_openrank
                            WHERE platform = 'GitHub'
                              AND created_at >= '2022-01-01 00:00:00'
                              AND created_at < '2023-01-01 00:00:00'
                            GROUP BY repo_id, repo_name
                            ORDER BY avg_openrank DESC
                            LIMIT $(limit_repo_num) OFFSET $(offset)
                            UNION
                            Distinct
                            (SELECT repo_id, repo_name, avg(openrank) AS avg_openrank
                             FROM global_openrank
                             where platform = 'GitHub'
                               and (
                                 org_login in ('polardb', 'X-lab2017')
                                     or repo_name in ('ClickHouse/ClickHouse', 'apache/doris', 'ingcap/tidb')
                                 )
                              AND toYear(created_at) >= ${starYear}
                              AND toYear(created_at) < ${endYear}
                             GROUP BY repo_id, repo_name
                             ORDER BY avg_openrank DESC)))
-- 获取 star 过仓库的开发者
   , actor_star_vscode AS (SELECT actor_id,
                                  repo_id,
                                  ROW_NUMBER() OVER (PARTITION BY repo_id ORDER BY created_at DESC) as rn
                           FROM events
                           WHERE platform = 'GitHub'
                             AND repo_id IN (SELECT repo_id FROM repo_id_5000)
                             AND type = 'WatchEvent')

-- SELECT count(distinct actor_id), count(distinct repo_id)
-- SELECT distinct actor_id
,repo_set as (SELECT distinct repo_id
                  FROM actor_star_vscode
                  WHERE rn <= ${num_people_per_repo})
,
    repo_latest AS (
    SELECT repo_id,
           repo_language,
           `repo_topics.name`,
           repo_description,
           created_at,
           ROW_NUMBER() OVER(PARTITION BY repo_id ORDER BY created_at DESC) as rn
    FROM events
    WHERE platform = 'GitHub'
      AND repo_id IN (SELECT repo_id FROM repo_set)  -- 确保repo_set已经在前面定义
      AND type in ('PullRequestReviewEvent','PullRequestReviewCommentEvent','PullRequestEvent')
--       AND (`repo_topics.name` != '[]' or repo_language != '')
)

SELECT repo_id, repo_language, `repo_topics.name`, repo_description
-- select count(repo_id)
FROM repo_latest
WHERE rn = 1;
;


select distinct type
from events where platform = 'GitHub' and `repo_topics.name` != '[]';