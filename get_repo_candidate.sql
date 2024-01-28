desc global_openrank;

desc events;

select repo_language, repo_topics.name
from events
where platform = 'GitHub'
  and repo_name = 'X-lab2017/open-digger'
order by created_at desc
limit 1;

select *
from global_openrank
where type = 'Repo'
order by created_at desc
limit 10;


-- 计算2022年每个GitHub项目的平均openrank，并选择平均openrank降序的第 offset ~ offset+limit_repo_num 的项目
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
                               AND created_at >= '2022-01-01 00:00:00'
                               AND created_at < '2023-01-01 00:00:00'
                             GROUP BY repo_id, repo_name
                             ORDER BY avg_openrank DESC)))

-- 获取 star 过仓库的开发者
   , actor_star_vscode AS (SELECT actor_id, repo_id
                           FROM events
                           WHERE platform = 'GitHub'
                             AND repo_id IN (SELECT repo_id FROM repo_id_5000)
                             AND type = 'WatchEvent'
                           GROUP BY actor_id, repo_id)

-- SELECT actor_id, repo_id
-- FROM actor_star_vscode;

SELECT count(distinct actor_id), count(distinct repo_id)
from actor_star_vscode;


-- 限制采样每个仓库的star者人数 10000,0,500
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
                               AND created_at >= '2022-01-01 00:00:00'
                               AND created_at < '2023-01-01 00:00:00'
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
SELECT  distinct repo_id
FROM actor_star_vscode
WHERE rn <= ${num_people_per_repo};

DROP TABLE IF EXISTS tmp_repo_id;
CREATE TEMPORARY TABLE tmp_repo_id
(
    repo_id Nullable(UInt64)
)
ENGINE = Memory;

