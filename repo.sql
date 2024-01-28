-- 2022 年度前 10000 的项目，每个项目采样至多 500 个开发者（repo.csv, 9478 ）
-- 参数：limit = 10000; offset = 0; starYear=2022;endYear=2023;num_people_per_repo: 500
with repo_id_5000 as (select distinct repo_id
                      from (SELECT repo_id, repo_name, AVG(openrank) AS avg_openrank
                            FROM global_openrank
                            WHERE platform = 'GitHub'
                              AND toYear(created_at) >= ${starYear}
                              AND toYear(created_at) < ${endYear}
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
--                                AND created_at >= '2022-01-01 00:00:00'
--                                AND created_at < '2023-01-01 00:00:00'
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

SELECT count(distinct actor_id), count(distinct repo_id)
-- SELECT distinct actor_id
-- SELECT  distinct repo_id
FROM actor_star_vscode
WHERE rn <= ${num_people_per_repo};