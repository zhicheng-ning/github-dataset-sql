-- 2022 年度前 10000 的项目，每个项目采样 star 过项目的至多  500 个开发者（actor_star_repo.csv）
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
                             AND type = 'WatchEvent'
                             AND toYear(created_at) >= ${starYear}
                             AND toYear(created_at) < ${endYear})

-- SELECT count(distinct actor_id), count(distinct repo_id)
-- SELECT distinct actor_id
-- SELECT  distinct repo_id
-- 从 star 过仓库的开发者中找到 有过 fork 仓库行为的人
   , actor_id_who_star as (SELECT distinct actor_id
                           FROM actor_star_vscode
                           WHERE rn <= ${num_people_per_repo})
   , repo_id_who_be_stared as (SELECT distinct repo_id
                               FROM actor_star_vscode
                               WHERE rn <= ${num_people_per_repo})

SELECT distinct actor_id, repo_id
from events
WHERE platform = 'GitHub'
  and actor_id in (actor_id_who_star)
  and repo_id in (repo_id_who_be_stared)
  AND type = 'ForkEvent'
  AND toYear(created_at) >= ${starYear}
  AND toYear(created_at) < ${endYear}


-- 每个开发者分别关注了多少个仓库
-- SELECT actor_id, COUNT(DISTINCT repo_id) AS num_starred_repos
-- FROM actor_star_vscode  -- 替换为你的表名
-- GROUP BY actor_id order by num_starred_repos desc
;