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

-- 从 star 过仓库的开发者中找到 有过 fork 仓库行为的人
   , actor_id_who_star as (SELECT distinct actor_id
                           FROM actor_star_vscode
                           WHERE rn <= ${num_people_per_repo})
   , repo_id_who_be_stared as (SELECT distinct repo_id
                               FROM actor_star_vscode
                               WHERE rn <= ${num_people_per_repo})
   , actor_issue_repo as (select actor_id, repo_id, concat(toString(repo_id), '#', toString(issue_number)) as issue_id
                       from events
                       where platform = 'GitHub'
                         and actor_id in (actor_id_who_star)
                         and repo_id in (repo_id_who_be_stared)
                         and toYear(created_at) >= ${starYear}
                         and toYear(created_at) < ${endYear}
                         and type in ('IssuesEvent', 'IssuesReactionEvent', 'IssueCommentEvent'))
SELECT actor_id, issue_id, COUNT(*) as weight
FROM actor_issue_repo
GROUP BY actor_id, repo_id, issue_id;


