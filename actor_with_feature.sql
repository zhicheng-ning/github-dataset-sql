-- actor_with_feature：带初始特征的开发者 (325835 个，总开发者人数有 1297572，占比为 25%)


-- 2022 年度前 10000 的项目，每个项目采样至多 500 个开发者（actor.csv）
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
                             AND type = 'WatchEvent')

   , actor_set as (SELECT distinct actor_id
                   FROM actor_star_vscode
                   WHERE rn <= ${num_people_per_repo})
   , actor_latest as (select actor_id, repo_language, `repo_topics.name`
                      from events
                      where platform = 'GitHub'
                        and actor_id in actor_set
--                         and type in ('PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent'))
                        and type in ('PullRequestEvent'))
   , per_actor as (select actor_id
                        , repo_language
                        , `repo_topics.name`
                   from actor_latest)
   , RankedLanguages AS (SELECT actor_id,
                                repo_language,
                                COUNT(*)                                                                        as language_count, -- 计算每种语言的出现次数
                                ROW_NUMBER() OVER (PARTITION BY actor_id ORDER BY COUNT(*) DESC, repo_language) as rn              -- 对每个 actor_id 的语言进行排名
                         FROM per_actor
                         WHERE repo_language != '' -- 排除空的repo_language
                         GROUP BY actor_id, repo_language)
   , TopLanguages AS (SELECT actor_id, repo_language
                      FROM RankedLanguages
                      WHERE rn <= 3 -- 选择每个 actor_id 排名前三的语言
)
   , AggregatedTopics AS (SELECT actor_id,
                                 arrayJoin(`repo_topics.name`) as topic_name
                          FROM per_actor
                          WHERE `repo_topics.name` != '[]')
   , tmp_table as (SELECT tl.actor_id,
                          tl.repo_language,
                          groupArray(AggregatedTopics.topic_name) as topics -- 使用groupArray聚合函数
                   FROM TopLanguages tl
                            JOIN AggregatedTopics at ON tl.actor_id = AggregatedTopics.actor_id

                   GROUP BY tl.actor_id, tl.repo_language)
   , final_table as (SELECT actor_id,
                            groupArrayDistinct(repo_language)               as languages, -- 合并repo_language并去重
                            arrayDistinct(arrayFlatten(groupArray(topics))) as topics     -- 合并topics.name并去重
                     FROM tmp_table
                     GROUP BY actor_id)

select *
from final_table;

-- 39022409,"['Java','Go','JavaScript']","['documentation','students','life','sdk','create','chrome-extension','github','paper','research','openrank','data-analysis','hacktoberfest','rabbitmq','redis','golang','gorm','gin','halo-document','docs','works-with-codespaces','website']"

select actor_id, repo_language, `repo_topics.name`
from events
where platform = 'GitHub'
  and type in ('PullRequestEvent', 'PullRequestReviewEvent', 'PullRequestReviewCommentEvent')
  and actor_id = 39022409