import duckdb
import pandas as pd
import networkx as nx
from sknetwork.data import from_edge_list
from sknetwork.ranking import PageRank
import time
loadJob="""CREATE TABLE aka_name (
                          id integer NOT NULL,
                          person_id integer NOT NULL,
                          name text NOT NULL,
                          imdb_index character varying(12),
                          name_pcode_cf character varying(5),
                          name_pcode_nf character varying(5),
                          surname_pcode character varying(5),
                          md5sum character varying(32),
                          primary key(id)
);

CREATE TABLE aka_title (
                           id integer NOT NULL,
                           movie_id integer NOT NULL,
                           title text NOT NULL,
                           imdb_index character varying(12),
                           kind_id integer NOT NULL,
                           production_year integer,
                           phonetic_code character varying(5),
                           episode_of_id integer,
                           season_nr integer,
                           episode_nr integer,
                           note text,
                           md5sum character varying(32),
                          primary key(id)
);

CREATE TABLE cast_info (
                           id integer NOT NULL,
                           person_id integer NOT NULL,
                           movie_id integer NOT NULL,
                           person_role_id integer,
                           note text,
                           nr_order integer,
                           role_id integer NOT NULL,
                          primary key(id)
);

CREATE TABLE char_name (
                           id integer NOT NULL,
                           name text NOT NULL,
                           imdb_index character varying(12),
                           imdb_id integer,
                           name_pcode_nf character varying(5),
                           surname_pcode character varying(5),
                           md5sum character varying(32),
                          primary key(id)
);

CREATE TABLE comp_cast_type (
                                id integer NOT NULL,
                                kind character varying(32) NOT NULL,
                          primary key(id)
);

CREATE TABLE company_name (
                              id integer NOT NULL,
                              name text NOT NULL,
                              country_code character varying(255),
                              imdb_id integer,
                              name_pcode_nf character varying(5),
                              name_pcode_sf character varying(5),
                              md5sum character varying(32),
                          primary key(id)
);

CREATE TABLE company_type (
                              id integer NOT NULL,
                              kind character varying(32) NOT NULL,
                          primary key(id)
);

CREATE TABLE complete_cast (
                               id integer NOT NULL,
                               movie_id integer,
                               subject_id integer NOT NULL,
                               status_id integer NOT NULL,
                          primary key(id)
);

CREATE TABLE info_type (
                           id integer NOT NULL,
                           info character varying(32) NOT NULL,
                          primary key(id)
);

CREATE TABLE keyword (
                         id integer NOT NULL,
                         keyword text NOT NULL,
                         phonetic_code character varying(5),
                          primary key(id)
);

CREATE TABLE kind_type (
                           id integer NOT NULL,
                           kind character varying(15) NOT NULL,
                          primary key(id)
);

CREATE TABLE link_type (
                           id integer NOT NULL,
                           link character varying(32) NOT NULL,
                          primary key(id)
);

CREATE TABLE movie_companies (
                                 id integer NOT NULL,
                                 movie_id integer NOT NULL,
                                 company_id integer NOT NULL,
                                 company_type_id integer NOT NULL,
                                 note text,
                          primary key(id)
);

CREATE TABLE movie_info (
                            id integer NOT NULL,
                            movie_id integer NOT NULL,
                            info_type_id integer NOT NULL,
                            info text NOT NULL,
                            note text,
                          primary key(id)
);

CREATE TABLE movie_info_idx (
                                id integer NOT NULL,
                                movie_id integer NOT NULL,
                                info_type_id integer NOT NULL,
                                info text NOT NULL,
                                note text,
                          primary key(id)
);

CREATE TABLE movie_keyword (
                               id integer NOT NULL,
                               movie_id integer NOT NULL,
                               keyword_id integer NOT NULL,
                          primary key(id)
);

CREATE TABLE movie_link (
                            id integer NOT NULL,
                            movie_id integer NOT NULL,
                            linked_movie_id integer NOT NULL,
                            link_type_id integer NOT NULL,
                          primary key(id)
);

CREATE TABLE name (
                      id integer NOT NULL,
                      name text NOT NULL,
                      imdb_index character varying(12),
                      imdb_id integer,
                      gender character varying(1),
                      name_pcode_cf character varying(5),
                      name_pcode_nf character varying(5),
                      surname_pcode character varying(5),
                      md5sum character varying(32),
                          primary key(id)
);

CREATE TABLE person_info (
                             id integer NOT NULL,
                             person_id integer NOT NULL,
                             info_type_id integer NOT NULL,
                             info text NOT NULL,
                             note text,
                          primary key(id)
);

CREATE TABLE role_type (
                           id integer NOT NULL,
                           role character varying(32) NOT NULL,
                          primary key(id)
);

CREATE TABLE title (
                       id integer NOT NULL,
                       title text NOT NULL,
                       imdb_index character varying(12),
                       kind_id integer NOT NULL,
                       production_year integer,
                       imdb_id integer,
                       phonetic_code character varying(5),
                       episode_of_id integer,
                       season_nr integer,
                       episode_nr integer,
                       series_years character varying(49),
                       md5sum character varying(32),
                          primary key(id)
);

copy aka_name from 'job-data/aka_name.csv' csv escape '\\' null '';
copy aka_title from 'job-data/aka_title.csv' csv escape '\\' null '';
copy cast_info from 'job-data/cast_info.csv' csv escape '\\' null '';
copy char_name from 'job-data/char_name.csv' csv escape '\\' null '';
copy company_name from 'job-data/company_name.csv' csv escape '\\' null '';
copy company_type from 'job-data/company_type.csv' csv escape '\\' null '';
copy comp_cast_type from 'job-data/comp_cast_type.csv' csv escape '\\' null '';
copy complete_cast from 'job-data/complete_cast.csv' csv escape '\\' null '';
copy info_type from 'job-data/info_type.csv' csv escape '\\' null '';
copy keyword from 'job-data/keyword.csv' csv escape '\\' null '';
copy kind_type from 'job-data/kind_type.csv' csv escape '\\' null '';
copy link_type from 'job-data/link_type.csv' csv escape '\\' null '';
copy movie_companies from 'job-data/movie_companies.csv' csv escape '\\' null '';
copy movie_info from 'job-data/movie_info.csv' csv escape '\\' null '';
copy movie_info_idx from 'job-data/movie_info_idx.csv' csv escape '\\' null '';
copy movie_keyword from 'job-data/movie_keyword.csv' csv escape '\\' null '';
copy movie_link from 'job-data/movie_link.csv' csv escape '\\' null '';
copy name from 'job-data/name.csv' csv escape '\\' null '';
copy person_info from 'job-data/person_info.csv' csv escape '\\' null '';
copy role_type from 'job-data/role_type.csv' csv escape '\\' null '';
copy title from 'job-data/title.csv' csv escape '\\' null '';
"""

con = duckdb.connect(database=':memory:')
con.execute(loadJob)
con.execute("SET threads TO 1;");
start = time.time()
startDuckDB = time.time()
df= con.execute("""
select c1.person_id,c2.person_id
from cast_info c1, cast_info c2, role_type r, movie_info mi, info_type it
where c1.movie_id=c2.movie_id
  and c1.role_id=r.id
  and c2.role_id=r.id
  and r.role='actor'
  and c1.nr_order>c2.nr_order
  and it.id=mi.info_type_id
  and it.info = 'genres'
  and mi.info='Drama'
  and mi.movie_id=c1.movie_id
""").fetchdf()
endDuckDB = time.time()

useNetworkX=False
pr_df=None
if useNetworkX:
    graphStart = time.time()
    G = nx.from_pandas_edgelist(df, "person_id", "person_id_2")
    graphEnd = time.time()
    pagerankStart = time.time()
    pr_df = pd.DataFrame(nx.pagerank(G, max_iter=100).items(), columns=["pid", "pr"])
    pagerankEnd = time.time()
else:
    graphStart = time.time()
    edge_list = list(df.itertuples(index=False))
    graph = from_edge_list(edge_list, directed=True)
    graphEnd = time.time()
    pagerankStart = time.time()
    pagerank = PageRank(n_iter=100,tol=0)
    names=graph.names
    pr=pagerank.fit_transform(graph.adjacency)
    pr_df=pd.DataFrame.from_dict({"pid":names,"pr":pr})
    pagerankEnd = time.time()


startDuckDB2 = time.time()
top10=con.execute("""
select n.person_id,min(n.name),min(pr.pr) as pagerank
from aka_name n, pr_df pr
where n.person_id=pr.pid
group by n.person_id
order by pagerank desc
limit 10 
""").fetchdf()
endDuckDB2 = time.time()
end = time.time()

print("DuckDB:",(endDuckDB - startDuckDB)+(endDuckDB2 - startDuckDB2))
print("Graph Construction:",graphEnd - graphStart)
print("PageRank:",pagerankEnd - pagerankStart)
print(top10)