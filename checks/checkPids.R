# with
# pids as (
#   select id, value, 'pid' as type from metadata where property = 'https://vocabs.acdh.oeaw.ac.at/schema#hasPid'
#   union
#   select id, value, 'cmdi' as type from metadata where property = 'https://vocabs.acdh.oeaw.ac.at/schema#hasMetadataPid'
#   union
#   select id, ids, 'id' as type from identifiers i where ids like 'https://hdl.handle.net/%' and not exists (select 1 from metadata where id = i.id and ids = value and property in ('https://vocabs.acdh.oeaw.ac.at/schema#hasPid', 'https://vocabs.acdh.oeaw.ac.at/schema#hasMetadataPid'))
# ),
# foo as (
#   select p.id, p.value as pid, p.type, coalesce(r9.target_id, r8.target_id, r7.target_id, r6.target_id, r5.target_id, r4.target_id, r3.target_id, r2.target_id, r1.target_id, p.id) as topcolid
#   from
#   pids p
#   left join relations r1 on p.id         = r1.id and r1.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r2 on r1.target_id = r2.id and r2.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r3 on r2.target_id = r3.id and r3.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r4 on r3.target_id = r4.id and r4.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r5 on r4.target_id = r5.id and r5.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r6 on r5.target_id = r6.id and r6.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r7 on r6.target_id = r7.id and r7.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r8 on r7.target_id = r8.id and r8.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r9 on r8.target_id = r9.id and r9.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
# )
# select foo.*, m1.value as topcolclass, min(m2.value) as topcollabel
# from
# foo
# left join metadata m1 on foo.topcolid = m1.id and m1.property = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
# join metadata m2 on foo.topcolid = m2.id and m2.property = 'https://vocabs.acdh.oeaw.ac.at/schema#hasTitle'
# group by 1, 2, 3, 4, 5;

library(dplyr)
arche = readr::read_csv('pids_arche.csv') %>%
  mutate(pid = sub('https://hdl.handle.net/', '', pid))
hdl = readr::read_csv2('pids.csv', col_types = 'ccccc') %>%
  mutate(
    finalurl = coalesce(finalurl, url)
  )

# ARCHE-only
arche %>% 
  anti_join(hdl) %>%
  mutate(prefix = sub('/.*$', '', pid)) %>%
  select(-topcolclass) %>%
  tidyr::nest(.by = c(topcolid, topcollabel, type, prefix))
arche %>% 
  anti_join(hdl) %>%
  select(-topcolclass) %>%
  arrange(topcollabel) %>%
  readr::write_csv2('arche_only.csv', na = '')

all = full_join(hdl %>% mutate(hdl = T), arche) %>%
  mutate(
    type = coalesce(type, 'non-arche'),
    domain = sub('^(https?://[^/]+).*', '\\1', url),
    path = if_else(domain == 'https://id.acdh.oeaw.ac.at', sub('^(https?://[^/]+)/([^/]+).*', '\\2', url), ''),
    status = case_when(
      status == '503' ~ 'Service unavailable (503)',
      status == '502' ~ 'Bad gateway (502)',
      status == '500' ~ 'Internal Server Error (500)',
      status == '400' ~ 'Bad request (400)',
      status == '401' ~ 'Access denied (401/403)',
      status == '403' ~ 'Access denied (401/403)',
      status == '404' ~ 'Redirect URL does not exist (404)',
      status == '200' & url == 'https://arche.acdh.oeaw.ac.at/brokenpid' ~ 'OK (broken)',
      status == '200' ~ 'OK',
      status %in% c('empty URL', 'no URL') ~ 'Empty URL/No URL',
      TRUE ~ status
    ),
    prefix = sub('/.*$', '', pid),
    user = purrr::map_chr(data, function(x){
      if (!is.na(x)) {
        return(unlist(jsonlite::fromJSON(x[1])$parsed_data)['adminId'])
      } else {
        return(NA_character_)
      }
    })
  )
all %>% group_by(type, hdl, prefix) %>% summarize(n = n()) %>% ungroup() %>% mutate(p = 100 * n / sum(n)) %>% arrange(desc(p))
all %>% group_by(type, hdl, status, prefix) %>% summarize(n = n()) %>% arrange(type, desc(n))
all %>% 
  filter(prefix == '21.11115') %>%
  filter(!grepl('OK|Empty', status) | is.na(status)) %>%
  group_by(type, domain, path, status) %>%
  summarize(n = n()) %>%
  arrange(type, status, domain == 'https://id.acdh.oeaw.ac.at', desc(n), domain, path) %>%
  print(n = 1000)
all %>% 
  filter(status == 'Redirect URL does not exist (404)') %>% 
  mutate(project = paste0(domain, '/', path)) %>%
  select(project, pid, url) %>%
  arrange(project, url) %>%
  readr::write_csv2('errors.csv', na = '')

all %>% 
  filter(finalurl != 'https://arche.acdh.oeaw.ac.at/brokenpid') %>%
  group_by(domain, path, finalurl) %>% 
  summarize(n = n()) %>% 
  filter(n > 1) %>%
  group_by(domain, path) %>% 
  summarize(sum = sum(n), n = n()) %>% 
  arrange(domain == 'https://id.acdh.oeaw.ac.at', desc(n)) %>%
  print(n = 1000)
