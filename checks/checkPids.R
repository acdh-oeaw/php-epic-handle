# with
# foo as (
#   select p.id, p.value as pid, coalesce(r9.target_id, r8.target_id, r7.target_id, r6.target_id, r5.target_id, r4.target_id, r3.target_id, r2.target_id, r1.target_id, p.id) as topcolid
#   from
#   metadata p
#   left join relations r1 on p.id         = r1.id and r1.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r2 on r1.target_id = r2.id and r2.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r3 on r2.target_id = r3.id and r3.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r4 on r3.target_id = r4.id and r4.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r5 on r4.target_id = r5.id and r5.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r6 on r5.target_id = r6.id and r6.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r7 on r6.target_id = r7.id and r7.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r8 on r7.target_id = r8.id and r8.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   left join relations r9 on r8.target_id = r9.id and r9.property = 'https://vocabs.acdh.oeaw.ac.at/schema#isPartOf'
#   where p.property = 'https://vocabs.acdh.oeaw.ac.at/schema#hasPid'
# )
# select foo.*, m1.value as topcolclass, min(m2.value) as topcollabel
# from
# foo
# left join metadata m1 on foo.topcolid = m1.id and m1.property = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
# join metadata m2 on foo.topcolid = m2.id and m2.property = 'https://vocabs.acdh.oeaw.ac.at/schema#hasTitle'
# group by 1, 2, 3, 4;

library(dplyr)
arche = readr::read_csv2('pids_arche.csv') %>%
  mutate(pid = sub('https://hdl.handle.net/', '', pid))
hdl = readr::read_csv2('pids.csv', col_types = 'ccccc') %>%
  mutate(
    finalurl = coalesce(finalurl, url)
  )

# ARCHE-only
arche %>% 
  anti_join(hdl) %>%
  select(-topcolclass) %>%
  tidyr::nest(.by = c(topcolid, topcollabel))
arche %>% 
  anti_join(hdl) %>%
  select(-topcolclass) %>%
  arrange(topcollabel) %>%
  readr::write_csv2('arche_only.csv', na = '')

all = full_join(hdl %>% mutate(hdl = T), arche %>% mutate(arche = T)) %>% 
  mutate(
    domain = sub('^(https?://[^/]+).*', '\\1', url),
    path = if_else(domain == 'https://id.acdh.oeaw.ac.at', sub('^(https?://[^/]+)/([^/]+).*', '\\2', url), ''),
    status = case_when(
      status == '400' ~ 'Bad request (400)',
      status == '401' ~ 'Access denied (401/403)',
      status == '403' ~ 'Access denied (401/403)',
      status == '404' ~ 'Redirect URL does not exist (404)',
      status == '200' & url == 'https://arche.acdh.oeaw.ac.at/brokenpid' ~ 'OK (broken)',
      status == '200' ~ 'OK',
      status %in% c('empty URL', 'no URL') ~ 'Empty URL/No URL',
      TRUE ~ status
    ),
    user = purrr::map_chr(data, function(x){
      if (!is.na(x)) {
        return(unlist(jsonlite::fromJSON(x[1])$parsed_data)['adminId'])
      } else {
        return(NA_character_)
      }
    })
  )
all %>% group_by(arche, hdl) %>% summarize(n = n())
all %>% group_by(arche, hdl, status, user) %>% summarize(n = n()) %>% arrange(arche, desc(n))
all %>% 
  filter(!grepl('OK|Empty', status) | is.na(status)) %>%
  group_by(arche, domain, path, status) %>%
  summarize(n = n()) %>%
  arrange(arche, status, domain == 'https://id.acdh.oeaw.ac.at', desc(n), domain, path) %>%
  print(n = 1000)
all %>% 
  filter(status == 'Redirect URL does not exist (404)') %>% 
  mutate(project = paste0(domain, '/', path)) %>%
  select(project, pid, url) %>%
  arrange(project, url) %>%
  readr::write_csv2('errors.csv', na = '')
