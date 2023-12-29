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
hdl = readr::read_csv2('pids.csv') %>%
  mutate(
    data = if_else(grepl('^http', url), NA_character_, url),
    url = if_else(grepl('^http', url), url, NA_character_)
  ) %>%
  filter(grepl('./.', pid))

# ARCHE-only
arche %>% 
  anti_join(hdl) %>%
  select(-topcolclass) %>%
  tidyr::nest(id, pid)
arche %>% 
  anti_join(hdl) %>%
  select(-topcolclass) %>%
  arrange(topcollabel) %>%
  readr::write_csv2('arche_only.csv', na = '')

all = full_join(hdl %>% mutate(hdl = T), arche %>% mutate(arche = T, pid = sub('https://hdl.handle.net/', '', pid))) %>% 
  mutate(
    domain = sub('^(https?://[^/]+).*', '\\1', url),
    path = sub('^(https?://[^/]+)/([^/]+).*', '\\2', url)
  ) %>%
  mutate(
    path = if_else(domain %in% c('http://www.bruckner-online.at', 'https://digitarium-app.acdh.oeaw.ac.at', 'https://ferdinand-korrespondenz.acdh.oeaw.ac.at'), '', path),
    status = case_when(
      status == '400' ~ 'Bad request',
      status == '401' ~ 'Authorization required',
      status == '403' ~ 'Access denied',
      status == '404' ~ 'Redirect URL does not exist',
      status == '200' ~ 'OK',
      TRUE ~ status
    )
  )
all %>% group_by(arche, hdl) %>% summarize(n = n())
all %>% group_by(status) %>% summarize(n = n())
all %>% 
  filter(status != 'OK') %>%
  group_by(arche, domain, path, status) %>%
  summarize(n = n()) %>%
  arrange(arche,  domain, path, status) %>%
  print(n = 1000)

# HTTP error
d = all %>%
  filter(status == 'HTTP error') %>%
  select(id, pid, data) %>%
  mutate(
    url = purrr::map_chr(data, function(x) (jsonlite::fromJSON(jsonlite::fromJSON(x)$piddata) %>% filter(type == 'URL'))$parsed_data %>% unlist()),
    error = sub('^.*cURL error [0-9]+: ([^:]+).*$', '\\1', data)
  )
d %>%
  group_by(id, url, error) %>%
  summarize(n = n())
