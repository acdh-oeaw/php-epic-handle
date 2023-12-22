library(dplyr)
arche = readr::read_csv2('pids_arche.csv')
hdl = readr::read_csv2('pids.csv')
all = full_join(hdl %>% mutate(hdl = T), arche %>% rename(pid = arche_pid) %>% mutate(arche = T, pid = sub('https://hdl.handle.net/', '', pid))) %>% 
  mutate(
    domain = sub('^(https?://[^/]+).*', '\\1', url),
    path = sub('^(https?://[^/]+)/([^/]+).*', '\\2', url)
  ) %>%
  mutate(
    path = if_else(domain %in% c('http://www.bruckner-online.at', 'https://digitarium-app.acdh.oeaw.ac.at', 'https://ferdinand-korrespondenz.acdh.oeaw.ac.at'), '', path),
    status = case_when(
      status == -3 ~ 'PID with empty redirect URL',
      status == -2 ~ 'PID without redirect URL',
      status == -1 ~ 'Network error',
      status == 401 ~ 'Authorization required',
      status == 403 ~ 'Access denied',
      status == 404 ~ 'Redirect URL does not exist',
      status == 200 ~ 'OK'
    )
  )
all %>% group_by(arche, hdl) %>% summarize(n = n())
all %>% group_by(status) %>% summarize(n = n())
all %>% 
  filter(status != 200) %>%
  group_by(arche, domain, path, status) %>%
  summarize(n = n()) %>%
  arrange(arche,  domain, path, status) %>%
  print(n = 1000)
all %>% 
  filter(is.na(hdl)) %>%
  summarize(n = n())
