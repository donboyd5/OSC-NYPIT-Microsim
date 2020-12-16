

sweights_2017 = pd.read_csv(PUFDIR + 'allweights_geo_restricted_qmatrix-ipopt.csv')
puf2018 = pd.read_parquet(PUFDIR + 'puf2018' + '.parquet', engine='pyarrow')


# weights
wtdir <- r"(C:\programs_python\puf_analysis\ignore\puf_versions\)"
# C:\programs_python\puf_analysis\ignore\puf_versions\allweights2017_geo_restricted_qmatrix-ipopt.csv
wtfname_2017 <- "allweights2017_geo_restricted_qmatrix-ipopt.csv"
wtfname <- "allweights2018_geo2017_grown.csv"  # 2018 weights

weights2017 <- read_csv(paste0(wtdir, wtfname_2017))
weights <- read_csv(paste0(wtdir, wtfname))

# do a quick check on pct changes from 2017 to 2018, then we don't need 2017 anymore
check <- bind_rows(weights2017 %>% mutate(year="y2017"),
                   weights %>% mutate(year="y2018")) %>%
  pivot_longer(cols=-c(pid, ht2_stub, year)) %>%
  pivot_wider(names_from = year) %>%
  mutate(ratio=y2018 / y2017)
quantile(check$ratio) # good 1.64% for all


weights_long <- weights %>%
  rename("US"="weight") %>%
  select(-c(ht2_stub, geoweight_sum)) %>%
  pivot_longer(-pid, names_to = "stabbr", values_to = "weight")

# check that we have  weights for states we want to look at
sts <- unique(weights_long$stabbr)
# setdiff_all(sts, nyus_sort)  # should be no difference


# read a bunch of files, collapse by ht2stub, state
tcout <- r"(C:\programs_python\puf_analysis\ignore\taxcalc_output\)"
jctdir <- paste0(tcout, "stack_jct/")
pufdir <- paste0(tcout, "puf_default_2018income/")

pidfile <- read_parquet(paste0(jctdir, "0_law2017.parquet"),
                         col_select = all_of(c("pid", "RECID", "filer", "c00100", "ht2_stub", "ht2range")))
# sum(pidfile$RECID - pidfile$pid - 1) # good, we know that RECID - pid = 1

f <- function(fname, dir, vars) {
  fnbase <- tools::file_path_sans_ext(fname)
  read_parquet(paste0(dir, fname),
               col_select = all_of(vars)) %>%
    mutate(reform=fnbase)
}
list.files(jctdir)
fnames <- c("0_law2017.parquet", "7_law2018.parquet")
vars <- c("pid", "c00100", "c04800", "iitax")
df <- map_dfr(fnames, f, jctdir, vars)
dfl <- df %>%
  left_join(pidfile %>% select(pid, filer, ht2_stub, ht2range), by="pid") %>%
  pivot_longer(cols=-c(pid, filer, ht2_stub, ht2range, reform))

# weight, summarise
dfsums <- dfl %>%
  right_join(weights_long, by="pid") %>%
  mutate(wvalue=weight * value) %>%
  group_by(filer, stabbr, name, ht2_stub, ht2range, reform) %>%
  summarise(wvalue=sum(wvalue, na.rm=TRUE), .groups="drop")

dfsumtot <- dfsums %>%
  group_by(filer, stabbr, name, reform) %>%
  summarise(wvalue=sum(wvalue), .groups="drop") %>%
  mutate(ht2_stub=0, ht2range=" Total")

dfsums2 <- bind_rows(dfsums, dfsumtot) %>%
  arrange(filer, stabbr, name, reform, ht2_stub, ht2range)


st <- "NY"
dfsums2 %>%
  filter(filer, stabbr==st, reform=="7_law2018") %>%
  select(-filer, -reform) %>%
  mutate(wvalue=wvalue / 1e3) %>%
  pivot_wider(values_from = wvalue) %>%
  kable(format.args = list(big.mark=","), format="rst")

# compare agi under old law and new law
st <- "NY"
dfsums2 %>%
  filter(filer, stabbr==st, reform %in% c("0_law2017", "7_law2018"), name=="c00100") %>%
  select(-filer, -name) %>%
  mutate(wvalue=wvalue / 1e3) %>%
  pivot_wider(names_from=reform, values_from = wvalue) %>%
  mutate(diff=`7_law2018` - `0_law2017`,
         pdiff=diff / `0_law2017` * 100) %>%
  kable(digits=c(rep(0, 6), 1), format.args = list(big.mark=","), format="rst")


# compare agi in puf.csv with default settings ----
# first, get same variables as before
f2 <- function(fname, dir) {
  fnbase <- case_when(str_detect(fname, "2017law") ~ "2017law",
                      str_detect(fname, "2018law") ~ "2018law",
                      TRUE ~ "ERROR")
  read_parquet(paste0(dir, fname)) %>%
    mutate(reform=fnbase)
}

fnames <- c("pufcsv_2017law.parquet", "pufcsv_2018law.parquet")
pufdf <- map_dfr(fnames, f2, pufdir)
count(pufdf, reform)
ns(pufdf)


taxvals <- pufdf %>%
  left_join(pidfile %>% select(pid, RECID, filer, ht2_stub, ht2range), by="RECID") %>%
  filter(filer) %>%
  select(reform, ht2_stub, ht2range, s006, iitax) %>%
  group_by(reform, ht2_stub, ht2range) %>%
  summarise(iitax=sum(iitax * s006, na.rm=TRUE), .groups="drop")

taxvals %>%
  bind_rows(taxvals %>% 
              group_by(reform) %>% 
              summarise(iitax=sum(iitax)) %>%
              mutate(ht2_stub=0, ht2range="  Totals")) %>%
  mutate(iitax = iitax / 1e9) %>%
  pivot_wider(values_from = iitax, names_from = reform) %>%
  mutate(diff=`2018law` - `2017law`,
         pdiff=diff / `2017law` * 100) %>%
  arrange(ht2_stub) %>%
  kable(digits=1, format.args=list(big.mark=","), format="rst")



# identify records where c00100 changed
changerecs <- pufdf %>%
  select(reform, RECID, c00100) %>%
  pivot_wider(names_from = reform, values_from = c00100) %>%
  filter(`2017law` != `2018law`) %>%
  mutate(change=`2018law` - `2017law`) %>%
  arrange(-change)
summary(changerecs)
# 211687 227622 207120 205708

recs <- c(211687, 227622, 207120, 205708)
changedata <- pufdf %>%
  right_join(changerecs %>% select(RECID, agichange=change), by="RECID") %>%
  left_join(pidfile %>% select(pid, RECID, filer, ht2_stub, ht2range), by="RECID")  %>%
  pivot_longer(-c(pid, RECID, filer, ht2_stub, ht2range, reform, agichange, s006)) %>%
  pivot_wider(names_from = reform) %>%
  filter(`2018law` != 0 | `2018law` != 0) %>%
  mutate(itemchange=`2018law` - `2017law`) %>%
  arrange(-agichange, RECID, -abs(itemchange))

gids <- tibble(RECID=unique(changedata$RECID)) %>%
  mutate(gid=row_number())

changedata2 <- changedata %>%
  left_join(gids, by="RECID")

taxchange <- changedata2 %>%
  filter(name=="iitax") %>%
  mutate(across(c(`2017law`, `2018law`, itemchange), ~ .x * s006)) %>%
  group_by(ht2_stub, ht2range) %>%
  summarise(across(c(`2017law`, `2018law`, itemchange), ~ sum(.x) / 1e6), .groups="drop")

taxchange %>%
  bind_rows(taxchange %>% 
              summarise(across(c(`2017law`, `2018law`, itemchange), ~ sum(.x))) %>%
              mutate(ht2_stub=0, ht2range="  Totals")) %>%
  arrange(ht2_stub) %>%
  kable(digits=0, format.args=list(big.mark=","), format="rst")

# xvars <- c("c05800", "c09600", "c00100", "combined", "iitax", "aftertax_income",
#            "odc", "fips", "data_source", "s006", "FLPDYR", "expanded_income")

# c62100 Alternative Minimum Tax (AMT) taxable income
# e02000 Sch E total rental, royalty, partnership, S-corporation, etc, income/loss
# e00900 Description: Sch C business net profit/loss for filing unit
# sey : Self-employment income
# e26270 Sch E: Combined partnership and S-corporation net income/loss (in e02000)
# cmbtp Estimate of income on (AMT) Form 6251 but not in AGI

xvars <- c("combined", "iitax")
group1 <- c("c00100", "c62100")
group2 <- c("c02900", "c02500", "ymod1")
group3 <- c("e02000", "e00900")
group4 <- c("MARS", "XTOT", "dwks14", "dwks19", "c05800", "c09600", "ymod", "c04800")
yesvars <- c(group1, group2, group3)

changedata2 %>%
  filter(gid==6, !name %in% xvars) %>%
  mutate(bigval=pmax(abs(`2017law`), abs(`2018law`)),
         group=case_when(name %in% group1 ~ 1,
                         name %in% group2 ~ 2,
                         name %in% group3 ~ 3,
                         name %in% group4 ~ 4,
                         TRUE ~ 4)) %>%
  arrange(group, -bigval) %>%
  filter((itemchange > 0) | (row_number() < 20) | (name %in% c(yesvars))) %>%
  select(gid, name, `2017law`, `2018law`, itemchange) %>%
  kable(digits=c(0, 0, 0, 0, 0, 0, 0), format.args=list(big.mark=","), format="rst")


pufdfl <- pufdf %>%
  filter(RECID %in% changerecs$RECID) %>%
  left_join(pidfile %>% select(pid, RECID, filer, ht2_stub, ht2range), by="RECID") %>%
  pivot_longer(cols=-c(pid, RECID, filer, s006, ht2_stub, ht2range, reform)) %>%
  pivot_wider(names_from = reform) %>%
  filter(`2017law` != `2018law`) %>%
  mutate(w2017=s006 * `2017law`,
         w2018=s006 * `2018law`) %>%
  group_by(filer, ht2_stub, ht2range, name) %>%
  summarise(w2017=sum(w2017), 
            w2018=sum(w2018), .groups="drop")

pufsums <- pufdfl %>%
  group_by(filer, name) %>%
  summarise(w2017=sum(w2017),
            w2018=sum(w2018)) %>%
  mutate(ht2_stub=0, ht2range="  Totals")


pufdfl2 <- bind_rows(pufdfl, pufsums) %>%
  arrange(ht2_stub) %>%
  mutate(diff=w2018 - w2017,
         pdiff=diff / w2017 * 100)
count(pufdfl2, filer)


# which puf variables changed the most, among those who had a change in agi?
pufdfl2 %>%
  filter(filer, ht2_stub==0) %>%
  arrange(-abs(diff)) %>%
  mutate(across(.cols=c(w2017, w2018, diff), ~ .x / 1000)) %>%
  kable(digits=c(rep(0, 7), 2), format.args=list(big.mark=","), format="rst")

# look at a few people who had agi increase


pufdfl2 %>%
  filter(filer, ht2_stub==10) %>%
  arrange(-diff)

var <- "iitax"
pufdfl2 %>%
  # filter(filer) %>%
  filter(name==var) %>%
  mutate(across(.cols=c(w2017, w2018, diff), ~ .x / 1000)) %>%
  kable(digits=c(rep(0, 7), 1), format.args=list(big.mark=","), format="rst")

# tax change in the puf
pufref <- pufdf %>%
  select(reform, RECID, s006, iitax) %>%
  left_join(pidfile %>% select(pid, RECID, filer, ht2_stub, ht2range), by="RECID") %>%
  group_by(filer, ht2_stub, ht2range, reform) %>%
  summarise(iitax=sum(s006 * iitax, na.rm=TRUE) / 1e3, 
            .groups="drop")
pufref2 <- bind_rows(pufref, 
                     pufref %>% 
                       group_by(filer, reform) %>% 
                       summarise(iitax=sum(iitax), .groups="drop") %>%
                       mutate(ht2_stub=0, ht2range="  Totals")) %>%
  pivot_wider(names_from = reform, values_from = iitax) %>%
  mutate(diff=`2018law` - `2017law`,
         pdiff=diff / `2017law` * 100) %>%
  arrange(filer, ht2_stub)

pufref2 %>%
  filter(filer) %>%
  kable(digits=c(rep(0, 6), 1), format.args=list(big.mark=","), format="rst")




# more below here ----

ht2stubs <- df1 %>%
  select(ht2_stub, ht2range) %>%
  distinct() %>%
  arrange(ht2_stub)

# create collapsed stubs that combine ranges 1, 2, and 3
agi_bounds <- c(-Inf, 25e3, 50e3, 75e3, 100e3, 200e3, 500e3, 1e6, Inf)
agistubs <- ht2stubs %>%
  mutate(agi_stub = ifelse(ht2_stub <= 3, 1, ht2_stub - 2),
         agi_label=ifelse(ht2_stub <= 3, "Under $25,000", ht2range)) %>%
  select(agi_stub, agi_label) %>%
  distinct() %>%
  mutate(agi_ge=agi_bounds[1:(length(agi_bounds) - 1)],
         agi_lt=agi_bounds[2:length(agi_bounds)]) %>%
  add_row(agi_stub = 99,
          agi_label = "  Totals")
agistubs # these are the ranges we will use for the report

get_stub <- function(agi, agi_bounds){
  # get the income stub for a given agi
  cut(agi, agi_bounds, labels=FALSE, right=FALSE)
}

# get_stub(c(-1000, 0, 24999, 25e3, 1e9), agi_bounds) # check

# define agi stub for every person in 2017 base year
pid_stubs <- basedf %>%
  select(pid, filer, c00100) %>%
  mutate(agi_stub=get_stub(c00100, agi_bounds)) %>%
  arrange(pid)

# explore losses ----
vars <- c("pid", "c00100", "c04800", "iitax", "e00900", "e02000")
df <- map_dfr(fnames, f, jctdir, vars)
dfl <- df %>%
  mutate(e00900_neg=(e00900 < 0) * e00900,
         e02000_neg=(e02000 < 0) * e02000) %>%
  left_join(pidfile %>% select(pid, filer, ht2_stub, ht2range), by="pid") %>%
  pivot_longer(cols=-c(pid, filer, ht2_stub, ht2range, reform))


# weight, summarise
dfsums <- dfl %>%
  right_join(weights_long, by="pid") %>%
  mutate(wvalue=weight * value) %>%
  group_by(filer, stabbr, name, ht2_stub, ht2range, reform) %>%
  summarise(wvalue=sum(wvalue, na.rm=TRUE), .groups="drop")

dfsumtot <- dfsums %>%
  group_by(filer, stabbr, name, reform) %>%
  summarise(wvalue=sum(wvalue), .groups="drop") %>%
  mutate(ht2_stub=0, ht2range=" Total")

dfsums2 <- bind_rows(dfsums, dfsumtot) %>%
  arrange(filer, stabbr, name, reform, ht2_stub, ht2range)

var <- "e02000_neg"  # "e00900_neg"
dfsums2 %>%
  filter(filer, stabbr=="US", name==var) %>%
  pivot_wider(names_from = reform, values_from = wvalue) %>%
  kable(format.args=list(big.mark=","), format="rst")



f <- function(fname, dir, vars) {
  fnbase <- tools::file_path_sans_ext(fname)
  read_parquet(paste0(dir, fname),
               col_select = all_of(vars)) %>%
    mutate(reform=fnbase)
}
list.files(jctdir)
fnames <- c("0_law2017.parquet", "7_law2018.parquet")
vars <- c("pid", "c00100", "c04800", "iitax")
df <- map_dfr(fnames, f, jctdir, vars)
dfl <- df %>%
  left_join(pidfile %>% select(pid, filer, ht2_stub, ht2range), by="pid") %>%
  pivot_longer(cols=-c(pid, filer, ht2_stub, ht2range, reform))


# look at default 2017 puf and regrown 2017 puf ----
# C:\programs_python\puf_analysis\ignore\puf_versions
# puf2017_default.parquet
defdir <- r"(C:\programs_python\puf_analysis\ignore\puf_versions\)"
defpuf <- "puf2017_default.parquet"
rgpuf <- "puf2017_regrown.parquet"
puf2017a <- read_parquet(paste0(defdir, defpuf)) %>%
  mutate(ftype="pufdefault2017")
puf2017b <- read_parquet(paste0(defdir, rgpuf))

check <- puf2017a %>%
  select(RECID, s006a=s006) %>%
  left_join(puf2017b %>% select(RECID, s006b=s006),
            by="RECID")

# get the initial reweights for puf2017b
ns(puf2017b)
rw1 <- read_csv(paste0(defdir, "weights_reweight1_ipopt.csv"))
puf2017b2 <- puf2017b %>%
  left_join(rw1 %>% select(pid, weight), by="pid") %>%
  mutate(s006=ifelse(is.na(weight), s006, weight),
         ftype="rgrw2017") %>%
  select(-weight)



ns(puf2017)
count(puf2017, FLPDYR)


ystubs <- c(-9e99, 1.0, 10e3, 25e3, 50e3, 75e3, 100e3, 200e3, 500e3, 1e6, 9e99)
check <- c(-10, -1, 0, 1, 1000, 9999, 10e3, 10001, 1e9)
cut(check, ystubs, right=FALSE)

puffile <- puf2017b2
puffile <- puf2017a
vars <- c("ftype", "pid", "RECID", "filer", "s006", "c00100", "c04800", "iitax", "e00900", "e02000")
dfl <- puffile %>%
  select(all_of(vars)) %>%
  mutate(agigroup=cut(c00100, ystubs, right=FALSE),
         agilev=as.numeric(agigroup),
         agilab=as.character(agigroup),
         e00900_zpos=(e00900 >= 0) * e00900,
         e02000_zpos=(e02000 >= 0) * e02000,
         e00900_neg=(e00900 < 0) * e00900,
         e02000_neg=(e02000 < 0) * e02000) %>%
  select(-agigroup) %>%
  pivot_longer(cols=-c(ftype, pid, RECID, filer, agilev, agilab, s006)) %>%
  mutate(wvalue=s006 * value)

dfsums1 <- dfl %>%
  group_by(ftype, filer, agilev, agilab, name) %>%
  summarise(wvalue=sum(wvalue, na.rm=TRUE), .groups="drop")

dfsums2 <- dfsums1 %>%
  group_by(ftype, filer, name) %>%
  summarise(wvalue=sum(wvalue, na.rm=TRUE), .groups="drop") %>%
  mutate(agilev=0, agilab="Total")

dfsums <- bind_rows(dfsums1, dfsums2) %>%
  arrange(filer, name, agilev)


var <- "e02000_neg"  # e00900_neg e00900_zpos e02000_neg e02000_zpos
dfsums %>%
  filter(name==var, filer) %>%
  mutate(wvalue=wvalue / 1000) %>%
  kable(digits=0, format.args=list(big.mark=","), format="rst")


# HT2_AGI_STUBS = [-9e99, 1.0, 10e3, 25e3, 50e3, 75e3, 100e3,
#                  200e3, 500e3, 1e6, 9e99]
# 
# ht2stubs = pd.DataFrame([
#   [0, 'All income ranges'],
#   [1, 'Under $1'],
#   [2, '$1 under $10,000'],
#   [3, '$10,000 under $25,000'],
#   [4, '$25,000 under $50,000'],
#   [5, '$50,000 under $75,000'],
#   [6, '$75,000 under $100,000'],
#   [7, '$100,000 under $200,000'],
#   [8, '$200,000 under $500,000'],
#   [9, '$500,000 under $1,000,000'],
#   [10, '$1,000,000 or more']],
#   columns=['ht2stub', 'ht2range'])



