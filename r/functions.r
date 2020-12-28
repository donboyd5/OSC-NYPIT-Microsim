
# utility functions ----

get_stname <- function(stabbr){
  stname <- case_when(stabbr == "US" ~ "United States",
                      stabbr == "other" ~ "Other states combined",
                      stabbr %in% state.abb ~ state.name[match(stabbr, state.abb)],
                      TRUE ~ "ERROR")
  return(stname)
}
# get_stname(usny_sort)


get_stabbr <- function(stname){
  stabbr <- case_when(stname == "United States" ~ "US",
                      stname == "Other states combined" ~ "other",
                      stname %in% state.name ~ state.abb[match(stname, state.name)],
                      TRUE ~ "ERROR")
  return(stabbr)
}
# get_stabbr(get_stname(usny_sort))


html_wrap <- function(s, len){
  # gt uses <br> to break columns in html, rather than \n, so 
  # wrap lines and then replace \n with <br> for column headings
  wrapped <- str_wrap(s, len)
  return(str_replace_all(wrapped, "\\n", "<br>"))
}


# data functions ----
pol_change <- function(wide, policy1, policy2, vname="iitax"){
  # generic function to take two policies and create a file with changes and percent changes
  p1name <- policy1
  p2name <- policy2
  data <- wide %>%
    filter(variable==all_of(vname)) %>%
    rename(policy1=all_of(policy1), policy2=all_of(policy2)) %>%
    mutate(p1name=p1name, p2name=p2name, 
           diff=policy2 - policy1,
           pdiff=ifelse(policy1 > 0, diff / policy1 * 100, NA_real_)) %>% 
    select(variable, p1name, p2name, all_of(idvars), policy1, policy2, diff, pdiff)
  return(data)
}


# table functions ----
#.. policy changes ----
pol_change_table <- function(data, tab_title, tab_subtitle, p1name, p2name){
  stub <- names(data)[1]
  stub_label <- ifelse(stub=="stname", "", "Adjusted gross income")
  dollar_scale <- ifelse(stub=="stname", 1e-9, 1e-6)
  dollar_decimal <- ifelse(stub=="stname", 1, 0)
  # dollar_scale_label <- ifelse(stub=="stname", "millions", "billions")
  
  p1label <- html_wrap(p1name, 15)
  p2label <- html_wrap(p2name, 15)
  
  tab <- data %>%
    rename(stub=all_of(stub)) %>%
    gt() %>%  tab_header(
      title = tab_title,
      subtitle = tab_subtitle
    ) %>%
    cols_label(
      stub = stub_label,
      policy1 = html(p1label),
      policy2 = html(p2label),
      diff = html("$ change"),
      pdiff = html("% change")
    ) %>%
    fmt_currency(
      columns = c("policy1", "policy2", "diff"),
      rows=1,
      decimals = dollar_decimal,
      scale_by = dollar_scale,
      suffixing = FALSE
    ) %>%
    fmt_number(
      columns = c("policy1", "policy2", "diff"),
      rows=2:nrow(data),
      decimals = dollar_decimal,
      scale_by = dollar_scale,
      suffixing = FALSE
    ) %>%
    fmt_percent(
      columns="pdiff",
      scale_values = FALSE,
      decimals = 1
    ) %>% 
    tab_style(
      style = list(
        cell_fill(color = "#f7f7f7")
      ),
      locations = cells_body(
        rows = seq(1, nrow(data), 2)
      )
    ) %>%
    tab_source_note(source_note)
  return(tab)
}


pdiff_wide_table <- function(data, tab_title, tab_subtitle){
  data %>%
    gt() %>%  tab_header(
      title = tab_title,
      subtitle = tab_subtitle
    ) %>%
    cols_label(
      agi_label = "Adjusted gross income"
    ) %>%
    fmt_percent(
      columns=2:ncol(data),
      scale_values = FALSE,
      decimals = 1
    ) %>% 
    tab_style(
      style = list(
        cell_fill(color = "#f7f7f7")
      ),
      locations = cells_body(
        rows = seq(1, nrow(data), 2)
      )
    ) %>%
    tab_source_note(source_note)
}

diff_wide_table <- function(data, tab_title, tab_subtitle){
  data %>%
    gt() %>%  tab_header(
      title = tab_title,
      subtitle = tab_subtitle
    ) %>%
    cols_label(
      agi_label = "Adjusted gross income"
    ) %>%
    fmt_currency(
      columns = 2:ncol(data),
      rows=1,
      decimals = 0,
      suffixing = FALSE
    ) %>%
    fmt_number(
      columns = 2:ncol(data),
      rows=2:nrow(data),
      decimals = 0,
      suffixing = FALSE
    ) %>%    
    tab_style(
      style = list(
        cell_fill(color = "#f7f7f7")
      ),
      locations = cells_body(
        rows = seq(1, nrow(data), 2)
      )
    ) %>%
    tab_source_note(source_note)
}


#.. state_byagi_table ----
get_state_policies <- function(st, policy1, policy2){
  data <- df_changes_jct %>%
    filter(stabbr==st) %>%
    select(all_of(idvars), policy1=all_of(policy1), policy2=all_of(policy2), diff, pdiff) %>%
    arrange(agi_stub) %>%
    select(agi_label, policy1, policy2, diff, pdiff)
  return(data)
}

get_state_tab <- function(st, policy1, policy2, p1name, p2name, tab_title_suffix, dollars){
  data <- get_state_policies(st, policy1, policy2)
  tab_title <- paste0(get_stname(st), ":  ", tab_title_suffix)
  tab_subtitle <- paste0("Amounts are in ", dollars, " of 2018 dollars")
  tab <- pol_change_table(data, tab_title, tab_subtitle, p1name, p2name)
  return(tab)
}



