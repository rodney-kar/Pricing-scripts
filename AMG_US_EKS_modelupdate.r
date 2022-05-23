
print("************ START NEW ALGO RUN ****************")
mastertime<-Sys.time()
print(mastertime)
print(gc())

# BEGIN: LIBRARIES, which are required for all code in this script
SHOWLIB<-TRUE;if (SHOWLIB==TRUE){
  #Add libraries/source internal functions
  library(RODBC)
  #library(h2o);h2o.init()
  library(sqldf)
  library(lubridate)
  library(quantmod)
  library(plyr)
  library(dplyr)
  library(timeDate)
  library(tseries)
  library(tidyr)
  library(stringr)
  library(jsonlite)
  library(taskscheduleR)
  library(keyring)
  options(error=NULL)
  SNOWFLAKE_dbConnection<-odbcConnect("snowflake_azure", pwd = key_get('SNOWFLAKE'))
  
  # .rs.restartR() # Restart R session
  #h2o.removeAll() remove all h2o objects
  
}
# END:   LIBRARIES

# BEGIN: LOADDATA
LOADDATA<-TRUE;if(LOADDATA==TRUE){

usa.sql<-function(){"WITH
    input_data
        AS (
        SELECT
            (SUBSTRING(wc.vin::TEXT, 1, 8) || '_' || SUBSTRING(wc.vin::TEXT, 10, 1))::TEXT                              AS vin8_10,
            wc.vin::TEXT                                                                                                AS vin,
            wc.ODOMETER_MI::FLOAT                                                                                       AS mileage,
            wc.sale_date::TEXT                                                                                          AS sale_date,
            wc.AUTOGRADE::FLOAT                                                                                         AS vehicle_condition_grade,
            mvl.BASEMSRPHIGH::TEXT                                                                                      AS mvl_msrp,
            mvl.year::TEXT                                                                                              AS mvl_model_year,
            mvl.make::TEXT                                                                                              AS mvl_make,
            mvl.model::TEXT                                                                                             AS mvl_model,
            mvl.trim::TEXT                                                                                              AS mvl_trim,
            wc.series::TEXT                                                                                             AS client_supplied_trim,
            mvl.bodytype::TEXT                                                                                          AS mvl_bodytype,
            mvl.cylinders::TEXT                                                                                         AS mvl_cylinders,
            mvl.beststylename::TEXT                                                                                     AS mvl_best_style_name,
            mvl.marketclassname::TEXT                                                                                   AS mvl_market_class_name,
            mvl.drivingwheels::TEXT                                                                                     AS mvl_drivingwheels,
            mvl.fueltype::TEXT                                                                                          AS mvl_fueltype,
            mvl.disp_liters::TEXT /*new field*/                                                                         AS mvl_displ_liters,
            mvl.forced_induction_type::TEXT /*new field*/                                                               AS mvl_forced_induction_type,
            zrx.region::TEXT                                                                                            AS buyer_region,
            wc.sale_price                                                                                               AS sale_price,
            ROW_NUMBER() OVER (PARTITION BY wc.sale_date, wc.vin ORDER BY EDITDISTANCE(UPPER(trim), UPPER(series)) ASC) AS edit_filter
--             buyer_zip::TEXT                                                           AS buyer_zip,
--             row_id::TEXT                                                              AS row_id
        FROM
            ADESA_SHARE.CERTIFIED_ANALYTICS.WHOLE_CAR_OFFERED_AND_SOLD wc
                LEFT JOIN (SELECT
                           DISTINCT
                            SUBSTRING(vin::TEXT, 1, 8) || '_' || SUBSTRING(vin::TEXT, 10, 1)::TEXT vin8_10,
                            trim,
                            mode(BASEMSRPHIGH) as basemsrphigh,
                            mode(year) as year,
                            mode(make) as make,
                            mode(model) as model,
                            mode(bodytype) as bodytype,
                            mode(cylinders) as cylinders,
                            mode(beststylename) as beststylename,
                            mode(marketclassname) as marketclassname,
                            mode(drivingwheels) as drivingwheels,
                            mode(fueltype) as fueltype,
                            mode(disp_liters) as disp_liters,
                            mode(forced_induction_type) as forced_induction_type
                            FROM prod.stg_mvl.v_chrome_vins
                            WHERE COUNTRY = 'US'
                            GROUP BY
                            1,2
                    )    mvl
                              ON mvl.vin8_10 = (SUBSTRING(wc.vin::TEXT, 1, 8) || '_' || SUBSTRING(wc.vin::TEXT, 10, 1))::TEXT
                              AND
                              (
                                 /* 4 backslashes in R/Python 2 backslash as direct SQL */
                                 REGEXP_INSTR(REGEXP_REPLACE(UPPER(trim), '[^a-zA-Z0-9 - ./]', ' '),
                                              '\\\\b' || REGEXP_REPLACE(UPPER(series), '[^a-zA-Z0-9 - ./]', ' ') ||
                                              '\\\\b') > 0
                             OR
                                 /* 4 backslashes in R/Python 2 backslash as direct SQL */
                                 REGEXP_INSTR(REGEXP_REPLACE(UPPER(series), '[^a-zA-Z0-9 - ./]', ' '),
                                              '\\\\b' || REGEXP_REPLACE(UPPER(trim), '[^a-zA-Z0-9 - ./]', ' ') ||
                                              '\\\\b') > 0
                                              )
        LEFT JOIN prod.dwh.LKP_ZIP_REGION_XREF zrx
        ON AUCTION_POSTAL_CODE = zrx.ZIP
        WHERE
          SALE_DATE BETWEEN CURRENT_DATE - INTERVAL '5 years' AND CURRENT_DATE
          AND AUCTION_COUNTRY = 'UNITED STATES'
          AND wc.sale_price > 300
          AND wc.odometer_mi BETWEEN 1000 AND 200000
          AND LENGTH(wc.vin) = 17
          AND wc.enterprise_category IN ('In Lane', 'DealerBlock At Auction', 'Simulcast')
          AND LOWER(wc.transaction_type) = 'sold'
            QUALIFY edit_filter = 1
    ),

    /* join input data and mvl values with vin8_10 rollup */
    client_chrome
        AS (
        SELECT DISTINCT
            id.sale_price,
            id.vin8_10                                                                 AS vin8_10,
            id.vin,
            id.mileage,
            id.sale_date                                                               AS sale_date,
            DATE_TRUNC('week', ('' || id.sale_date)::DATE)::DATE                       AS sale_week,
            (DATE_TRUNC('week', ('' || id.sale_date)::DATE) + INTERVAL '1 week')::DATE AS sale_week_end,
            last_day(id.sale_date::DATE)::DATE                                         AS salemonth,
            extract(MONTH FROM id.sale_date::DATE)::TEXT                               AS seasonal,
            COALESCE(id.mvl_msrp::FLOAT, cv8.med_msrp_v810::FLOAT,
                     cv8.avg_base_msrp_high::FLOAT)                                    AS msrp,
            id.vehicle_condition_grade::FLOAT                                          AS vehicle_grade,
            COALESCE(id.mvl_model_year::TEXT,
                     cv8.model_year::TEXT)                                             AS my,
            DATEDIFF(MONTHS,
                     ((COALESCE(id.mvl_model_year, cv8.model_year)::INT - 1)::TEXT || '-09-01')::DATE,
                     id.sale_date::DATE
                )                                                                      AS age,
            UPPER(COALESCE(id.mvl_model::TEXT,
                           cv8.mode_best_model_name::TEXT))                            AS model_nm,
            UPPER(COALESCE(id.mvl_bodytype::TEXT,
                           cv8.mode_body_type::TEXT))                                  AS body_type,
            UPPER(COALESCE(id.mvl_make::TEXT, cv8.division::TEXT))                     AS make_nm,
            COALESCE(id.mvl_cylinders::TEXT, cv8.mode_cylinders::TEXT,
                     '5')::TEXT                                                        AS mode_cylinders,
            UPPER(COALESCE(id.mvl_make::TEXT, cv8.division::TEXT)::TEXT) ||
            ':' || UPPER(COALESCE(id.mvl_model::TEXT,
                                  cv8.mode_best_model_name::TEXT)::TEXT) || ':' ||
            COALESCE(id.mvl_model_year::TEXT,
                     cv8.model_year::TEXT)::TEXT                                       AS mmy,
            UPPER(COALESCE(id.mvl_make::TEXT, cv8.division::TEXT)::TEXT) ||
            ':' || UPPER(COALESCE(id.mvl_model::TEXT,
                                  cv8.mode_best_model_name::TEXT)::TEXT)               AS mm,
            UPPER(COALESCE(id.mvl_make::TEXT, cv8.division::TEXT)::TEXT) ||
            ':' || UPPER(COALESCE(id.mvl_model::TEXT,
                                  cv8.mode_best_model_name::TEXT)::TEXT) || ':' ||
            COALESCE(id.mvl_trim::TEXT, 'OTHER'::TEXT)::TEXT                            AS mmt,
            UPPER(COALESCE(id.mvl_trim::TEXT, 'OTHER'::TEXT))                          AS trim_nm,
            id.client_supplied_trim                                                    AS client_supplied_trim,

            UPPER(COALESCE(id.mvl_best_style_name::TEXT,
                           cv8.mode_best_style_name::TEXT))                            AS best_style_name,
            UPPER(COALESCE(id.mvl_market_class_name::TEXT,
                           cv8.mode_market_class_name::TEXT))                          AS market_class,
            CASE
                WHEN UPPER(COALESCE(id.mvl_drivingwheels::TEXT,
                                    cv8.mode_driving_wheels::TEXT)) = '4X4'
                    THEN '4x4'
                WHEN UPPER(COALESCE(id.mvl_drivingwheels::TEXT,
                                    cv8.mode_driving_wheels::TEXT)) = 'AWD'
                    THEN 'AWD'
                WHEN UPPER(COALESCE(id.mvl_drivingwheels::TEXT,
                                    cv8.mode_driving_wheels::TEXT)) = 'FRONT'
                    THEN 'Front'
                WHEN UPPER(COALESCE(id.mvl_drivingwheels::TEXT,
                                    cv8.mode_driving_wheels::TEXT)) = 'REAR'
                    THEN 'Rear'
                    ELSE 'unknown'
                END                                                                    AS mode_drivingwheels,
            CASE
                WHEN INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(
                        COALESCE(id.mvl_fueltype::TEXT,
                                 cv8.mode_fuel_type::TEXT), '[^[A-Za-z,]]', ' ')), ' , ', ', '),
                                                           '/', ' '), '-', ' ')) IN
                     ('Diesel Fuel', 'Electric Fuel System', 'Flex Fuel Capability',
                      'Flex Fuel Electric Hybrid', 'Gas Electric Hybrid', 'Gasoline Fuel',
                      'Gasoline Natural Gas', 'Hydrogen Fuel', 'Natural Gas Fuel',
                      'Plug In Electric Gas')
                    THEN INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(REGEXP_REPLACE(
                        COALESCE(id.mvl_fueltype::TEXT,
                                 cv8.mode_fuel_type::TEXT), '[^[A-Za-z,]]', ' ')), ' , ', ', '),
                                                               '/', ' '), '-', ' '))
                    ELSE 'Gasoline Fuel'::TEXT
                END                                                                    AS mode_fueltype,
            id.buyer_region::TEXT                                                      AS analytical_region,
            COALESCE(id.mvl_displ_liters::text, 'unknown'::text)                       AS mode_displiters,
            COALESCE(id.mvl_forced_induction_type::text, 'unknown'::text)              AS mode_induction

        FROM
            input_data                                    id
                LEFT JOIN prod.stg_prc_ftr.v_chrome_vin8_10_us cv8
                              ON cv8.vin8_10 = id.vin8_10
    )
/*client supplied and decoded data joined with support feature tables*/

        SELECT DISTINCT
            cmvl.vin8_10,
            cmvl.vin,
            cmvl.mileage,
            cmvl.sale_date,
            cmvl.sale_week,
            cmvl.sale_week_end,
            cmvl.salemonth,
            cmvl.seasonal,
            cmvl.sale_price,
            cmvl.msrp,
            cmvl.vehicle_grade,
            cmvl.my,
            cmvl.age,
            cmvl.model_nm,
            cmvl.body_type,
            cmvl.make_nm,
            cmvl.mode_cylinders,
            cmvl.mmy,
            cmvl.mm,
            cmvl.mmt,
            cmvl.client_supplied_trim,
            cmvl.trim_nm,
            cmvl.best_style_name,
            cmvl.market_class,
            cmvl.mode_drivingwheels,
            cmvl.mode_fueltype,
            -- new mvl features
            cmvl.mode_displiters,
            cmvl.mode_induction,
            cmvl.analytical_region,
--            NULL                                                                  AS trim_match_filter,      -- place holder
            /* multiply predicted price by factor_low and factor_high for range */
--     COALESCE(dr.low_pct + 1, 0.95::FLOAT)                                 AS factor_low,
--     COALESCE(dr.high_pct + 1, 1.05::FLOAT)                                AS factor_high,
            ha.historical_price,
            ha.historical_mileage,
            ha.sold::INT                                                          AS weekly_sold,
            ha.total_sold,
            ha.pricevel,
            ha.priceacc,
            ha.salesvel,
            ha.salesacc,
            ha.fittedsales,
            cmvl.mileage::FLOAT / ha.historical_mileage::FLOAT                    AS historical_sm_ratio,
            dep.dcurve,
            fred.ema50_sp500_lag1,                                                                           -- not used in new model retained for comparison
            fred.gasregcovw_lag1,
            fred.aisrsa_lag4,
            fred.cusr0000seta02_lag8,
            fred.b140rg3q086sbea_lag28,
            -- new fred features start here
            fred.a145rc1q027sbea_lag28,
            fred.altsales_lag4,
            fred.b140rc1q027sbea_lag28,
            fred.dtctlveuanq_lag28,
            -- new hist_trim_avg features
            COALESCE(hta.historical_mmt_mileage, htother.historical_mmt_mileage)  AS historical_mmt_mileage, -- not used directly in model
            cmvl.mileage::FLOAT / COALESCE(hta.historical_mmt_mileage::FLOAT,
                                           htother.historical_mmt_mileage::FLOAT) AS historical_sm_mmt_ratio,
            COALESCE(hta.fittedsales_mmt, htother.fittedsales_mmt)                AS fittedsales_mmt,
            COALESCE(hta.fittedsales_mmyt, htother.fittedsales_mmyt)              AS fittedsales_mmyt,
            COALESCE(hta.historical_mmyt_price, htother.historical_mmyt_price)    AS historical_mmyt_price,
            cmvl.sale_price/cmvl.msrp AS sp_ratio,


            CASE
                WHEN cmvl.age::INT > 239
                    THEN 'Age is greater than 239 months;'
                    ELSE ''
                END || CASE
                           WHEN cmvl.msrp IS NULL
                               THEN 'Chrome msrp info missing;'
                               ELSE ''
                END || CASE
                           WHEN dep.dcurve IS NULL
                               THEN 'Pre calculated depreciation score is missing;'
                               ELSE ''
                END || CASE
                           WHEN fred.gasregcovw_lag1 IS NULL
                               OR fred.aisrsa_lag4 IS NULL
                               OR fred.cusr0000seta02_lag8 IS NULL
                               OR fred.b140rg3q086sbea_lag28 IS NULL
                               OR fred.a145rc1q027sbea_lag28 IS NULL
                               OR fred.altsales_lag4 IS NULL
                               OR fred.b140rc1q027sbea_lag28 IS NULL
                               OR fred.dtctlveuanq_lag28 IS NULL
                               THEN 'Fred data is missing for sale date;'
                               ELSE ''
                END || CASE
                           WHEN ha.historical_price IS NULL
                               OR ha.historical_mileage IS NULL
                               OR ha.sold IS NULL
                               OR ha.total_sold IS NULL
                               OR ha.pricevel IS NULL
                               OR ha.priceacc IS NULL
                               OR ha.salesvel IS NULL
                               OR ha.salesacc IS NULL
                               OR ha.fittedsales IS NULL
                               OR (hta.fittedsales_mmt IS NULL AND htother.fittedsales_mmt IS NULL)
                               OR (hta.fittedsales_mmyt IS NULL AND htother.fittedsales_mmyt IS NULL)
                               OR (hta.historical_mmyt_price IS NULL AND htother.historical_mmyt_price IS NULL)
                               OR (hta.historical_mmt_mileage IS NULL AND htother.historical_mmt_mileage IS NULL)
                               THEN 'Missing historical sales data;'
                               ELSE ''
                END                                                               AS MESSAGE
        FROM
            client_chrome                                           cmvl
                LEFT JOIN prod.stg_prc_ftr.v_depreciation                dep
                              ON dep.makemodel = cmvl.mm
                              AND dep.age = cmvl.age
                LEFT JOIN prod.stg_prc_ftr.v_historical_averages_us      ha
                              ON ha.mmy = cmvl.mmy
                              AND ha.sale_week_end = cmvl.sale_week
                LEFT JOIN prod.stg_prc_ftr.v_fred_us                     fred
                              ON fred.sale_week = cmvl.sale_week
                              --         LEFT JOIN stg_prc_ftr.amg_dynamic_ranges dr
--                   ON dr.make_nm = cmvl.make_nm
--                       AND dr.model_nm = cmvl.model_nm
--                       AND dr.my = cmvl.my
                LEFT JOIN prod.stg_prc_ftr.v_historical_trim_averages_us hta
                              ON hta.sale_week_end = cmvl.sale_week
                              AND hta.make = cmvl.make_nm
                              AND hta.model = cmvl.model_nm
                              AND hta.my = cmvl.my
                              AND hta.trim = cmvl.trim_nm
                LEFT JOIN prod.stg_prc_ftr.v_historical_trim_averages_us htother
                              ON htother.sale_week_end = cmvl.sale_week
                              AND htother.make = cmvl.make_nm
                              AND htother.model = cmvl.model_nm
                              AND htother.my = cmvl.my
                              AND htother.trim = 'OTHER'
             WHERE  cmvl.msrp <> 0
                    AND cmvl.msrp IS NOT NULL
                    AND cmvl.age > 0
                    AND dep.dcurve IS NOT NULL
                    AND cmvl.vehicle_grade BETWEEN 0 AND 5
                    AND cmvl.sale_price BETWEEN 1000 AND 500000
                    AND cmvl.sale_price/cmvl.msrp  BETWEEN 0 and 1
                    AND cmvl.body_type IS NOT NULL
                    AND cmvl.market_class IS NOT NULL
  
  "
}
data.df<-sqlQuery(SNOWFLAKE_dbConnection,usa.sql(),stringsAsFactors=T)  
print(head(data.df))
data.df<-data.df %>% filter(MESSAGE=='')
names(data.df)<-tolower(names(data.df))
print(summary(data.df))


input.df<-data.df %>% filter(as.Date(sale_date) < Sys.Date()- 30)
last30.df<-data.df %>% filter(as.Date(sale_date) >= Sys.Date()- 30)


input.df$sale_date<-as.character(input.df$sale_date)
input.df$sale_week<-as.character(input.df$sale_week)
input.df$salemonth<-as.character(input.df$salemonth)
input.df$seasonal<-as.factor(input.df$seasonal)

last30.df$sale_date<-as.character(last30.df$sale_date)
last30.df$sale_week<-as.character(last30.df$sale_week)
last30.df$salemonth<-as.character(last30.df$salemonth)
last30.df$seasonal<-as.factor(last30.df$seasonal)


}
# END: LOADDATA

# BEGIN: PRICING MODEL
PRICINGMODEL=TRUE;if(PRICINGMODEL==TRUE){
  print("BEGIN: PRICING") 
  # h2o initiate and remove old data
  library(glue)
  library(h2o)
  start_h2o <- function(model_name, baseport=40000, nthreads=10, mem_gb=45){
    command <- glue::glue('java -Xmx{mem_gb}g -jar "C:\\Program Files\\R\\R-4.1.2\\library\\h2o\\java\\h2o.jar" -ip "localhost" -name {model_name} -baseport {baseport} -nthreads {nthreads} -notify_local "C:\\temp_h2o\\{model_name}.txt"')
    system(command, wait = F)
    Sys.sleep(30)
    info_file = glue::glue("C:\\temp_h2o\\{model_name}.txt")
    write('\n', file = info_file, append = T)
    connection_info <- read.csv(info_file, header = F, sep = ':', col.names = c('ip', 'port'))
    
    h2o.init(ip = connection_info$ip[1], port = connection_info$port[1])
  }
   # connect
  start_h2o('amgeks',mem_gb=60)
  h2o.removeAll() 
  if(as.numeric(str_extract_all(capture.output(h2o.clusterInfo())[9],"\\(?[0-9,.]+\\)?")[[1]][2]) <= 50){h2o.shutdown(prompt=FALSE)} else{
  
  #Model construction
  GBM=TRUE;if(GBM==TRUE){
    amg_us.h2o <-as.h2o(input.df)
    names(amg_us.h2o)<-tolower(names(amg_us.h2o))
    
    last30_us.h2o <-as.h2o(last30.df)
    names(last30_us.h2o)<-tolower(names(last30_us.h2o))
       
    splits.amg_us_update <- h2o.splitFrame(
      data = amg_us.h2o, 
      ratios = c(0.7,0.15),   ## only need to specify 2 fractions, the 3rd is implied
      destination_frames = c("amg_us_train.hex", "amg_us_valid.hex", "amg_us_test.hex"), seed = 3791
    )
    train.amg_us_update <- splits.amg_us_update[[1]]
    valid.amg_us_update <- splits.amg_us_update[[2]]
    test.amg_us_update  <- splits.amg_us_update[[3]]
    
    vars_alt=T;if(vars_alt==T){
      response_target <- "sp_ratio"
      segments <- c("sale_date","sale_week","sale_month","odometer_mi","vin","msrp","sale_price","vehicle_grade","age","model_nm","mm","mmy","mmt","mmyt","AMG_ratio_3_0","AMG_3_0")
      predictors_vars <- c(#Vehicle
                           "make_nm"
                           ,"body_type"
                           ,"market_class"
                           ,"my" 
                           ,"mileage"
                           ,"vehicle_grade"
                           ,"analytical_region" 
                           #Engine
                           ,"mode_cylinders"
                           ,"mode_fueltype"
                           ,"mode_drivingwheels"
                           ,"mode_induction"
                           ,"mode_displiters"
                           
                           #Derived
                           ,"dcurve"
                           ,"historical_price"
                           ,"historical_mmyt_price"
                           ,"historical_sm_mmt_ratio"
                           ,"historical_sm_ratio"
                           ,"pricevel"
                           ,"salesvel"
                           ,"salesacc"
                           ,"priceacc"
                           ,"fittedsales_mmt"
                           ,"fittedsales"
                           ,"fittedsales_mmyt"
                           #FRED
                           ,"gasregcovw_lag1"
                           ,"aisrsa_lag4" 
                           ,"altsales_lag4"
                           ,"cusr0000seta02_lag8"
                           ,"dtctlveuanq_lag28"
                           ,"b140rg3q086sbea_lag28"
                           ,"a145rc1q027sbea_lag28"
                           ,"b140rc1q027sbea_lag28")
      
    }
    
    mm_details<-data.frame(input.df %>% filter(mm %in% names(tail(sort(table(input.df$mm)),25))) %>%
                           dplyr::group_by(mm) %>%
                           summarise(
                           trims = length(unique(mmt)),
                           msrp_range = diff(range(msrp)),
                           msrp_pct_range = diff(range(msrp))/max(msrp,na.rm=T),
                           my_range = diff(range(my)),
                           N = length(unique(vin))
                           ) %>% arrange(desc(msrp_pct_range))
                          
    )
    
    mmt_details<-data.frame(input.df %>% filter(mmt %in% names(tail(sort(table(input.df$mmt)),50))) %>%
                             dplyr::group_by(mmt) %>%
                             summarise(
                               msrp_range = diff(range(msrp)),
                               msrp_pct_range = diff(range(msrp))/max(msrp,na.rm=T),
                               my_range = diff(range(my)),
                               N = length(unique(vin))
                             ) %>% arrange(desc(msrp_pct_range))
                           %>% filter(N> 5000)
    )
    
    # base model
    basesearch=FALSE;if(basesearch==TRUE){
                  gbm_train <- h2o.gbm(x = predictors_vars
                         , y = response_target
                         , ntrees=3500
                         , max_depth = 14
                         , nbins = 512        
                         , nbins_cats = 1024  
                         , training_frame = train.amg_us_update
                         , validation_frame = valid.amg_us_update
                         , learn_rate = 0.05
                        # , learn_rate_annealing=0.99
    )
    data.frame(h2o.varimp(gbm_train))
    }
    
    #targeted model
    targetsearch=FALSE;if(targetsearch==TRUE){
      currentversionpath_amg <- dir("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\")
      mojo_amgversion_path<-last(currentversionpath_amg[grep("AMG_",currentversionpath_amg)])
      mojo_amgmodel_path<-paste("AMG_US_model_20"
                                ,substring(mojo_amgversion_path,20,21)
                                ,substring(mojo_amgversion_path,16,17)
                                ,substring(mojo_amgversion_path,18,19),sep="")
      s3modelfile_amg<-paste("F:/Projects/Adesa_Market_Guide/SNOWFLAKE_models/h2o_models/",mojo_amgversion_path,"/",mojo_amgmodel_path,".zip",sep="")
      genmodelfile_amg<-paste("F:/Projects/Adesa_Market_Guide/SNOWFLAKE_models/h2o_models/",mojo_amgversion_path,"/h2o-genmodel.jar",sep="")
      currentupdate_amg_model<-h2o.loadModel(path=paste("F:/Projects/Adesa_Market_Guide/SNOWFLAKE_models/h2o_models/",mojo_amgversion_path,"/",mojo_amgmodel_path,sep=""))
      data.frame(h2o.varimp(currentupdate_amg_model))
      
      
      amg_target_model<-h2o.gbm(x = predictors_vars,
                                y = response_target,
                                max_depth = currentupdate_amg_model@allparameters$max_depth,
                                ntrees=  currentupdate_amg_model@allparameters$ntrees,
                                sample_rate = currentupdate_amg_model@allparameters$sample_rate,
                                col_sample_rate = currentupdate_amg_model@allparameters$col_sample_rate,
                                col_sample_rate_change_per_level = currentupdate_amg_model@allparameters$col_sample_rate_change_per_level,
                                min_rows = currentupdate_amg_model@allparameters$min_rows,
                                nbins = currentupdate_amg_model@allparameters$nbins,
                                nbins_cats = currentupdate_amg_model@allparameters$nbins_cats,
                                min_split_improvement = currentupdate_amg_model@allparameters$min_split_improvement,
                                histogram_type = currentupdate_amg_model@allparameters$histogram_type,
                                categorical_encoding = currentupdate_amg_model@allparameters$categorical_encoding,
                                monotone_constraints = list(vehicle_grade = 1),
                                distribution = currentupdate_amg_model@allparameters$distribution,
                                seed = 151,
                                learn_rate = 0.05,
                                model_id = paste("AMG_US_model_",gsub("-","",max(as.Date(last30.df$sale_date),na.rm=T)),sep=""),
                                training_frame = train.amg_us_update,
                                validation_frame =valid.amg_us_update
      )
      h2o.rmse(h2o.performance(amg_target_model,valid.amg_us_update))
      h2o.rmse(h2o.performance(amg_target_model,test.amg_us_update))
      h2o.rmse(h2o.performance(amg_target_model,last30_us.h2o))
      data.frame(h2o.varimp(amg_target_model))
    }
    
    
    # hyper parameters search
    gridst<-Sys.time()
    amg_us_grid<-h2o.grid(hyper_params = list(max_depth = seq(5,15,2),
                                       ntrees=c(8:20)*250,
                                       sample_rate = c(seq(0.2,0.5,0.1),seq(0.55,1,0.05)),
                                       col_sample_rate = c(seq(0.2,0.4,0.05),seq(0.45,0.75,0.1),seq(0.80,1,0.05)),                                         
                                       col_sample_rate_change_per_level = c(0.5,seq(0.9,1.1,0.05),1.5,2), 
                                       min_rows = 2^seq(4,10),   
                                       nbins = 2^seq(4,10),        
                                       nbins_cats = 2^seq(5,12),    
                                       min_split_improvement = c(1e-8,1e-6,1e-4,1e-3,0),    
                                       histogram_type = c("QuantilesGlobal","RoundRobin"),
                                       categorical_encoding = c("Enum"),
                                       learn_rate = c(0.01, 0.1)),
                   search_criteria = list(strategy = "RandomDiscrete",
                                          seed = 1143,
                                          max_runtime_secs = 21600, #6 hours     
                                          max_models = 100,
                                          stopping_rounds = 3,
                                          stopping_tolerance = 1e-3,
                                          stopping_metric = "RMSE"),
                   algorithm="gbm",
                   nfolds=5,
                   keep_cross_validation_predictions = TRUE,
                   grid_id="AMG_us_grid",
                   x = predictors_vars, 
                   y = response_target, 
                   training_frame = train.amg_us_update, 
                   validation_frame = valid.amg_us_update,
                   seed= 1143
                   #learn_rate_annealing=0.99
                   
    ) 
    print(paste("GRID TIME",Sys.time()-gridst,sep='=')) 

    gc()
    amgusGrid <- h2o.getGrid("AMG_us_grid", sort_by = "RMSE")    
    
    if(length(amgusGrid@model_ids)>=5){
    #selection
    model1<-h2o.getModel(amgusGrid@model_ids[[1]])
    model2<-h2o.getModel(amgusGrid@model_ids[[2]])
    model3<-h2o.getModel(amgusGrid@model_ids[[3]])
    model4<-h2o.getModel(amgusGrid@model_ids[[4]])
    model5<-h2o.getModel(amgusGrid@model_ids[[5]])
    modelselection<-data.frame(c('model1','model2','model3','model4','model5'),rep(NA,5),rep(NA,5),rep(NA,5),rep(NA,5))
    names(modelselection)<-c('model_id','varimp','valdiff','rmse','last30')
    for(i in 1:5){
    modelselection$varimp[i]<-all(data.frame(h2o.varimp(h2o.getModel(amgusGrid@model_ids[[i]])))$percentage < 0.5)[1]
    modelselection$valdiff[i]<-h2o.getModel(amgusGrid@model_ids[[i]])@model$training_metrics@metrics$RMSE-h2o.getModel(amgusGrid@model_ids[[i]])@model$validation_metrics@metrics$RMSE
    modelselection$rmse[i]<-h2o.rmse(h2o.performance(h2o.getModel(amgusGrid@model_ids[[i]]), newdata = test.amg_us_update))
    modelselection$last30[i]<-h2o.rmse(h2o.performance(h2o.getModel(amgusGrid@model_ids[[i]]), newdata = last30_us.h2o))
    }
    modelselection<-subset(modelselection,varimp==T & abs(valdiff)<=0.01)
    modelselection<- head(modelselection[order(modelselection$rmse),],1)
    as.numeric(row.names(modelselection))
    
    best_update <- h2o.getModel(amgusGrid@model_ids[[as.numeric(row.names(modelselection))]])
    best_update@parameters
    data.frame(h2o.varimp(best_update))
    } else { best_update <- h2o.getModel(amgusGrid@model_ids[[1]]) }
    
    #All modeloutput and features
    currentversionpath <- dir("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\")
    currentversionpath <- first(tail(currentversionpath[grep("AMG_US",currentversionpath)],1))
    
    currentmodelpath<-paste("AMG_US_model_20"
                            ,substring(currentversionpath,20,21)
                            ,substring(currentversionpath,16,17)
                            ,substring(currentversionpath,18,19),sep="")
    
    
    current_model<-h2o.loadModel(path=paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",currentversionpath,"\\",currentmodelpath,sep=""))
    as.matrix(unlist(current_model@parameters))[1:19,]
    data.frame(h2o.varimp(current_model))
    currentmodel_metrics<-cbind.data.frame(current_model@model$training_metrics@metrics$RMSE
                                ,current_model@model$validation_metrics@metrics$RMSE
                                ,current_model@model$training_metrics@metrics$RMSE-current_model@model$validation_metrics@metrics$RMSE
                                ,h2o.rmse(h2o.performance(current_model, newdata = test.amg_us_update))
                                ,h2o.rmse(h2o.performance(current_model, newdata = last30_us.h2o))
                          )
    dimnames(currentmodel_metrics)[[2]]<-c("training","validation","valdiff","newtest_rmse","last30_rmse") 
    
    best_update_metrics<-cbind.data.frame(best_update@model$training_metrics@metrics$RMSE
                              ,best_update@model$validation_metrics@metrics$RMSE
                              ,best_update@model$training_metrics@metrics$RMSE-best_update@model$validation_metrics@metrics$RMSE
                              ,h2o.rmse(h2o.performance(best_update, newdata = test.amg_us_update))
                              ,h2o.rmse(h2o.performance(best_update, newdata = last30_us.h2o))
                        )
    dimnames(best_update_metrics)[[2]]<-c("training","validation","valdiff","newtest_rmse","last30_rmse") 
    
    model_metrics<-rbind(currentmodel_metrics,best_update_metrics)
    row.names(model_metrics)<-c("current","update-selection")
    model_metrics
    
    #Save best grid selected model
    mojo_gridversion_path<-paste("AMG_us_gridversion_"
                             ,substring(gsub("-","",max(input.df$sale_date,na.rm=T)),5,6)
                             ,substring(gsub("-","",max(input.df$sale_date,na.rm=T)),7,8)
                             ,substring(gsub("-","",max(input.df$sale_date,na.rm=T)),3,4),sep="")
    
    dir.create(paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_gridmodels\\",mojo_gridversion_path,sep=""))
    grid_modelfile <- h2o.download_mojo(best_update,path = paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_gridmodels\\",mojo_gridversion_path,sep=""), get_genmodel_jar=TRUE)
    
    h2o.saveModel(object=best_update, path=paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_gridmodels\\",mojo_gridversion_path,sep=""), force=TRUE)
    
    bestgrid_model<-h2o.loadModel(path=paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_gridmodels\\",mojo_gridversion_path,"\\",best_update@model_id,sep=""))
    
    data.frame(h2o.varimp(bestgrid_model)) 
    
    
    # fit final selected model using all data, adding monotonic constrant for vehicle grade.
    fullset.df<-rbind(input.df,last30.df)
    fullset_us.h2o <-as.h2o(fullset.df)
    names(fullset_us.h2o)<-tolower(names(fullset_us.h2o))
    maxdate<-max(fullset.df$sale_date)
    splits.amg_us_final <- h2o.splitFrame(
      data = fullset_us.h2o, 
      ratios = c(0.8,0.10),   ## only need to specify 2 fractions, the 3rd is implied
      destination_frames = c("fulltrain_us.hex", "fullvalid_us.hex", "fulltest_us.hex"), seed = 3791
    )
    train.amg_us_final <-  splits.amg_us_final[[1]]
    valid.amg_us_final <-  splits.amg_us_final[[2]]
    test.amg_us_final <-  splits.amg_us_final[[3]]
    
    finalst<-Sys.time()
    
    final_us_model<-h2o.gbm(x = predictors_vars, 
                         y = response_target, 
                         max_depth = best_update@allparameters$max_depth,
                         ntrees=  best_update@allparameters$ntrees,
                         sample_rate = best_update@allparameters$sample_rate, 
                         col_sample_rate = best_update@allparameters$col_sample_rate,                                         
                         col_sample_rate_change_per_level = best_update@allparameters$col_sample_rate_change_per_level, 
                         min_rows = best_update@allparameters$min_rows,
                         nbins = best_update@allparameters$nbins,        
                         nbins_cats = best_update@allparameters$nbins_cats,    
                         min_split_improvement = best_update@allparameters$min_split_improvement,    
                         histogram_type = best_update@allparameters$histogram_type,
                         categorical_encoding = best_update@allparameters$categorical_encoding,
                         monotone_constraints = list(vehicle_grade = 1),
                         distribution = best_update@allparameters$distribution,
                         seed = 1251,
                         learn_rate =  best_update@allparameters$learn_rate,
                         model_id = paste("AMG_US_model_",gsub("-","",max(fullset.df$sale_date,na.rm=T)),sep=""),
                         training_frame = train.amg_us_final, 
                         validation_frame = valid.amg_us_final
                         
    )  
    
    
    data.frame(h2o.varimp(final_us_model)) 
    plot(final_us_model)
    print(paste("FINAL MODEL TIME",Sys.time()-finalst,sep='=')) 
    
    print(h2o.rmse(h2o.performance(final_us_model, newdata = test.amg_us_final)))
    
   #Save final selected model
    mojo_model_path<-paste("AMG_US_model_",gsub("-","",max(fullset.df$sale_date,na.rm=T)),sep="")
    mojo_version_path<-paste("AMG_US_version_"
                             ,substring(gsub("-","",max(fullset.df$sale_date,na.rm=T)),5,6)
                             ,substring(gsub("-","",max(fullset.df$sale_date,na.rm=T)),7,8)
                             ,substring(gsub("-","",max(fullset.df$sale_date,na.rm=T)),3,4),sep="")
    
    dir.create(paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,sep=""))
    final_us_modelfile <- h2o.download_mojo(final_us_model,path = paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,sep=""), get_genmodel_jar=TRUE)
    
    h2o.saveModel(object=final_us_model, path=paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,sep=""), force=TRUE)
    
    final_update_metrics<-cbind.data.frame(final_us_model@model$training_metrics@metrics$RMSE
                                          ,final_us_model@model$validation_metrics@metrics$RMSE
                                          ,final_us_model@model$training_metrics@metrics$RMSE-final_us_model@model$validation_metrics@metrics$RMSE
                                          ,h2o.rmse(h2o.performance(final_us_model, newdata = test.amg_us_update))
                                          ,h2o.rmse(h2o.performance(final_us_model, newdata = last30_us.h2o))
    )
    dimnames(final_update_metrics)[[2]]<-c("training","validation","valdiff","newtest_rmse","last30_rmse") 
    
    model_metrics<-rbind(model_metrics,final_update_metrics)
    row.names(model_metrics)<-c("current","update-selection","final-update")
    model_metrics
    
    }    
  
  #Model Metrics (MM results)
  MM_results=TRUE;if(MM_results==TRUE){
    
     
    mojo_model_path<-paste("AMG_US_model_",gsub("-","",max(fullset.df$sale_date,na.rm=T)),sep="")
    mojo_version_path<-paste("AMG_US_version_"
                             ,substring(gsub("-","",max(fullset.df$sale_date,na.rm=T)),5,6)
                             ,substring(gsub("-","",max(fullset.df$sale_date,na.rm=T)),7,8)
                             ,substring(gsub("-","",max(fullset.df$sale_date,na.rm=T)),3,4),sep="")
    
    #current
    currentversionpath <- dir("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\")
    currentversionpath <- first(tail(currentversionpath[grep("AMG_",currentversionpath)],2))
    
    currentmodelpath<-paste("AMG_US_model_20"
                            ,substring(currentversionpath,20,21)
                            ,substring(currentversionpath,16,17)
                            ,substring(currentversionpath,18,19),sep="")
    
    
    current_model<-h2o.loadModel(path=paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",currentversionpath,"\\",currentmodelpath,sep=""))
    
    #final
    final_model<-h2o.loadModel(path=paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,"\\",mojo_model_path,sep=""))
    
  
    #predict over last 90
    last90<-subset(fullset.df,as.Date(sale_date) >= as.Date(Sys.Date())-90)
    last90$pred_ratio<-h2o.mojo_predict_df(last90
                                         ,mojo_zip_path = paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,"\\",mojo_model_path,".zip",sep="")
                                         ,genmodel_jar_path = paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,"\\h2o-genmodel.jar",sep="")
                                         )$predict
    last90$amg_price<-round(last90$pred_ratio*last90$msrp,0)
    
    
  library(dplyr)
  amg_overall<-data.frame(last90 %>% filter(amg_price>1000 & sale_price>1000) %>%
                           summarise(
                              AMG = round(mean(amg_price,na.rm=T),0)
                             ,SALE_PRICE = round(mean(sale_price,na.rm=T),0)
                             ,MPE = round(mean((amg_price/sale_price)-1,na.rm=T),3)
                             ,MAPE = round(mean(abs(amg_price-sale_price)/sale_price,na.rm=T),3)
                             ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),3)
                             ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                             ,N = length(unique(vin))
                           ))
                    
  
  
  amg_make<-data.frame(last90 %>% filter(amg_price>1000 & sale_price>1000) %>%
                         dplyr::group_by(make_nm) %>%
                         summarise(
                           AMG = round(mean(amg_price,na.rm=T),0)
                           ,SALE_PRICE = round(mean(sale_price,na.rm=T),0)
                           ,MPE = round(mean((amg_price/sale_price)-1,na.rm=T),3)
                           ,MAPE = round(mean(abs(amg_price-sale_price)/sale_price,na.rm=T),3)
                           ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),3)
                           ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                           ,N = length(unique(vin))
                         ))
  
 
  n_mmys<-tail(sort(table(last90$mmy)),50)          
  amg_mmy<-data.frame(last90 %>% filter(mmy %in% names(n_mmys)))
  amg_mmy<-data.frame(amg_mmy %>%
               dplyr::group_by(mmy) %>%
                 summarise(
                   AMG = round(mean(amg_price,na.rm=T),0)
                   ,SALE_PRICE = round(mean(sale_price,na.rm=T),0)
                   ,MPE = round(mean((amg_price/sale_price)-1,na.rm=T),3)
                   ,MAPE = round(mean(abs(amg_price-sale_price)/sale_price,na.rm=T),3)
                   ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),3)
                   ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                   ,N = length(unique(vin))
                 ) %>% arrange(desc(MAD))
             )
  
  
 amg_trim<-data.frame(last90 %>% filter(mmt %in% names(tail(sort(table(last90$mmt)),50))) %>%
                        dplyr::group_by(model_nm,mmt) %>%
                        summarise(
                          AMG = round(mean(amg_price,na.rm=T),0)
                          ,SALE_PRICE = round(mean(sale_price,na.rm=T),0)
                          ,MPE = round(mean((amg_price/sale_price)-1,na.rm=T),3)
                          ,MAPE = round(mean(abs(amg_price-sale_price)/sale_price,na.rm=T),3)
                          ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),3)
                          ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                          ,N = length(unique(vin))
                          ,AVG_MILEAGE = round(mean(mileage,na.rm=T),0)
                          ,AVG_GRADE = round(mean(vehicle_grade,na.rm=T),1)
                          ,AVG_MSRP = round(mean(msrp,na.rm=T),0)
                        ) %>% arrange(model_nm,desc(MAD))
  )
  
  amg_grade<-data.frame(last90 %>% filter(amg_price>1000 & sale_price>1000) %>%
                          dplyr::group_by(floor(vehicle_grade)) %>%
                          summarise(
                            AMG = round(mean(amg_price,na.rm=T),0)
                            ,SALE_PRICE = round(mean(sale_price,na.rm=T),0)
                            ,MPE = round(mean((amg_price/sale_price)-1,na.rm=T),3)
                            ,MAPE = round(mean(abs(amg_price-sale_price)/sale_price,na.rm=T),3)
                            ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),3)
                            ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                            ,N = length(unique(vin))
                          ) %>%
            mutate(CHANGE = c(NA,diff(AMG))))
  
  amg_region<-data.frame(last90 %>% filter(amg_price>1000 & sale_price>1000) %>%
                          dplyr::group_by(analytical_region) %>%
                           summarise(
                             AMG = round(mean(amg_price,na.rm=T),0)
                             ,SALE_PRICE = round(mean(sale_price,na.rm=T),0)
                             ,MPE = round(mean((amg_price/sale_price)-1,na.rm=T),3)
                             ,MAPE = round(mean(abs(amg_price-sale_price)/sale_price,na.rm=T),3)
                             ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),3)
                             ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                             ,N = length(unique(vin))
                           ))
  
  amg_class<-data.frame(last90 %>% filter(amg_price>1000 & sale_price>1000) %>%
                          dplyr::group_by(market_class) %>%
                          summarise(
                            AMG = round(mean(amg_price,na.rm=T),0)
                            ,SALE_PRICE = round(mean(sale_price,na.rm=T),0)
                            ,MPE = round(mean((amg_price/sale_price)-1,na.rm=T),3)
                            ,MAPE = round(mean(abs(amg_price-sale_price)/sale_price,na.rm=T),3)
                            ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),3)
                            ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                            ,N = length(unique(vin))
                          ))
  
  amg_my<-data.frame(last90 %>% filter(amg_price>1000 & sale_price>1000) %>%
                          dplyr::group_by(my) %>%
                       summarise(
                         AMG = round(mean(amg_price,na.rm=T),0)
                         ,SALE_PRICE = round(mean(sale_price,na.rm=T),0)
                         ,MPE = round(mean((amg_price/sale_price)-1,na.rm=T),3)
                         ,MAPE = round(mean(abs(amg_price-sale_price)/sale_price,na.rm=T),3)
                         ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),3)
                         ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                         ,N = length(unique(vin))
                       ))
  
  amg_monthly_vins<-data.frame(fullset.df %>% 
                         dplyr::group_by(salemonth) %>%
                         summarise(
                             AGE = round(mean(age,na.rm=T),0),
                             GRADE = round(mean(vehicle_grade,na.rm=T),1),
                             MILES = round(mean(mileage),0),
                             N = length(unique(vin))
                         ))
  
  
  }
 
  # BEGIN: Markdown
  MARKDOWN=TRUE;if(MARKDOWN == T){
  library(markdown)
  markdown_path<- "F:/Projects/Adesa_Market_Guide/SNOWFLAKE_models/AMG_EKS_model_rmd.rmd"
  Sys.setenv(RSTUDIO_PANDOC="C:/Program Files/RStudio/bin/pandoc")
  
  rmarkdown::render(markdown_path,  
                    output_file =  paste("AMG_ModelUpdate_Report_",as.Date(Sys.Date()),".html", sep='') 
                    ,output_dir = "F:/Projects/Adesa_Market_Guide/SNOWFLAKE_models/AMG_updates/"
                    ,output_format = c("html_document"))
  
  
  
  
}
  # END: Markdown

  #shutdown H2O connection
  h2o.shutdown(prompt = FALSE)
  print("END: PRICING") 
}
  # END: PRICING ALGO
}
# END: PRICING MODEL


print("************ END NEW ALGO RUN ****************")   
endtime<-difftime(Sys.time(),mastertime)
print("OVERALL RUN TIME = ")
print(endtime)

#Schedule task
ST=FALSE;if(ST==TRUE){
  taskscheduler_create(taskname = "AMG_EKS_modelload"
                       , rscript = "F:/Projects/Adesa_Market_Guide/SNOWFLAKE_models//AMG_US_EKS_modelupdate.R"
                       , schedule = "WEEKLY"
                       , starttime = "22:00"
                       , startdate = format(Sys.Date(), "%m/%d/%Y")
                       , days =  c("MON","WED","FRI","SUN"))
}

