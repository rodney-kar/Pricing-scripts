print("************ START NEW ALGO RUN ****************")
mastertime<-Sys.time()
print(mastertime)
print(gc())

# BEGIN: LIBRARIES, which are required for all code in this script
SHOWLIB<-TRUE;if (SHOWLIB==TRUE){
  #Add libraries/source internal functions
  library(RODBC)
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
  library(rmarkdown)
  library(keyring)

  options(error=NULL)
  # .rs.restartR() # Restart R session
  # h2o.shutdown(prompt = FALSE)
  # h2o.removeAll() remove all h2o objects
  SNOWFLAKE_dbConnection<-odbcConnect("snowflake_azure", pwd = key_get('SNOWFLAKE'))


}
# END:   LIBRARIES

#BEGIN: LOADDATA
LOADDATA<-TRUE;if(LOADDATA==TRUE){

canada.sql<-function(){"WITH
    input_data AS
        (
            SELECT
                -- request.row_id AS row_id,
                -- from request
                request.sale_date                                                                                      AS sale_date,
                request.floor_price                                                                                    AS floor_price,
                request.sale_price                                                                                     AS sale_price,
                DATE_TRUNC('week', request.sale_date)::DATE                                                            AS sale_week,
                EXTRACT(MONTH FROM request.sale_date)::TEXT                                                            AS seasonal,
                request.vin                                                                                            AS vin,
                request.country                                                                                        AS country,
                request.vin8_10                                                                                        AS vin8_10,
                request.odometer_km                                                                                    AS odometer_km,
                SUBSTRING(request.zipcode, 1, 1)                                                                       AS region,
                (FLOOR((request.odometer_km::FLOAT * 0.621372) / 10000) * 10000)::INT                                  AS mileage_bin,
                request.vehicle_grade::NUMERIC(2,1)                                                                    AS vehicle_grade,
                -- private, adesa.ca, physical, traderev
                request.marketvenue                                                                                    AS marketvenue,
                -- from mvl/chrome
                COALESCE(request.make, v8.MODE_BEST_MAKE_NAME)                                                         AS make,
                COALESCE(request.model, v8.MODE_BEST_MODEL_NAME)                                                       AS model,
                COALESCE(request.my, v8.MODEL_YEAR)                                                                    AS my,
                COALESCE(request.make, v8.mode_best_make_name::text) || ':' || COALESCE(request.model,
                                                                                        v8.mode_best_model_name::text) AS mm,
                COALESCE(request.make, v8.mode_best_make_name::text) || ':' || COALESCE(request.model,
                                                                                        v8.mode_best_model_name::text) ||
                ':' || COALESCE(request.my, v8.model_year::text)                                                       AS
                                                                                                                          mmy,
                request.trim                                                                                           AS trim,
                request.input_trim                                                                                     AS input_trim,
                DATEDIFF(MONTHS,
                         ((COALESCE(request.my, v8.model_year)::INT - 1)::TEXT || '-09-01')::DATE,
                         request.sale_date::DATE
                    )                                                                                                  AS age,
                COALESCE(request.cylinders, v8.MODE_CYLINDERS)                                                         AS mode_cylinders,
                COALESCE(request.drivingwheels, 'UNKNOWN')                                                             AS mode_drivingwheels,
                COALESCE(request.displ_liters, v8.MODE_DISPL_LITERS)                                                   AS displ_liters,
                -- TO DO factor
                (
                    CASE
                        WHEN ROUND(COALESCE(request.displ_liters::NUMERIC(3,1), v8.mode_displ_liters::NUMERIC(3,1)), 1) > 20
                            THEN '0'::TEXT
                        WHEN ROUND(COALESCE(request.displ_liters::NUMERIC(3,1), v8.mode_displ_liters::NUMERIC(3,1)), 0) =
                             COALESCE(request.displ_liters::NUMERIC(3,1), v8.mode_displ_liters::NUMERIC(3,1))
                            THEN COALESCE(request.displ_liters::NUMERIC(3,1), v8.mode_displ_liters::NUMERIC(3,1))::FLOAT::INT::TEXT
                        ELSE ROUND(COALESCE(request.displ_liters::NUMERIC(3,1), v8.mode_displ_liters::NUMERIC(3,1)), 1)::TEXT
                        END)::TEXT                                                                                     AS mode_disp_factor,
                COALESCE(request.forced_induction_type, 'STANDARD')                                                    AS mode_forced_induction_type,
                COALESCE(request.body_type, v8.MODE_BODY_TYPE)                                                         AS mode_bodytype,
                COALESCE(request.marketclassname, v8.MODE_MARKET_CLASS_NAME)                                           AS mode_marketclassname,
                COALESCE(request.fueltype, v8.MODE_FUEL_TYPE)                                                          AS mode_fueltype,
                COALESCE(request.basemsrphigh, v8.AVG_BASE_MSRP_HIGH,
                         mmy_msrp.mmy_avg_msrp)                                                                        AS msrp,
                request.host_auction                                                                                   AS host_auction,
                request.gross_payoff                                                                                   AS gross_payoff,
                request.residual_price                                                                                 AS residual_price
            FROM
                (-- template here
                    SELECT
                        wc.sale_date::DATE                                                                                                         AS sale_date,
                        wc.floor_price::FLOAT                                                                                                      AS floor_price,
                        wc.sale_price::FLOAT                                                                                                       AS sale_price,
                        UPPER(wc.vin::text)                                                                                                        AS vin,
                        SUBSTRING(UPPER(wc.vin::text), 1, 8) || '_' || SUBSTRING(UPPER(wc.vin::text), 10, 1
                            )                                                                                                                      AS vin8_10,
                        wc.odometer_km::INT                                                                                                        AS odometer_km,
                        UPPER(COALESCE(wc.AUCTION_POSTAL_CODE_FULL::text,
                                       wc.SELLER_POSTAL_CODE_FULL::text))                                                                          AS zipcode,
                        wc.AUTOGRADE::text                                                                                                         AS vehicle_grade,
                        wc.host_auction                                                                                                            AS host_auction,
                        CASE
                            WHEN UPPER(wc.host_auction) IN
                                 ('ADESA TORONTO', 'ADESA MONTREAL', 'ADESA VANCOUVER', 'ADESA CALGARY',
                                  'ADESA EDMONTON', 'ADESA OTTAWA', 'ADESA KITCHENER',
                                  'ADESA WINNIPEG', 'ADESA HALIFAX', 'ADESA QUEBEC CITY', 'ADESA SASKATOON',
                                  'ADESA MONCTON', 'ADESA ST. JOHN\\'S', 'ADESA WINDSOR')
                                THEN 'PHYSICAL'
                            WHEN UPPER(wc.host_auction) = 'TRADEREV'
                                THEN 'TRADEREV'
                            WHEN UPPER(wc.host_auction) = 'ADESA.CA'
                                THEN 'ADESA.CA'
                            WHEN UPPER(wc.host_auction) = 'PRIVATE'
                                THEN 'PRIVATE'
                                ELSE
                                'PRIVATE'
                            END
                                                                                                                                                   AS marketvenue,
                        -- mvl/chrome sourced
                        UPPER(mvl.make::text)                                                                                                      AS make,
                        UPPER(mvl.model::text)                                                                                                     AS model,
                        UPPER(mvl.year::text)                                                                                                      AS my,
                        UPPER(mvl.trim::text)                                                                                                      AS trim,
                        UPPER(mvl.country::text)                                                                                                   AS country,
                        UPPER(mvl.cylinders::text)                                                                                                 AS cylinders,
                        UPPER(mvl.drivingwheels::text)                                                                                             AS drivingwheels,
                        UPPER(mvl.disp_liters::text)                                                                                               AS displ_liters,
                        UPPER(mvl.forced_induction_type::text)                                                                                     AS forced_induction_type,
                        UPPER(mvl.bodytype::text)                                                                                                  AS body_type,
                        UPPER(mvl.marketclassname::text)                                                                                           AS marketclassname,
                        UPPER(mvl.fueltype::text)                                                                                                  AS fueltype,
                        mvl.basemsrphigh::FLOAT                                                                                                    AS basemsrphigh,
                        COALESCE(wc.SERIES, tr.trim_level)                                                                                         AS input_trim,
                        lai.gross_payoff                                                                                                           AS gross_payoff,
                        li.residual_price                                                                                                          AS residual_price,
                        ROW_NUMBER() OVER (PARTITION BY wc.sale_date, wc.vin ORDER BY EDITDISTANCE(UPPER(mvl.trim),
                                                                                                   UPPER(COALESCE(wc.series, tr.trim_level))) ASC) AS edit_filter
                    FROM
                        ADESA_SHARE.CERTIFIED_ANALYTICS.WHOLE_CAR_OFFERED_AND_SOLD wc
                            LEFT JOIN
                                      adesa_share.src_openlane.vw_adprod_lease_infos li
                                      ON
                                      li.VEHICLE_ID = wc.VEHICLE_ID
                            LEFT JOIN
                                      ADESA_SHARE.SRC_OPENLANE.VW_ADPROD_LEASE_ADDITIONAL_INFOS lai
                                      ON
                                      lai.VEHICLE_ID = wc.VEHICLE_ID
                            LEFT JOIN
                                      (SELECT DISTINCT
                                           vin,
                                           UPPER(trim_level)                                            AS trim_level,
                                           ROW_NUMBER() OVER (PARTITION BY vin ORDER BY date_sold DESC) AS filt
                                       FROM
                                           prod.dwh.DIM_TRADE
                                           QUALIFY filt = 1
                                      )                                            tr
                                          ON tr.vin = wc.vin
                            LEFT JOIN (SELECT
                                        SUBSTRING(vin::TEXT, 1, 8) || '_' || SUBSTRING(vin::TEXT, 10, 1)::TEXT vin8_10,
                                        trim,
                                        country,
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
                                        WHERE COUNTRY = 'CA'
                                        GROUP BY
                                        1,2,3
                                )    mvl
                                          ON
                                                      mvl.vin8_10 =
                                                      (SUBSTRING(wc.vin::TEXT, 1, 8) || '_' ||
                                                       SUBSTRING(wc.vin::TEXT, 10, 1))::TEXT
                                                  AND
                                                (
                                                  /* 4 backslashes in R/Python 2 backslash as direct SQL */
                                                      REGEXP_INSTR(
                                                              REGEXP_REPLACE(UPPER(mvl.trim), '[^a-zA-Z0-9 - ./]', ' '),
                                                              '\\\\b' ||
                                                              REGEXP_REPLACE(UPPER(COALESCE(wc.series, tr.trim_level)), '[^a-zA-Z0-9 - ./]', ' ') ||
                                                              '\\\\b') > 0
                                                OR
                                                  /* 4 backslashes in R/Python 2 backslash as direct SQL */
                                                      REGEXP_INSTR(
                                                              REGEXP_REPLACE(UPPER(COALESCE(wc.series, tr.trim_level)), '[^a-zA-Z0-9 - ./]', ' '),
                                                              '\\\\b' ||
                                                              REGEXP_REPLACE(UPPER(mvl.trim), '[^a-zA-Z0-9 - ./]', ' ') ||
                                                              '\\\\b') > 0
                                                )


                    WHERE
                      wc.SALE_DATE BETWEEN CURRENT_DATE - INTERVAL '3 years' AND CURRENT_DATE
                      AND wc.AUCTION_COUNTRY = 'CANADA'
                      AND wc.sale_price > 1000
                      AND wc.odometer_km BETWEEN 1000 AND 400000
                      AND LENGTH(wc.vin) = 17
                      AND LOWER(wc.transaction_type) = 'sold'
                      AND ( wc.SALE_CLASS <> 'Closed - Grounding Dealer' OR wc.SALE_CLASS is null)
                        QUALIFY edit_filter = 1
                )                                   request
                    LEFT JOIN
                    (
                        SELECT
                            model_year,
                            mode_best_make_name,
                            mode_best_model_name,
                            AVG(avg_base_msrp_high::int) AS mmy_avg_msrp
                        FROM
                            prod.stg_prc_ftr.V_CHROME_VIN8_10_CA
                        GROUP BY
                            model_year,
                            mode_best_make_name,
                            mode_best_model_name)   mmy_msrp
                        ON
                                model_year::INT = request.my::INT
                            AND mmy_msrp.mode_best_make_name = request.make
                            AND mmy_msrp.mode_best_model_name = request.model
                    LEFT JOIN
                   prod.stg_prc_ftr.V_CHROME_VIN8_10_CA v8
                        ON
                        request.vin8_10 = v8.vin8_10
        )
SELECT
    id.vin,
    id.vin8_10,
    id.odometer_km,
    id.country,
    -- pulling through for reference
    id.host_auction,
    id.marketvenue,
    id.mm,
    id.mmy,
    id.trim,
    -- trim that was used to fit to trim
    -- id.trim is to be used for modeling
    -- id.input_trim just for reference
    id.input_trim,
    id.floor_price,
    id.sale_price,
    id.sale_date,
    id.sale_week,
    id.age,
    id.mileage_bin,
    -- from chrome/mvl call
    id.make,
    id.model,
    id.my,
    id.mode_cylinders,
    id.mode_drivingwheels,
    id.mode_disp_factor,
    id.mode_forced_induction_type,
    id.mode_bodytype,
    id.mode_marketclassname,
    id.mode_fueltype,
    -- substring on zipcode
    id.region,
    -- sale_date derived
    id.seasonal,
    -- vehicle_grade
    ROUND(COALESCE(id.vehicle_grade::FLOAT, ROUND(ge.vehicle_grade,1)::FLOAT),1)::FLOAT AS vehicle_grade,
    -- binary on grade estimate
    (
        CASE
            WHEN id.vehicle_grade IS NULL
                THEN 'TRUE'
                ELSE 'FALSE'
            END)                                                                   AS grade_fill,
    -- etl sourced
    -- depreciation
    CASE
        WHEN id.age > 239
            THEN md.dcurve_min
            ELSE dep.dcurve
        END                                                                        AS dcurve,
    -- historical averages
    ha.historical_price,
    (
        CASE
            WHEN ha.historical_mileage IS NOT NULL
                AND ha.historical_mileage > 0
                THEN id.odometer_km::FLOAT / ha.historical_mileage::FLOAT
                ELSE NULL
            END)                                                                   AS historical_sm_ratio,
    ha.sold                                                                        AS weekly_sold,
    ha.total_sold,
    -- grade_estimate
    ge.vehicle_grade                                                               AS grade_estimate,
    -- fred
    f.dexcaus_lag1,
    f.cauisa_lag4,
 --   1::boolean                                                                   AS send_price,
 --  ''                                                                            AS message,
    id.residual_price                                                              AS residual_price,
    id.gross_payoff                                                                AS gross_payoff,
    id.msrp                                                                        AS msrp,
    id.sale_price / id.msrp                                                        AS sp_ratio
FROM
    input_data                               id
        LEFT JOIN
        prod.stg_prc_ftr.V_HISTORICAL_AVERAGES_CA ha
            ON
                    ha.mmy = id.mmy
                AND ha.sale_week_end = id.sale_week
        LEFT JOIN
        prod.stg_prc_ftr.V_GRADE_ESTIMATE         ge
            ON
                    id.vin8_10 = ge.vin8_10
                AND ge.model_key = id.mm
                AND ge.mileage_bin = id.mileage_bin
        LEFT JOIN
       prod.stg_prc_ftr.V_DEPRECIATION           dep
            ON
                    dep.makemodel = id.mm
                AND dep.age = id.age
        LEFT JOIN
        (
            SELECT
                makemodel,
                MIN(dcurve) AS dcurve_min
            FROM
               prod.stg_prc_ftr.V_DEPRECIATION
            GROUP BY
                makemodel)                   md
            ON
            md.makemodel = id.mm
        LEFT JOIN
        prod.stg_prc_ftr.V_FRED_CA                f
            ON
            f.sale_week = id.sale_week
WHERE
        id.msrp <> 0
    AND id.age BETWEEN 0 and 239
    AND id.sale_price BETWEEN 1000 AND 500000
    AND dcurve IS NOT NULL
    AND id.sale_price / id.msrp BETWEEN 0 AND 1
    AND mode_marketclassname IS NOT NULL
    AND mode_disp_factor IS NOT NULL
    AND mode_bodytype IS NOT NULL
    AND mode_cylinders IS NOT NULL
    AND region IS NOT NULL
    AND historical_price > 1000
    AND ge.vehicle_grade BETWEEN 0 and 5
      "}
data.df<-sqlQuery(SNOWFLAKE_dbConnection,canada.sql(),stringsAsFactors=T)
names(data.df)<-tolower(names(data.df))
head(data.df)
summary(data.df)

input.df<-data.df %>% filter(as.Date(sale_date) < Sys.Date()- 30)
calast30.df<-data.df %>% filter(as.Date(sale_date) >= Sys.Date()- 30)

input.df$sale_date<-as.character(input.df$sale_date)
input.df$sale_week<-as.character(input.df$sale_week)
input.df$seasonal<-as.factor(input.df$seasonal)
input.df$mode_disp_factor<-as.factor(input.df$mode_disp_factor)

calast30.df$sale_date<-as.character(calast30.df$sale_date)
calast30.df$sale_week<-as.character(calast30.df$sale_week)
calast30.df$seasonal<-as.factor(calast30.df$seasonal)
calast30.df$mode_disp_factor<-as.factor(calast30.df$mode_disp_factor)
}
# END LOAD DATA

# BEGIN PRICING MODEL
PRICINGMODEL=TRUE;if(PRICINGMODEL==TRUE){


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

      connection_info

      h2o.init(ip = connection_info$ip[1], port = connection_info$port[1])
    }

    # connect
    start_h2o('cmg',mem_gb=60)
    h2o.removeAll()
    if(as.numeric(str_extract_all(capture.output(h2o.clusterInfo())[9],"\\(?[0-9,.]+\\)?")[[1]][2]) <= 50){h2o.shutdown(prompt=FALSE)} else{

    cmg.h2o <-as.h2o(input.df)
    names(cmg.h2o)<-tolower(names(cmg.h2o))

    calast30.h2o <-as.h2o(calast30.df)
    names(calast30.h2o)<-tolower(names(calast30.h2o))

    splits.cmg_update <- h2o.splitFrame(
      data = cmg.h2o,
      ratios = c(0.7,0.15),   ## only need to specify 2 fractions, the 3rd is implied
      destination_frames = c("cmg_train.hex", "cmg_valid.hex", "cmg_test.hex"),
      seed = 2022
    )
    train.cmg_update <- splits.cmg_update[[1]]
    valid.cmg_update <- splits.cmg_update[[2]]
    test.cmg_update  <- splits.cmg_update[[3]]

    response_target <- "sp_ratio"
    predictors_vars <- c('make','my'
                         ,'mode_cylinders','mode_drivingwheels','mode_disp_factor','mode_fueltype','mode_forced_induction_type'
                         ,'mode_bodytype','mode_marketclassname'
                         ,'seasonal'
                         ,'vehicle_grade','grade_fill'
                         ,'dcurve','odometer_km'
                         ,'historical_price','historical_sm_ratio'
                         ,'weekly_sold','total_sold'
                         ,'marketvenue','region'
                         ,'dexcaus_lag1','cauisa_lag4')

    default_model=FALSE;if(default_model==TRUE){
    gbm_train <- h2o.gbm(x = predictors_vars, y = response_target, training_frame = train.cmg_update)
    h2o.rmse(h2o.performance(gbm_train,valid.cmg_update))
    h2o.rmse(h2o.performance(gbm_train,test.cmg_update))
    h2o.rmse(h2o.performance(gbm_train,calast30.h2o))
    data.frame(h2o.varimp(gbm_train))
    }
    targetsearch=FALSE;if(targetsearch==TRUE){

      currentversionpath_cmg <- dir("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\")
      mojo_cmgversion_path<-last(currentversionpath_cmg[grep("CMG_",currentversionpath_cmg)])
      mojo_cmgmodel_path<-paste("CMG_model_20"
                                ,substring(mojo_cmgversion_path,17,18)
                                ,substring(mojo_cmgversion_path,13,14)
                                ,substring(mojo_cmgversion_path,15,16),sep="")
      s3modelfile_cmg<-paste("F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/h2o_models/",mojo_cmgversion_path,"/",mojo_cmgmodel_path,".zip",sep="")
      genmodelfile_cmg<-paste("F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/h2o_models/",mojo_cmgversion_path,"/h2o-genmodel.jar",sep="")
      currentupdate_cmg_model<-h2o.loadModel(path=paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_cmgversion_path,"\\",mojo_cmgmodel_path,sep=""))
      data.frame(h2o.varimp(currentupdate_cmg_model))


    cmg_target_model<-h2o.gbm(x = predictors_vars,
            y = response_target,
            max_depth = currentupdate_cmg_model@allparameters$max_depth,
            ntrees=  currentupdate_cmg_model@allparameters$ntrees,
            sample_rate = currentupdate_cmg_model@allparameters$sample_rate,
            col_sample_rate = currentupdate_cmg_model@allparameters$col_sample_rate,
            col_sample_rate_change_per_level = currentupdate_cmg_model@allparameters$col_sample_rate_change_per_level,
            min_rows = currentupdate_cmg_model@allparameters$min_rows,
            nbins = currentupdate_cmg_model@allparameters$nbins,
            nbins_cats = currentupdate_cmg_model@allparameters$nbins_cats,
            min_split_improvement = currentupdate_cmg_model@allparameters$min_split_improvement,
            histogram_type = currentupdate_cmg_model@allparameters$histogram_type,
            categorical_encoding = currentupdate_cmg_model@allparameters$categorical_encoding,
            monotone_constraints = list(vehicle_grade = 1),
            distribution = currentupdate_cmg_model@allparameters$distribution,
            seed = 1251,
            learn_rate = 0.05,
            model_id = paste("CMG_model_",gsub("-","",max(as.Date(calast30.df$sale_date),na.rm=T)),sep=""),
            training_frame = train.cmg_update,
            validation_frame = valid.cmg_update
    )
    h2o.rmse(h2o.performance(cmg_target_model,valid.cmg_update))
    h2o.rmse(h2o.performance(cmg_target_model,test.cmg_update))
    h2o.rmse(h2o.performance(cmg_target_model,calast30.h2o))
    data.frame(h2o.varimp(cmg_target_model))
    }

    #grid search
    canada_hypergrid<-h2o.grid(hyper_params = list(max_depth = seq(5,15,2),
                                                   ntrees=c(5:17)^3,
                                                   sample_rate = c(0.50, 0.60, 0.70, 0.80, 0.90, 1.00),
                                                   col_sample_rate =  c(seq(0.2,0.4,0.05),seq(0.45,0.75,0.1),seq(0.80,1,0.05)),
                                                   col_sample_rate_change_per_level = c(0.5,seq(0.9,1.1,0.05),1.5,2),
                                                   min_rows = 2^seq(4,10),
                                                   nbins = 2^seq(4,10),
                                                   nbins_cats = 2^seq(5,12),
                                                   min_split_improvement = c(1e-4),
                                                   histogram_type = c("QuantilesGlobal","RoundRobin"),
                                                   categorical_encoding = c("Enum"),
                                                   learn_rate = c(0.01, 0.1)),
                               search_criteria = list(strategy = "RandomDiscrete",
                                                      max_runtime_secs = 21600,
                                                      max_models = 50,
                                                      stopping_rounds = 3,
                                                      stopping_tolerance = 1e-3,
                                                      stopping_metric = "RMSE"),
                               algorithm="gbm",
                               nfolds=3,
                               keep_cross_validation_predictions = TRUE,
                               grid_id='CMG_hyper_grid',
                               x = predictors_vars,
                               y = response_target,
                               training_frame = train.cmg_update,
                               validation_frame = valid.cmg_update,
                               seed= 1111
                               #learn_rate_annealing=0.99

    )
    canada_sortedgrid <- h2o.getGrid('CMG_hyper_grid', sort_by="RMSE")


    if(length(canada_sortedgrid@model_ids)>=5){
      #selection
      model1<-h2o.getModel(canada_sortedgrid@model_ids[[1]])
      model2<-h2o.getModel(canada_sortedgrid@model_ids[[2]])
      model3<-h2o.getModel(canada_sortedgrid@model_ids[[3]])
      model4<-h2o.getModel(canada_sortedgrid@model_ids[[4]])
      model5<-h2o.getModel(canada_sortedgrid@model_ids[[5]])

      modelselection<-data.frame(c('model1','model2','model3','model4','model5'),rep(NA,5),rep(NA,5),rep(NA,5),rep(NA,5))
      names(modelselection)<-c('model_id','varimp','valdiff','rmse','last30')
      for(i in 1:5){
        modelselection$varimp[i]<-all(data.frame(h2o.varimp(h2o.getModel(canada_sortedgrid@model_ids[[i]])))$percentage < 0.5)[1]
        modelselection$valdiff[i]<-h2o.getModel(canada_sortedgrid@model_ids[[i]])@model$training_metrics@metrics$RMSE-h2o.getModel(canada_sortedgrid@model_ids[[i]])@model$validation_metrics@metrics$RMSE
        modelselection$rmse[i]<-h2o.rmse(h2o.performance(h2o.getModel(canada_sortedgrid@model_ids[[i]]), newdata = test.cmg_update))
        modelselection$last30[i]<-h2o.rmse(h2o.performance(h2o.getModel(canada_sortedgrid@model_ids[[i]]), newdata = calast30.h2o))
      }
      modelselection<-subset(modelselection,varimp==T & abs(valdiff)<=0.01)
      modelselection<- head(modelselection[order(modelselection$rmse),],1)
      as.numeric(row.names(modelselection))

      best_cmg_update <- h2o.getModel(canada_sortedgrid@model_ids[[as.numeric(row.names(modelselection))]])
      best_cmg_update@parameters
      data.frame(h2o.varimp(best_cmg_update))
    } else { best_cmg_update <- h2o.getModel(canada_sortedgrid@model_ids[[1]]) }

    #Save best grid model
    mojo_gridversion_path<-paste("CMG_gridversion_"
                                 ,substring(gsub("-","",max(input.df$sale_date,na.rm=T)),5,6)
                                 ,substring(gsub("-","",max(input.df$sale_date,na.rm=T)),7,8)
                                 ,substring(gsub("-","",max(input.df$sale_date,na.rm=T)),3,4),sep="")

    dir.create(paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_gridmodels",mojo_gridversion_path,sep=""))
    grid_modelfile <- h2o.download_mojo(best_cmg_update,path = paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_gridmodels",mojo_gridversion_path,sep=""), get_genmodel_jar=TRUE)
    h2o.saveModel(object=best_cmg_update, path=paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_gridmodels",mojo_gridversion_path,sep=""), force=TRUE)

    # fit final selected model using all data, adding monotonic constrant for vehicle grade.
    cmg.df<-rbind(input.df,calast30.df)
    cmg_fullset.h2o <-as.h2o(cmg.df)
    splits.cmg_final <- h2o.splitFrame(
      data = cmg_fullset.h2o,
      ratios = c(0.8,0.10),   ## only need to specify 2 fractions, the 3rd is implied
      destination_frames = c("cmg_fulltrain.hex", "cmg_fullvalid.hex", "cmg_fulltest.hex"), seed = 3791
    )
    train.cmg_final <-  splits.cmg_final[[1]]
    valid.cmg_final <-  splits.cmg_final[[2]]
    test.cmg_final <-  splits.cmg_final[[3]]


    final_cmg_model<-h2o.gbm(x = predictors_vars,
                         y = response_target,
                         max_depth = best_cmg_update@allparameters$max_depth,
                         ntrees=  best_cmg_update@allparameters$ntrees,
                         sample_rate = best_cmg_update@allparameters$sample_rate,
                         col_sample_rate = best_cmg_update@allparameters$col_sample_rate,
                         col_sample_rate_change_per_level = best_cmg_update@allparameters$col_sample_rate_change_per_level,
                         min_rows = best_cmg_update@allparameters$min_rows,
                         nbins = best_cmg_update@allparameters$nbins,
                         nbins_cats = best_cmg_update@allparameters$nbins_cats,
                         min_split_improvement = best_cmg_update@allparameters$min_split_improvement,
                         histogram_type = best_cmg_update@allparameters$histogram_type,
                         categorical_encoding = best_cmg_update@allparameters$categorical_encoding,
                         monotone_constraints = list(vehicle_grade = 1),
                         distribution = best_cmg_update@allparameters$distribution,
                         seed = 1111,
                         learn_rate =  best_cmg_update@allparameters$learn_rate,
                         model_id = paste("CMG_model_",gsub("-","",max(as.Date(cmg.df$sale_date),na.rm=T)),sep=""),
                         training_frame = train.cmg_final,
                         validation_frame = valid.cmg_final

    )
    data.frame(h2o.varimp(final_cmg_model))
    h2o.rmse(h2o.performance(final_cmg_model,valid.cmg_final))
    h2o.rmse(h2o.performance(final_cmg_model,test.cmg_final))

    #check final model results verses grid model
    if(round(final_cmg_model@model$validation_metrics@metrics$RMSE,3) > round(best_cmg_update@model$validation_metrics@metrics$RMSE,3)+0.005){
      print(paste("GRID MODEL: ", best_cmg_update@model$validation_metrics@metrics$RMSE,sep=""))
      print(paste("FINAL MODEL: ", final_cmg_model@model$validation_metrics@metrics$RMSE,sep=""))
      print("FINAL MODEL FAILED: CHECK LAST 30 DAYS")
    } else{

    #Save final selected model
    mojo_model_path<-paste("CMG_model_",gsub("-","",max(cmg.df$sale_date,na.rm=T)),sep="")
    mojo_version_path<-paste("CMG_version_"
                             ,substring(gsub("-","",max(cmg.df$sale_date,na.rm=T)),5,6)
                             ,substring(gsub("-","",max(cmg.df$sale_date,na.rm=T)),7,8)
                             ,substring(gsub("-","",max(cmg.df$sale_date,na.rm=T)),3,4),sep="")


    dir.create(paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,sep=""))
    final_modelfile <- h2o.download_mojo(final_cmg_model,path = paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,sep=""), get_genmodel_jar=TRUE)
    h2o.saveModel(object=final_cmg_model, path=paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_version_path,sep=""), force=TRUE)

    #best grid model used for rerun
    bgm=FALSE;if(bgm){
    best_cmg_update<-h2o.loadModel(path=paste("F:\\Projects\\Canadian_Market_Guide\\h2o_gridmodels\\"
          ,last(dir("F:\\Projects\\Canadian_Market_Guide\\h2o_gridmodels\\"))
          ,"\\"
          ,first(dir(paste("F:\\Projects\\Canadian_Market_Guide\\h2o_gridmodels\\"
          ,last(dir("F:\\Projects\\Canadian_Market_Guide\\h2o_gridmodels\\"))
          ,sep=""))),sep=""))
    final_cmg_model<-h2o.loadModel(path=paste("F:\\Projects\\Canadian_Market_Guide\\h2o_models\\"
          ,last(dir("F:\\Projects\\Canadian_Market_Guide\\h2o_models\\"))
          ,"\\"
          ,first(dir(paste("F:\\Projects\\Canadian_Market_Guide\\h2o_models\\"
          ,last(dir("F:\\Projects\\Canadian_Market_Guide\\h2o_models\\"))
          ,sep=""))),sep=""))
    }

    previousversionpath_cmg <- dir("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\")
    mojo_cmgversion_path<-head(tail(previousversionpath_cmg[grep("CMG_",previousversionpath_cmg)],2),1)
    mojo_cmgmodel_path<-paste("CMG_model_20"
                              ,substring(mojo_cmgversion_path,17,18)
                              ,substring(mojo_cmgversion_path,13,14)
                              ,substring(mojo_cmgversion_path,15,16),sep="")
    s3modelfile_cmg<-paste("F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/h2o_models/",mojo_cmgversion_path,"/",mojo_cmgmodel_path,".zip",sep="")
    genmodelfile_cmg<-paste("F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/h2o_models/",mojo_cmgversion_path,"/h2o-genmodel.jar",sep="")

    previous_update_cmg_model<-h2o.loadModel(path=paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_cmgversion_path,"\\",mojo_cmgmodel_path,sep=""))
    data.frame(h2o.varimp(previous_update_cmg_model))
    previous_update_cmg_model
    previous_cmg_modelmojo <- h2o.download_mojo(previous_update_cmg_model,path = paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_cmgversion_path,sep=""), get_genmodel_jar=TRUE)

    print(paste("PREVIOUS MODEL ",previous_update_cmg_model@model_id, previous_update_cmg_model@model$validation_metrics@metrics$RMSE,sep=":"))
    print(paste("FINAL MODEL ", final_cmg_model@model_id,final_cmg_model@model$validation_metrics@metrics$RMSE,sep=":"))

    previous_update_metrics<-cbind.data.frame(previous_update_cmg_model@model$training_metrics@metrics$RMSE
                                           ,previous_update_cmg_model@model$validation_metrics@metrics$RMSE
                                           ,previous_update_cmg_model@model$training_metrics@metrics$RMSE-previous_update_cmg_model@model$validation_metrics@metrics$RMSE
                                           ,h2o.rmse(h2o.performance(previous_update_cmg_model, newdata = test.cmg_final))
                                           ,h2o.rmse(h2o.performance(previous_update_cmg_model, newdata = calast30.h2o))
    )
    dimnames(previous_update_metrics)[[2]]<-c("training","validation","valdiff","newtest_rmse","last30_rmse")


    final_update_metrics<-cbind.data.frame(final_cmg_model@model$training_metrics@metrics$RMSE
                                           ,final_cmg_model@model$validation_metrics@metrics$RMSE
                                           ,final_cmg_model@model$training_metrics@metrics$RMSE-final_cmg_model@model$validation_metrics@metrics$RMSE
                                           ,h2o.rmse(h2o.performance(final_cmg_model, newdata = test.cmg_final))
                                           ,h2o.rmse(h2o.performance(final_cmg_model, newdata = calast30.h2o))
    )
    dimnames(final_update_metrics)[[2]]<-c("training","validation","valdiff","newtest_rmse","last30_rmse")

    model_metrics<-rbind(previous_update_metrics,final_update_metrics)
    row.names(model_metrics)<-c("previous_version","final-update")
    model_metrics

    #FULL Model  Metrics
    PRICE_RESULTS=TRUE;if(PRICE_RESULTS==TRUE){
    #import final model
    #library(h2o);h2o.init(max_mem_size = "100g",enable_assertions = FALSE)

    cmg_fullset.h2o$CMG_RATIO <- h2o.predict(final_cmg_model,  cmg_fullset.h2o)

    #testing
    #temp<-subset(cmg_fullset.df,vin=='WDBNG75J94A421919')
    #temp.h2o<-as.h2o(temp)
    #h2o.predict(model4,  temp.h2o)

    cmg_fullset.h2o$CMG_PRICE <-  cmg_fullset.h2o$CMG_RATIO *  cmg_fullset.h2o$msrp
    cmg_fullset.df<-as.data.frame(cmg_fullset.h2o)
    cmg_fullset.df$CMG_PRICE<-ifelse(cmg_fullset.df$CMG_PRICE< 300, 300, cmg_fullset.df$CMG_PRICE)


    test.cmg_final$CMG_RATIO <-  h2o.predict(final_cmg_model,  test.cmg_final)
    test.cmg_final$CMG_PRICE <- test.cmg_final$CMG_RATIO*test.cmg_final$msrp
    test.df<-as.data.frame(test.cmg_final)
    test.df$CMG_RATIO<-ifelse(test.df$CMG_PRICE< 300, 300, test.df$CMG_PRICE)

    calast30.h2o$CMG_RATIO <-  h2o.predict(best_cmg_update,  calast30.h2o)
    calast30.h2o$CMG_PRICE <-  calast30.h2o$CMG_RATIO*calast30.h2o$msrp
    calast30.df<-as.data.frame(calast30.h2o)
    calast30.df$CMG_RATIO<-ifelse(calast30.df$CMG_PRICE< 300, 300, calast30.df$CMG_PRICE)



    #TradeRev Adjustments
    TR_INSPECTION=TRUE;if(TR_INSPECTION==TRUE){
      SNOWFLAKE_dbConnection<-odbcConnect("snowflake_azure", pwd = key_get('SNOWFLAKE'))
      inspections.sql<-function(){paste("select distinct
    VIN as vin,
    SUBSTRING(VIN,1,8) ||'_'||SUBSTRING(VIN,10,1) as vin8_10,
    UPPER(MAKE) as make,
    UPPER(MODEL) as model,
    mileage as odometer_km,
    MODEL_YEAR as my,
    DATE_SOLD as sale_date,
    date_trunc('week',sale_date) as sale_week,
    LAST_DAY(DATE_SOLD) as salemonth,
    SALE_PRICE as sale_price,
    AIRBAG_CONDITION_M,
    ANTI_LOCK_BREAK_CONDITION_M,
    ODOMETER_CONDITION_M,
    TIRE_CONDITION_M,
    WINDSHIELD_CONDITION_M,
    STRUCTURAL_CONDITION_M,
    ACCIDENT_BRAND_M,
    AS_IS,
    IS_OPERABLE,
    IS_REPAINTED,
    HAS_REPORT,
    IS_VALIDVIN,
    AIR_BAGS_MISSING_INOPERABLE,
    AIR_CONDITIONING,
    AIR_CONDITIONING_NEEDS_REPAIR,
    COMPUTER_NEEDS_REPAIR,
    FIRE_DAMAGED,
    CAMVAP_OR_LEMON,
    ANTI_LOCK_BRAKES_INOPERABLE,
    PREVIOUS_DAMAGE_EXCEEDING_LIMIT,
    FUEL_SYSTEM_NEEDS_REPAIR,
    ELECTRICAL_SYSTEM_NEEDS_REPAIR,
    ENGINE_NEEDS_REPAIR,
    SUSPENSION_SUBFRAME_NEEDS_REPAIR,
    HAS_ACCIDENT,
    HAS_BOOKS,
    IMMERSED_IN_WATER,
    LIEN,
    ODOMETER_ROLLED_BACK_REPLACED,
    POWER_TRAIN_NEEDS_REPAIR,
    STRUCTURAL,
    THEFT_RECOVERY,
    TOTAL_LOSS_BY_INSURER,
    TRANSMISSION_NEEDS_REPAIR,
    HAS_TITLE_DEFECT,
    HAS_MODIFICATIONS,
    CAR_WILL_NOT_START,
    MISSING_EXTERIOR_BODY_PANELS,
    US_VEHICLE

  from prod.DWH.DIM_TRADE  DT
    where IS_SOLD=1
    and BI_SOURCE_COUNTRY='CA'
    and DT.DATE_SOLD BETWEEN CURRENT_DATE - INTERVAL '3 years' AND CURRENT_DATE",sep="")
      }
      inspections.df<-sqlQuery(SNOWFLAKE_dbConnection,inspections.sql(),stringsAsFactors=FALSE)
      names(inspections.df)<-tolower(names(inspections.df))
      inspections.df$odometer_km<-as.numeric(inspections.df$odometer_km)
      inspections.df$sale_price<-as.numeric(inspections.df$sale_price)
      inspections.df$sale_date<-as.character(as.Date(inspections.df$sale_date))


      #merge inspection data with TradeRev sales with CMG
      base<-subset(cmg_fullset.df,marketvenue=='TRADEREV' & !is.na(msrp))
      base$vin<-as.character(base$vin)
      TRCMG_INSP.df<-merge(inspections.df,base,
                           by=c('vin','vin8_10','make','model','my','sale_week','sale_date','sale_price','odometer_km'))
      dim(TRCMG_INSP.df)
      summary(TRCMG_INSP.df)
      TRCMG_INSP.df$sale_date<-as.Date(TRCMG_INSP.df$sale_date)
      TRCMG_INSP.df$sale_week<-as.character(TRCMG_INSP.df$sale_week)
      TRCMG_INSP.df$salemonth<-as.character(TRCMG_INSP.df$salemonth)
      str(TRCMG_INSP.df)

      INSPECTION_METRICS=TRUE;if(INSPECTION_METRICS==TRUE){

        #remove "has_title_defect" due to data issues
        insp.df<-subset(inspections.df,select=-c(has_title_defect,structural))
        #Look at Damage data by grade assumption and prediction error
        inspection_results_summary<-data.frame(c(rep(NA,30)),c(rep(NA,30))
                                               ,c(rep(NA,30)),c(rep(NA,30))
                                               ,c(rep(NA,30)),c(rep(NA,30))
        )
        for(i in 1:30){
          inspection_results_summary[i,]<-c(round(prop.table(table(insp.df[,(17+i)])),3)
                                            ,round(tapply(insp.df$sale_price,insp.df[,(17+i)],mean,na.rm=T),0)
                                            ,round(tapply(insp.df$odometer_km,insp.df[,(17+i)],mean,na.rm=T),0))
          rownames(inspection_results_summary)[i]<-names(insp.df)[(17+i)]

        }
        names(inspection_results_summary)<-c("NOT_PRESENT","PRESENT"
                                             ,"AVG_SALE_NOT_PRESENT","AVG_SALE_PRESENT"
                                             ,"AVG_MILEAGE_NOT_PRESENT","AVG_MILEAGE_PRESENT"
        )
        inspection_results_summary$PRICE_DIFF<-inspection_results_summary$AVG_SALE_NOT_PRESENT-inspection_results_summary$AVG_SALE_PRESENT
        inspection_results_summary<-inspection_results_summary[order(inspection_results_summary$PRICE_DIFF),]
        inspection_results_summary
      }

      INSPECTION_MODEL=TRUE;if(INSPECTION_MODEL==TRUE){
        TRCMG_INSP.df<-data.frame(TRCMG_INSP.df %>% group_by(vin,sale_date,sale_price) %>%
                                         mutate(
                                          adds = sum(has_modifications,
                                                        has_books,
                                                        is_operable,
                                                        has_report,
                                                        air_conditioning,
                                                        lien,na.rm=T),
                                          deducts = sum(is_repainted,
                                                       previous_damage_exceeding_limit,
                                                       electrical_system_needs_repair,
                                                       fuel_system_needs_repair,
                                                       has_accident,
                                                       suspension_subframe_needs_repair,
                                                       anti_lock_brakes_inoperable,
                                                       air_conditioning_needs_repair,
                                                       as_is,na.rm=T),
                                         majors = sum(computer_needs_repair,
                                                      odometer_rolled_back_replaced,
                                                      engine_needs_repair,
                                                      transmission_needs_repair,
                                                      power_train_needs_repair,na.rm=T),
                                         salvage = sum(immersed_in_water,
                                                       camvap_or_lemon,
                                                       theft_recovery,
                                                       structural,
                                                       fire_damaged,
                                                       car_will_not_start,
                                                       missing_exterior_body_panels,
                                                       total_loss_by_insurer,
                                                       air_bags_missing_inoperable,
                                                       has_title_defect,na.rm=T)
                                  )

      )



      nosalvage<-subset(TRCMG_INSP.df,salvage==0 |is.na(salvage))
      #refit damage adjustment values = FALSE to run uncomment.
      #grade_weights<-glm(sale_price~adds+deducts+majors,data=nosalvage)
      #add_adj<-round(1+(coef(grade_weights)["adds"]/coef(grade_weights)["(Intercept)"]),2)
      #deducts_adj<-round(1+(coef(grade_weights)["deducts"]/coef(grade_weights)["(Intercept)"]),2)
      #majors_adj<-round(1+(coef(grade_weights)["majors"]/coef(grade_weights)["(Intercept)"]),2)

      #fixed values
      add_adj = 1.16
      deducts_adj = 0.62
      majors_adj = 0.57


      TRCMG_INSP.df$salvage_adjustment<-ifelse(TRCMG_INSP.df$accident_brand_m %in% c('IRREPARABLE','REBUILD','SALVAGE') |
                                                 TRCMG_INSP.df$airbag_condition_m %in% c('INOPERABLE','MISSING') |
                                                 TRCMG_INSP.df$anti_lock_break_condition_m %in% c('INOPERABLE') |
                                                 TRCMG_INSP.df$odometer_condition_m %in% c('REPLACED','ROLLBACK') |
                                                 TRCMG_INSP.df$structural_condition_m %in% c('DAMAGED') |
                                                 as.numeric(TRCMG_INSP.df$salvage)>0,0.50,NA)

      TRCMG_INSP.df$major_adjustment<-ifelse(TRCMG_INSP.df$structural_condition_m %in% c('REPAIRED') |
                                               TRCMG_INSP.df$majors>0,majors_adj,NA)

      TRCMG_INSP.df$minor_adjustment<-ifelse(TRCMG_INSP.df$deducts>2,deducts_adj,NA)

      TRCMG_INSP.df$adds_adjustment<-ifelse(TRCMG_INSP.df$deducts==0 & TRCMG_INSP.df$adds>3,add_adj,1)


      TRCMG_INSP.df<-data.frame(TRCMG_INSP.df %>% group_by(vin,sale_date,sale_price) %>%
                                  mutate(grade_adjustment = min(salvage_adjustment,major_adjustment,minor_adjustment,adds_adjustment,na.rm=T))
      )

      #adjust estimated grade based on inspection adjustments
      TRCMG_INSP.df$adj_vehicle_grade<-TRCMG_INSP.df$vehicle_grade*TRCMG_INSP.df$grade_adjustment
      TRCMG_INSP.df$orignal_grade<-TRCMG_INSP.df$vehicle_grade
      TRCMG_INSP.df$vehicle_grade<-TRCMG_INSP.df$adj_vehicle_grade


      # ? convert all character variable to factor?
      TRCMG_INSP.df[sapply(TRCMG_INSP.df, is.character)] <- lapply(TRCMG_INSP.df[sapply(TRCMG_INSP.df, is.character)], as.factor)

      # ? convert all integer variable to numeric?
      #TRCMG_INSP.df[sapply(TRCMG_INSP.df, is.integer)] <- lapply(TRCMG_INSP.df[sapply(TRCMG_INSP.df, is.integer)], as.numeric)
      }

      # re-price with new grade
      TRCMG.h2o<-as.h2o(TRCMG_INSP.df)
      TRCMG.h2o$TRMG_RATIO <- h2o.predict(final_cmg_model, TRCMG.h2o)
      TRCMG.h2o$TRMG_PRICE <- TRCMG.h2o$TRMG_RATIO * TRCMG.h2o$msrp
      TRCMG.df<-as.data.frame(TRCMG.h2o)

      TR.metrics<-data.frame(TRCMG.df %>% #filter(as.Date(sale_date)>= min(as.Date(subset(calast30.df,marketvenue=='TRADEREV')$sale_date))) %>%
                               dplyr::group_by(marketvenue) %>%

                               summarise(
                                 AVG_AGE = round(mean(age,na.rm=T),2)
                                 ,AVG_MILES = round(mean(odometer_km,na.rm=T),0)
                                 ,PCT_FILL = round(sum(as.character(grade_fill)=='TRUE')/length(vin),2)
                                 ,AVG_GRADE = mean(vehicle_grade,na.rm=T)
                                 ,AVG_FLOOR = round(mean(floor_price,na.rm=T),0)
                                 ,AVG_HIST = round(mean(historical_price,na.rm=T),0)
                                 ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                                 ,AVG_TRMG = round(mean(TRMG_PRICE,na.rm=T),0)
                                 ,SALE = round(mean(sale_price),0)

                                 ,HP_MAD = round(mad((historical_price/sale_price)-1,na.rm=T),3)
                                 ,HP_MPE = round(mean((historical_price/sale_price)-1,na.rm=T),3)
                                 ,HP_RET = round(mean(sale_price/historical_price,na.rm=T),3)

                                 ,CMG_MAPE = round(mean(abs(CMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                 ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                 ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                 ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)

                                 ,TR_MAPE = round(mean(abs(TRMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                 ,TR_MAD = round(mad((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                 ,TR_MPE = round(mean((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                 ,TR_RET = round(mean(sale_price/TRMG_PRICE,na.rm=T),3)

                                 ,N= length(vin)
                               ))
      TR.metrics

      majormetrics<-TRCMG.df
      majormetrics$major_cat<-ifelse(majormetrics$majors>2,'3+',majormetrics$majors)

      byMajor_adj<-data.frame( majormetrics %>%
                                 dplyr::group_by(major_cat) %>%
                                 summarise(AVG_GRADE = mean(vehicle_grade,na.rm=T)
                                           ,SALE = round(mean(sale_price),0)
                                           ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                                           ,AVG_TRMG = round(mean(TRMG_PRICE,na.rm=T),0)

                                           ,CMG_MAPE = round(mean(abs(CMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                           ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                           ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                           ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)

                                           ,TRMG_MAPE = round(mean(abs(TRMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                           ,TRMG_MAD = round(mad((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                           ,TRMG_MPE = round(mean((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                           ,TRMG_RET = round(mean(sale_price/TRMG_PRICE,na.rm=T),3)
                                           ,N= length(vin)
                                 ))
      byMajor_adj
      bySalvage_adj<-data.frame( majormetrics %>%
                                   dplyr::group_by(salvage) %>%
                                   summarise(AVG_GRADE = mean(vehicle_grade,na.rm=T)
                                             ,SALE = round(mean(sale_price),0)
                                             ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                                             ,AVG_TRMG = round(mean(TRMG_PRICE,na.rm=T),0)

                                             ,CMG_MAPE = round(mean(abs(CMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                             ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                             ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                             ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)

                                             ,TRMG_MAPE = round(mean(abs(TRMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                             ,TRMG_MAD = round(mad((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                             ,TRMG_MPE = round(mean((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                             ,TRMG_RET = round(mean(sale_price/TRMG_PRICE,na.rm=T),3)
                                             ,N= length(vin)
                                   ))
      bySalvage_adj
      deductmetrics<-TRCMG.df
      deductmetrics$deduct_cat<-ifelse(deductmetrics$deduct>2,'3+',deductmetrics$deduct)

      byDeduct_adj<-data.frame(deductmetrics %>%
                                 dplyr::group_by(deduct_cat) %>%
                                 summarise(AVG_GRADE = mean(vehicle_grade,na.rm=T)
                                           ,SALE = round(mean(sale_price),0)
                                           ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                                           ,AVG_TRMG = round(mean(TRMG_PRICE,na.rm=T),0)

                                           ,CMG_MAPE = round(mean(abs(CMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                           ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                           ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                           ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)

                                           ,TRMG_MAPE = round(mean(abs(TRMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                           ,TRMG_MAD = round(mad((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                           ,TRMG_MPE = round(mean((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                           ,TRMG_RET = round(mean(sale_price/TRMG_PRICE,na.rm=T),3)
                                           ,N= length(vin)
                                 ))
      byDeduct_adj
      byAdds_adj<-data.frame(deductmetrics %>%
                               dplyr::group_by(adds) %>%
                               summarise(AVG_GRADE = mean(vehicle_grade,na.rm=T)
                                         ,SALE = round(mean(sale_price),0)
                                         ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                                         ,AVG_TRMG = round(mean(TRMG_PRICE,na.rm=T),0)

                                         ,CMG_MAPE = round(mean(abs(CMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                         ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                         ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                         ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)

                                         ,TRMG_MAPE = round(mean(abs(TRMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                         ,TRMG_MAD = round(mad((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                         ,TRMG_MPE = round(mean((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                         ,TRMG_RET = round(mean(sale_price/TRMG_PRICE,na.rm=T),3)
                                         ,N= length(vin)
                               ))
      byAdds_adj
      byTRGrade<-data.frame(TRCMG.df %>%
                              dplyr::group_by(floor(vehicle_grade)) %>%
                              summarise( AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                                         ,AVG_TRMG = round(mean(TRMG_PRICE,na.rm=T),0)

                                         ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                         ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                         ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)

                                         ,TRMG_MAD = round(mad((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                         ,TRMG_MPE = round(mean((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                                         ,TRMG_RET = round(mean(sale_price/TRMG_PRICE,na.rm=T),3)
                                         ,N= length(vin)
                              ))



        temp<-TRCMG.df
        temp$yage<-TRCMG.df$age/12
        temp$yage<-ifelse(temp$yage<6,'1-5'
                          ,ifelse(temp$yage<10,'6-9'
                                  ,ifelse(temp$yage<16,'10-15'
                                          ,ifelse(temp$yage>15,'16+',NA))))
        byAGEadjTR<-data.frame(temp %>%
                            dplyr::group_by(yage) %>%

                            summarise(
                              #    AVG_AGE = round(mean(age,na.rm=T),2)
                              #    ,AVG_MILES = round(mean(odometer_km,na.rm=T),0)
                              #    ,PCT_FILL = round(sum(as.character(grade_fill)=='T')/length(vin),2)
                              #    ,AVG_GRADE = mean(vehicle_grade,na.rm=T)
                              #     ,AVG_FLOOR = round(mean(floor_price,na.rm=T),0)
                              AVG_TRMG = round(mean(TRMG_PRICE,na.rm=T),0)
                              #      ,SALE = round(mean(sale_price),0)
                              ,TRMG_MAD = round(mad((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                              ,TRMG_MPE = round(mean((TRMG_PRICE/sale_price)-1,na.rm=T),3)
                              ,TRMG_RET = round(mean(sale_price/TRMG_PRICE,na.rm=T),3)
                              ,N= length(vin)
                            ) %>% filter(N>10) %>% arrange(desc(N)))


        last30TR.df<-subset(TRCMG.df,as.Date(sale_date)  >= Sys.Date()- 30)


}

    model_metrics=T;if(model_metrics==T){

      #investigate totals by marketvenue
      data_coverage<-data.frame(cmg_fullset.df %>% filter(sale_price>0 & sale_price<200000) %>% dplyr::group_by(marketvenue)
                                %>% summarise(TotalVINs = length(unique(vin)),
                                              MAKEs = length(unique(make)),
                                              MODELs =  length(unique(model)),
                                              Total_Days = length(unique(sale_date)),
                                              AVG_Age = round(mean(age,na.rm=T),0),
                                              AVG_KMs = round(mean(odometer_km,na.rm=T),0),
                                              AVG_Price = round(mean(sale_price),0)

                                ))





      CA.metrics<-data.frame( cmg_fullset.df %>% filter(!is.na(CMG_PRICE)) %>%
                               dplyr::group_by(marketvenue) %>%

                               summarise(
                                 AVG_AGE = round(mean(age,na.rm=T),2)
                                 ,AVG_MILES = round(mean(odometer_km,na.rm=T),0)
                                 ,PCT_FILL = round(sum(as.character(grade_fill)=='T')/length(vin),2)
                                 ,AVG_GRADE = mean(vehicle_grade,na.rm=T)
                                 ,AVG_FLOOR = round(mean(floor_price,na.rm=T),0)
                                 ,AVG_HIST = round(mean(historical_price,na.rm=T),0)
                                 ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                                 ,SALE = round(mean(sale_price),0)

                                 ,HP_MAD = round(mad((historical_price/sale_price)-1,na.rm=T),3)
                                 ,HP_MPE = round(mean((historical_price/sale_price)-1,na.rm=T),3)
                                 ,HP_RET = round(mean(sale_price/historical_price,na.rm=T),3)

                                 ,CMG_MAPE = round(mean(abs(CMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                 ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                 ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                 ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)

                                 ,N= length(vin)
                               ))
      CA.metrics #%>% filter(as.Date(salemonth) > as.Date('2021-01-01'))

      byGRADE<-data.frame( cmg_fullset.df %>%
                            dplyr::group_by(marketvenue,floor(vehicle_grade)) %>%

                            summarise(
                              #    AVG_AGE = round(mean(age,na.rm=T),2)
                              #    ,AVG_MILES = round(mean(odometer_km,na.rm=T),0)
                              #    ,PCT_FILL = round(sum(as.character(grade_fill)=='T')/length(vin),2)
                              #    ,AVG_GRADE = mean(vehicle_grade,na.rm=T)
                              #     ,AVG_FLOOR = round(mean(floor_price,na.rm=T),0)
                              AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                              #      ,SALE = round(mean(sale_price),0)
                              ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                              ,AMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                              ,AMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)
                              ,N= length(vin)
                            ) %>% filter(N>10) %>% arrange(desc('floor.vehicle_grade.')))


      temp<-cmg_fullset.df
      temp$yage<-cmg_fullset.df$age/12
      temp$yage<-ifelse(temp$yage<6,'1-5'
                        ,ifelse(temp$yage<10,'6-9'
                                ,ifelse(temp$yage<16,'10-15'
                                        ,ifelse(temp$yage>15,'16+',NA))))
      byAGE<-data.frame(temp %>% filter(marketvenue!='TRADEREV') %>%
                          dplyr::group_by(marketvenue,yage) %>%

                          summarise(
                            #    AVG_AGE = round(mean(age,na.rm=T),2)
                            #    ,AVG_MILES = round(mean(odometer_km,na.rm=T),0)
                            #    ,PCT_FILL = round(sum(as.character(grade_fill)=='T')/length(vin),2)
                            #    ,AVG_GRADE = mean(vehicle_grade,na.rm=T)
                            #     ,AVG_FLOOR = round(mean(floor_price,na.rm=T),0)
                            AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                            #      ,SALE = round(mean(sale_price),0)
                            ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                            ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                            ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)
                            ,N= length(vin)
                          ) %>% filter(N>10) %>% arrange(marketvenue,desc(N)))


      byMAKE<-data.frame(cmg_fullset.df %>%
                           dplyr::group_by(marketvenue,make) %>%

                           summarise(
                             AVG_AGE = round(mean(age,na.rm=T),2)
                             ,AVG_MILES = round(mean(odometer_km,na.rm=T),0)
                             ,PCT_FILL = round(sum(as.character(grade_fill)=='T')/length(vin),2)
                             ,AVG_GRADE = mean(vehicle_grade,na.rm=T)
                             ,AVG_FLOOR = round(mean(floor_price,na.rm=T),0)
                             ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                             ,SALE = round(mean(sale_price),0)
                             ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                             ,AMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                             ,AMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)
                             ,N= length(vin)
                           ))
      byMAKE %>% filter(N > 200 & !is.na(make))

      byFORD<-data.frame(cmg_fullset.df %>% filter(make=='FORD' & model=='F-150') %>%
                           dplyr::group_by(marketvenue,mode_disp_factor) %>%

                           summarise(
                             AVG_AGE = round(mean(age,na.rm=T),2)
                             ,AVG_MILES = round(mean(odometer_km,na.rm=T),0)
                             ,PCT_FILL = round(sum(as.character(grade_fill)=='T')/length(vin),2)
                             ,AVG_GRADE = mean(vehicle_grade,na.rm=T)
                             ,AVG_FLOOR = round(mean(floor_price,na.rm=T),0)
                             ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                             ,SALE = round(mean(sale_price),0)
                             ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                             ,AMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                             ,AMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)
                             ,N= length(vin)
                           ))
      byFORD


    byTime<-data.frame( cmg_fullset.df %>% filter(!is.na(CMG_PRICE) & as.Date(sale_date)> as.Date(Sys.Date())-90) %>%
                              dplyr::group_by(marketvenue,sale_week) %>%

                              summarise(
                                AVG_AGE = round(mean(age,na.rm=T),2)
                                ,AVG_MILES = round(mean(odometer_km,na.rm=T),0)
                                ,PCT_FILL = round(sum(as.character(grade_fill)=='T')/length(vin),2)
                                ,AVG_GRADE = mean(vehicle_grade,na.rm=T)
                                ,AVG_FLOOR = round(mean(floor_price,na.rm=T),0)
                                ,AVG_HIST = round(mean(historical_price,na.rm=T),0)
                                ,AVG_CMG = round(mean(CMG_PRICE,na.rm=T),0)
                                ,SALE = round(mean(sale_price),0)

                                ,HP_MAD = round(mad((historical_price/sale_price)-1,na.rm=T),3)
                                ,HP_MPE = round(mean((historical_price/sale_price)-1,na.rm=T),3)
                                ,HP_RET = round(mean(sale_price/historical_price,na.rm=T),3)

                                ,CMG_MAPE = round(mean(abs(CMG_PRICE-sale_price)/sale_price,na.rm=T),3)
                                ,CMG_MAD = round(mad((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                ,CMG_MPE = round(mean((CMG_PRICE/sale_price)-1,na.rm=T),3)
                                ,CMG_RET = round(mean(sale_price/CMG_PRICE,na.rm=T),3)

                                ,N= length(vin)
                              ))
    byTime
  }

  et<-Sys.time()
  print("END: PRICING")
  print(difftime(et,mastertime))
  }

    # BEGIN MARKDOWN
    MARKDOWN=T;if(MARKDOWN == T){
    markdown_path<- "F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/CMG_EKS_model_rmd.rmd"
    Sys.setenv(RSTUDIO_PANDOC="C:/Program Files/RStudio/bin/pandoc")

    rmarkdown::render(markdown_path,
                    output_file =  paste("CMG_Model_updates_",as.Date(Sys.Date()),".html", sep='')
                    ,output_dir = "F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/CMG_updates"
                    ,output_format = c("html_document"))



}
    # END MARKDOWN
}

}
# END MODEL

#shutdown H2O connection
h2o.shutdown(prompt = FALSE)
}
# END PRICING MODEL

print("************ END NEW ALGO RUN ****************")
endtime<-difftime(Sys.time(),mastertime)
print("OVERALL RUN TIME = ")
print(endtime)

#Schedule task
ST=FALSE;if(ST==TRUE){
  library(taskscheduleR)
  taskscheduler_create(taskname = "CMG_EKS_modelupdate"
                       , rscript = "F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/CMG_EKS_modelupdate.R"
                       , schedule = "WEEKLY"
                       , starttime = "19:00"
                       , startdate = format(Sys.Date(), "%m/%d/%Y")
                       , days =  c("TUE","THU","SAT"))
}

