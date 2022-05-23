print("************ TEST AND PUBLISHING NEW PRICING MODELS TO KDD/KDP ****************")
mastertime<-Sys.time()
print(mastertime)
print(gc())

# BEGIN: LIBRARIES, which are required for all code in this script
SHOWLIB<-TRUE;if (SHOWLIB==TRUE){
  #Add libraries/source internal functions
  library(RODBC)
  library(sqldf)
  library(lubridate)
  library(dplyr)
  library(taskscheduleR)
  library(keyring)
  options(error=NULL)
  library(botor);library(aws.signature);library(reticulate);library(aws.s3)
  Sys.setenv("AWS_SHARED_CREDENTIALS_FILE" = aws.signature::default_credentials_file(),
             "AWS_CONFIG_FILE" = "C:\\Users\\rodney.ellis\\.aws\\config",
             "AWS_PROFILE" = "kdp")
  kdd_session <-botor::boto3$session$Session(profile_name = 'kdd')
  kdd_s3 <- kdd_session$client('s3')
  kdp_session <-botor::boto3$session$Session(profile_name = 'kdp')
  kdp_s3 <- kdp_session$client('s3')
  
  
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
  start_h2o('lpm',mem_gb=20)
  
  
}
# END:   LIBRARIES

#BEGIN: MODEL TESTING RESULTS 
UPDATE_MODELS=TRUE;if(UPDATE_MODELS==T){
      if(format(Sys.Date(), format="%A")=="Sunday"){
        
          CMG_EKS=TRUE;if(CMG_EKS==TRUE){  #CURRNET CMG/D2D (TradeRev)
           
            SNOWFLAKE_dbConnection<-odbcConnect("snowflake_azure", pwd = key_get('SNOWFLAKE'))
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
                request.vehicle_grade::NUMERIC                                                                         AS vehicle_grade,
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
                        WHEN ROUND(COALESCE(request.displ_liters::NUMERIC, v8.mode_displ_liters::NUMERIC), 1) > 20
                            THEN '0'::TEXT
                        WHEN ROUND(COALESCE(request.displ_liters::NUMERIC, v8.mode_displ_liters::NUMERIC), 0) =
                             COALESCE(request.displ_liters::NUMERIC, v8.mode_displ_liters::NUMERIC)
                            THEN COALESCE(request.displ_liters::NUMERIC, v8.mode_displ_liters::NUMERIC)::FLOAT::INT::TEXT
                            ELSE ROUND(COALESCE(request.displ_liters::NUMERIC, v8.mode_displ_liters::NUMERIC), 1)::TEXT
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
                      wc.SALE_DATE BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE
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
            last30.df<-data.df
            
            
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
            currentupdate_cmg_model
            current_cmg_modelmojo <- h2o.download_mojo(currentupdate_cmg_model,path = paste("F:\\Projects\\Canadian_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_cmgversion_path,sep=""), get_genmodel_jar=TRUE)
            
            last30.df$kdp_ratio<-h2o.mojo_predict_df(last30.df
                                                      ,mojo_zip_path = s3modelfile_cmg
                                                      ,genmodel_jar_path = genmodelfile_cmg
            )$predict
            last30.df$kdp_price<-round(last30.df$kdp_ratio*last30.df$msrp,0)
            
            cmg_update_metrics<-data.frame(last30.df %>% 
                                             filter(sale_price>1000 & !is.na(msrp)) %>%
                                             summarise(
                                               AVG_PRED = mean(kdp_price,na.rm=T)
                                               ,AVG_SALE = mean(sale_price,na.rm=T)
                                               ,RMSE = round(sqrt(mean((kdp_ratio-(sale_price/(kdp_price/kdp_ratio)))^2)),4)
                                               ,MAD = round(mad((kdp_price/sale_price)-1,na.rm=T),4)
                                               ,RET = round(mean((sale_price/kdp_price),na.rm=T),3)
                                              ,VAL_RMSE =  round(currentupdate_cmg_model@model$validation_metrics@metrics$RMSE,4)
                                               ,N = length(unique(vin))
                                             ))
            
  # get current model from KDP
  ls_results <- kdp_s3$list_objects(Bucket='kdp-configs', Prefix='pricing-amg-canada-api')
  ls_df<-data.frame(do.call(rbind, ls_results$Contents))
  kdp_cmgmodel_name<-sub("\\..*", "",(sub(".*/", "", last(ls_df$Key[grep("CMG_model_",ls_df$Key)]))))
  kdp_cmgversion_path<-paste("CMG_version_"
                         ,substring(kdp_cmgmodel_name,15,18)
                         ,substring(kdp_cmgmodel_name,13,14),sep="")
  kdp_cmg_model<-h2o.loadModel(path=paste("F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/h2o_models/",kdp_cmgversion_path,"\\",kdp_cmgmodel_name,sep=""))
  kdp_modelfile_cmg<-paste("F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/h2o_models/",kdp_cmgversion_path,"\\",kdp_cmgmodel_name,".zip",sep="")
  kdp_genmodelfile_cmg<-paste("F:/Projects/Canadian_Market_Guide/SNOWFLAKE_models/h2o_models/",kdp_cmgversion_path,"/h2o-genmodel.jar",sep="")
 
  
  last30.df$kdp_ratio<-h2o.mojo_predict_df(last30.df
                                           ,mojo_zip_path = kdp_modelfile_cmg
                                           ,genmodel_jar_path = kdp_genmodelfile_cmg
  )$predict
  last30.df$kdp_price<-round(last30.df$kdp_ratio*last30.df$msrp,0)
  
  cmg_kdp_metrics<-data.frame(last30.df %>% 
                                filter(sale_price>1000 & !is.na(msrp)) %>%
                                summarise(
                                  AVG_PRED = mean(kdp_price,na.rm=T)
                                  ,AVG_SALE = mean(sale_price,na.rm=T)
                                  ,RMSE = round(sqrt(mean((kdp_ratio-(sale_price/(kdp_price/kdp_ratio)))^2)),4)
                                  ,MAD = round(mad((kdp_price/sale_price)-1,na.rm=T),4)
                                  ,RET = round(mean((sale_price/kdp_price),na.rm=T),3)
                                  ,VAL_RMSE =  round(kdp_cmg_model@model$validation_metrics@metrics$RMSE,4)
                                  ,N = length(unique(vin))
                                )) 
  cmg_metrics<-data.frame(rbind(cmg_kdp_metrics,cmg_update_metrics))
  cmg_metrics<-data.frame(rbind(cmg_metrics,apply(cmg_metrics,2,diff)))
  rownames(cmg_metrics)<-c("CURRENT","PROPOSED","TEST")
  cmg_metrics$MODEL_ID<-c(kdp_cmg_model@model_id,currentupdate_cmg_model@model_id,"")
  print(cmg_metrics)
  
#Set Logic
 
  if(round(cmg_metrics$MAD[3],2) <= 0 & round(cmg_metrics$RMSE[3],2) <= 0 & round(cmg_metrics$RET[3],2) <= 0){
    #Dev
    kdd_s3$upload_file(paste(s3modelfile_cmg,sep='/'),'kdd-configs',paste('pricing-amg-canada-api',current_cmg_modelmojo,sep='/'))
    kdd_s3$upload_file(paste(genmodelfile_cmg,sep='/'),'kdd-configs','pricing-amg-canada-api/h2o-genmodel.jar')
    kdd_s3$upload_file(paste(s3modelfile_cmg,sep='/'),'kdd-configs',paste('pricing-traderev-api',current_cmg_modelmojo,sep='/'))
    kdd_s3$upload_file(paste(genmodelfile_cmg,sep='/'),'kdd-configs','pricing-traderev-api/h2o-genmodel.jar')
    #PROD
    kdp_s3$upload_file(paste(s3modelfile_cmg,sep='/'),'kdp-configs',paste('pricing-amg-canada-api',current_cmg_modelmojo,sep='/'))
    kdp_s3$upload_file(paste(genmodelfile_cmg,sep='/'),'kdp-configs','pricing-amg-canada-api/h2o-genmodel.jar')
    kdp_s3$upload_file(paste(s3modelfile_cmg,sep='/'),'kdp-configs',paste('pricing-traderev-api',current_cmg_modelmojo,sep='/'))
    kdp_s3$upload_file(paste(genmodelfile_cmg,sep='/'),'kdp-configs','pricing-traderev-api/h2o-genmodel.jar')
  }
  
}  
          
          AMG_US_EKS=TRUE;if(AMG_US_EKS==TRUE){  
           
            SNOWFLAKE_dbConnection<-odbcConnect("snowflake_azure", pwd = key_get('SNOWFLAKE'))
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
          SALE_DATE BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE
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
            last30.df<-sqlQuery(SNOWFLAKE_dbConnection,usa.sql(),stringsAsFactors=T)  
            names(last30.df)<-tolower(names( last30.df))
            
            currentversionpath_amgus <- dir("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\")
            mojo_amgus_version_path<-last(currentversionpath_amgus[grep("AMG_US",currentversionpath_amgus)])
            mojo_amgus_model_path<-paste("AMG_US_model_20"
                                      ,substring(mojo_amgus_version_path,20,21)
                                      ,substring(mojo_amgus_version_path,16,17)
                                      ,substring(mojo_amgus_version_path,18,19),sep="")
            s3modelfile_amgus<-paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_amgus_version_path,"/",mojo_amgus_model_path,".zip",sep="")
            genmodelfile_amgus<-paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_amgus_version_path,"/h2o-genmodel.jar",sep="")
            
            currentupdate_amgus_model<-h2o.loadModel(path=paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_amgus_version_path,"\\",mojo_amgus_model_path,sep=""))
            data.frame(h2o.varimp(currentupdate_amgus_model))
            currentupdate_amgus_model
            current_amgus_modelmojo <- h2o.download_mojo(currentupdate_amgus_model,path = paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",mojo_amgus_version_path,sep=""), get_genmodel_jar=TRUE)
            
            last30.df$pred_ratio<-h2o.mojo_predict_df(last30.df
                                                      ,mojo_zip_path = s3modelfile_amgus
                                                      ,genmodel_jar_path = genmodelfile_amgus
            )$predict
            last30.df$amg_price<-round(last30.df$pred_ratio*last30.df$msrp,0)
            
            amgus_update_metrics<-data.frame(last30.df %>% 
                                             filter(sale_price>1000) %>%
                                             summarise(
                                               AVG_PRICE = mean(amg_price,na.rm=T)
                                               ,RMSE = round(sqrt(mean((pred_ratio-(sale_price/(amg_price/pred_ratio)))^2)),4)
                                               ,MAD = round(mad((amg_price/sale_price)-1,na.rm=T),4)
                                               ,RET = round(mean((sale_price/amg_price),na.rm=T),3)
                                               ,VAL_RMSE = round(currentupdate_amgus_model@model$validation_metrics@metrics$RMSE,4)
                                               ,N = length(unique(vin))
                                             ))
            
            #CURRENT AMG from KDP
            ls_results <- kdp_s3$list_objects(Bucket='kdp-configs', Prefix='pricing-amg-us-api')
            ls_df<-data.frame(do.call(rbind, ls_results$Contents))
            kdp_amgusmodel_name<-sub("\\..*", "",(sub(".*/", "", last(ls_df$Key[grep("AMG_US_model_",ls_df$Key)]))))
            kdp_amgusversion_path<-paste("AMG_US_version_"
                                       ,substring(kdp_amgusmodel_name,18,19)
                                       ,substring(kdp_amgusmodel_name,20,21)
                                       ,substring(kdp_amgusmodel_name,16,17)
                                       ,sep="")
            kdp_amgus_model<-h2o.loadModel(path=paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",kdp_amgusversion_path,"\\",kdp_amgusmodel_name,sep=""))
            kdp_modelfile_amgus<-paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",kdp_amgusversion_path,"/",kdp_amgusmodel_name,".zip",sep="")
            kdp_genmodelfile_amgus<-paste("F:\\Projects\\Adesa_Market_Guide\\SNOWFLAKE_models\\h2o_models\\",kdp_amgusversion_path,"/h2o-genmodel.jar",sep="")
            
            last30.df$kdp_ratio<-h2o.mojo_predict_df(last30.df
                                                     ,mojo_zip_path = kdp_modelfile_amgus
                                                     ,genmodel_jar_path = kdp_genmodelfile_amgus
            )$predict
            last30.df$kdp_price<-round(last30.df$kdp_ratio*last30.df$msrp,0)
            amgus_kdp_metrics<-data.frame(last30.df %>% 
                                          filter(sale_price>1000) %>%
                                          summarise(
                                            AVG_PRICE = mean(kdp_price,na.rm=T)
                                            ,RMSE = round(sqrt(mean((kdp_ratio-(sale_price/(kdp_price/kdp_ratio)))^2)),4)
                                            ,MAD = round(mad((kdp_price/sale_price)-1,na.rm=T),4)
                                            ,RET = round(mean((sale_price/kdp_price),na.rm=T),3)
                                            ,VAL_RMSE =  round(kdp_amgus_model@model$validation_metrics@metrics$RMSE,4)
                                            ,N = length(unique(vin))
                                          )) 
            
            
            amgus_metrics<-data.frame(rbind(amgus_kdp_metrics,amgus_update_metrics))
            amgus_metrics<-data.frame(rbind(amgus_metrics,apply(amgus_metrics,2,diff)))
            rownames(amgus_metrics)<-c("CURRENT","PROPOSED","TEST")
            amgus_metrics$MODEL_ID<-c(kdp_amgus_model@model_id,currentupdate_amgus_model@model_id,"")
            
            print(amgus_metrics)
            
            #Set Logic
            
            if(round(amgus_metrics$MAD[3],2) <= 0 & round(amgus_metrics$RMSE[3],2) <= 0 & round(amgus_metrics$RET[3],2) <= 0){
              #Dev
              kdd_s3$upload_file(paste(s3modelfile_amgus,sep='/'),'kdd-configs',paste('pricing-amg-us-api',current_amgus_modelmojo,sep='/'))
              kdd_s3$upload_file(paste(genmodelfile_amgus,sep='/'),'kdd-configs','pricing-amg-us-api/h2o-genmodel.jar')
              #PROD
              kdp_s3$upload_file(paste(s3modelfile_amgus,sep='/'),'kdp-configs',paste('pricing-amg-us-api',current_amgus_modelmojo,sep='/'))
              kdp_s3$upload_file(paste(genmodelfile_amgus,sep='/'),'kdp-configs','pricing-amg-us-api/h2o-genmodel.jar')
            }
            
          }
          
      }
           }
#END: MODEL TESTING RESULTS   
  
h2o.shutdown(prompt=FALSE)

#Schedule task
ST=FALSE;if(ST==TRUE){
  taskscheduler_create(taskname = "EKS_Model_Updates"
                       , rscript = "F:/Projects/Adesa_Market_Guide/SNOWFLAKE_models/LoadPricingModels_EKS.R"
                       , schedule = "WEEKLY"
                       , starttime = "12:00"
                       , startdate = format(Sys.Date(), "%m/%d/%Y")
                       , days =  c("SUN"))
}
  
  
  
#mylog <- system.file("extdata", "CMG_dataload.log", package = "taskscheduleR")
#cat(readLines(mylog), sep = "\n") 
  
  
  
  
  
