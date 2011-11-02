SELECT

            'NOTSUM' "NOTSUM",
            B.I_CTN "CTN",               DECODE(SD_COM_MODEL.T_MODEL,NULL,'-',SD_COM_MODEL.T_MODEL) "단말기",
            DECODE(SD_CODE.T_CODE, NULL,'-',SD_CODE.T_CODE) "단말기서비스",
            DECODE(SD.T_CODE, NULL,'-',SD.T_CODE) "고객등급",
            NVL(SUM(DECODE(I_NET_CLS,'1',DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY))),0) "2G",
            NVL(SUM(DECODE(I_NET_CLS,'2',DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY))),0) "1X/EVDO",
            NVL(SUM(DECODE(I_NET_CLS,'4',DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY))),0) "WCDMA",    -- 추가
            NVL(SUM(DECODE(I_NET_CLS,'X',DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY))),0) "기타",
            SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY)) "시도호수",
            SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_COMPLETE)) "완료호수",

            ROUND(SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_COMPLETE))
            * 100 / DECODE(SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY)),0,1,SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY))),2) "완료율(%)",

            SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_DROP)) "절단호",
            ROUND(SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_DROP))
            * 100 / DECODE(SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_COMPLETE)),0,1,SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_COMPLETE))),2) "절단율(%)",

            ROUND(SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,AMT_CALL))
            / decode((10* SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY))),0,1,10* SUM(DECODE(I_SERVICE_GRP,'2',0,'7',0,'3',0,'6',0,CNT_CALL_TRY))),2) "통화시간",

            NVL(SUM(DECODE(I_SERVICE_GRP,'1',CNT_CALL_TRY,'10',CNT_CALL_TRY)),0) "VOICE",      -- 변경
            NVL(SUM(DECODE(I_SERVICE_GRP,'2',CNT_CALL_TRY)),0) "VMS",
            NVL(SUM(DECODE(I_SERVICE_GRP,'3',CNT_CALL_TRY)),0) "SMS",
            NVL(SUM(DECODE(I_SERVICE_GRP,'7',CNT_CALL_TRY)),0) "WIN",
            NVL(SUM(DECODE(I_SERVICE_GRP,'4',CNT_CALL_TRY)),0) "IWF",
            NVL(SUM(DECODE(I_SERVICE_GRP,'9',CNT_CALL_TRY)),0) "BEARER",   -- 추가
            NVL(SUM(DECODE(I_SERVICE_GRP, '6', CONN_CNT_CALL)) ,0) "IPAS접속시도호",
            NVL(SUM(DECODE(I_SERVICE_GRP, '6', CNT_CALL)) , 0) "IPAS시도횟수",
            NVL(SUM(DECODE(I_SERVICE_GRP, '6', CNT_CALL_TRY)) , 0) "IPAS총시도호수",
            NVL(SUM(DECODE(I_SERVICE_GRP,'4',CNT_CALL_COMPLETE)),0) "IWF완료호",
            NVL(SUM(DECODE(I_SERVICE_GRP,'4',CNT_CALL_DROP)),0) "IWF절단호",
            NVL(SUM(DECODE(I_SERVICE_GRP, '6', DROP_CALL)) , 0) "절단호",
            NVL(ROUND(SUM(TOT_TIME_DUR) / DECODE(SUM(CNT_CALL),0,1,SUM(CNT_CALL)),2),0) "통화시간(total_DU)",
            NVL(ROUND(SUM(CAT_TIME_DUR) / SUM(DECODE(I_SERVICE_GRP,'6',CNT_CALL_TRY)),2) , 0) "통화시간(cate_DU)",
            NVL(ROUND((SUM(TOT_ACC_BYTE)/32) / DECODE(SUM(CNT_CALL),0,1,SUM(CNT_CALL)),2),0) "사용량(session;PACKET)",
            NVL(ROUND((SUM(TOT_CAT_BYTE)/32) / SUM(DECODE(I_SERVICE_GRP,'6',CNT_CALL_TRY)),2),0) "사용량(category;PACKET)",
            NVL(SUM(DECODE(I_CFC_GRP,'0', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "완료호",
            NVL(SUM(DECODE(I_CFC_GRP,'1', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "발신가입자 원인 불완료호",
            NVL(SUM(DECODE(I_CFC_GRP,'2', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "착신가입자 원인 불완료호",
            NVL(SUM(DECODE(I_CFC_GRP,'3', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "국번/중계선 원인의 불완료호",
            NVL(SUM(DECODE(I_CFC_GRP,'4', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "기타 MSC 원인의 불완료호",
            NVL(SUM(DECODE(I_CFC_GRP,'5', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "기지국 원인의 불완료호",
            NVL(SUM(DECODE(I_CFC_GRP,'8', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "기타 원인 불완료호",
            NVL(SUM(DECODE(I_CFC_GRP,'9', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "HLR 원인 불완료호",
            NVL(SUM(DECODE(I_CFC_GRP,'10',(CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "발신가입자 원인 절단호",
            NVL(SUM(DECODE(I_CFC_GRP,'11',(CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "착신가입자 원인 절단호",
            NVL(SUM(DECODE(I_CFC_GRP,'12',(CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "국번/중계선 원인의 절단호",
            NVL(SUM(DECODE(I_CFC_GRP,'6', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "기지국 원인의 절단호",
            NVL(SUM(DECODE(I_CFC_GRP,'7', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "기타 MSC 원인의 절단호",
            NVL(SUM(DECODE(I_CFC_GRP,'13',(CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "기타 원인 절단호",
            NVL(SUM(DECODE(I_CFC_GRP,'14',(CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "HLR 원인 절단호",
            NVL(SUM(DECODE(I_CFC_GRP,'X', (CASE WHEN I_SERVICE_GRP IN ('1','4','9','10') THEN CNT_CALL_TRY ELSE 0 END ))),0) "기타"

FROM (
            SELECT
                I_CTN,
                O_CTN,
                I_NET_CLS,
                I_CFC_TYPE,
                I_SERVICE_GRP,
                I_CFC_GRP,
                I_CDR_SEQ,
                I_AN_RELEASE_IND,
                COUNT(*) CNT_CALL_TRY,
                COUNT(DECODE(I_CFC_TYPE, '1', 1, '2', 1))         CNT_CALL_COMPLETE,
                COUNT(DECODE(I_CFC_TYPE, '2', 1))                 CNT_CALL_DROP,
                SUM(AMT_CALL)                                     AMT_CALL ,
                SUM(CNT_CALL)                                     CONN_CNT_CALL ,
                COUNT(DECODE(I_CDR_SEQ, '1', 1))                  CNT_CALL ,
                COUNT(DECODE(I_AN_RELEASE_IND, '2', 1))           DROP_CALL,
                SUM(DECODE(I_CDR_SEQ, '1', TOT_TIME_DURATION))    TOT_TIME_DUR,
                SUM(CATEGORY_TIME_DURATION)                       CAT_TIME_DUR,
                SUM(ACC_UP_BYTE) + SUM(ACC_DOWN_BYTE)             TOT_ACC_BYTE,
                SUM(CATEGORY_UP_BYTE) + SUM(CATEGORY_DOWN_BYTE)   TOT_CAT_BYTE

            FROM (
                    SELECT
                        I_ETL_DT,
                        I_CALL_DT,
                        I_HOUR,
                        I_SERVICE_GRP,
                        I_CFC_GRP,
                        I_CFC_TYPE,
                        I_INOUT,
                        I_NET_CLS,
                        AMT_CALL,
                        i_out_ctn   i_ctn,
                        i_out_ctn   o_ctn,
                        'A' I_CDR_SEQ,
                        'A' I_AN_RELEASE_IND,
                        0 TOT_TIME_DURATION,
                        0 CATEGORY_TIME_DURATION,
                        0 ACC_UP_BYTE,
                        0 ACC_DOWN_BYTE,
                        0 CATEGORY_DOWN_BYTE,
                        0 CATEGORY_UP_BYTE,
                        0 CNT_CALL
                    from  srf_voice
                    WHERE   i_call_dt||'' between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss')
                        AND i_etl_dt between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss') + 1
                        AND (
                                (i_out_ctn in ('01036931998') AND i_inout in ('0'))
                                OR
                                (i_in_ctn in ('01036931998') AND i_inout in ('1'))
                            )
                UNION ALL

                    SELECT
                            I_ETL_DT,
                            I_CALL_DT,
                            I_HOUR,
                            '3' I_SERVICE_GRP,
                            'X' I_CFC_GRP,
                            I_CFC_TYPE,
                            I_INOUT,
                            I_NET_CLS,
                            0 AMT_CALL,
                            i_out_ctn   i_ctn,
                            i_out_ctn   o_ctn,
                            'A' I_CDR_SEQ,
                            'A' I_AN_RELEASE_IND,
                            0 TOT_TIME_DURATION,
                            0 CATEGORY_TIME_DURATION,
                            0 ACC_UP_BYTE,
                            0 ACC_DOWN_BYTE,
                            0 CATEGORY_DOWN_BYTE,
                            0 CATEGORY_UP_BYTE,
                            0 CNT_CALL

                    FROM    SRF_SMSS_LOG
                    WHERE   i_call_dt||'' between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss')
                        AND i_etl_dt between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss') + 1
                        AND (
                                (i_out_ctn in ('01036931998') AND I_INOUT = '0')
                                OR
                                (i_IN_ctn in ('01036931998') AND I_INOUT = '1')
                            )

                UNION ALL

                    select
                            I_ETL_DT,
                            I_CALL_DT,
                            I_HOUR,
                            I_SERVICE_GRP,
                            I_CFC_GRP,
                            I_CFC_TYPE,
                            I_INOUT,
                            'X' I_NET_CLS,
                            0 AMT_CALL ,
                            I_OUT_CTN I_CTN,
                            '' O_CTN,
                            I_CDR_SEQ,
                            I_AN_RELEASE_IND,
                            TOT_TIME_DURATION,
                            CATEGORY_TIME_DURATION,
                            ACC_UP_BYTE,
                            ACC_DOWN_BYTE,
                            CATEGORY_DOWN_BYTE,
                            CATEGORY_UP_BYTE,
                            CNT_CALL

                    FROM    srf_ipas
                    WHERE   i_call_dt||'' between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss')
                        AND i_etl_dt between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss')+1
                        and i_out_ctn in ('01036931998')
                        AND I_SERV_NODE_IND IN ('1','2')

                UNION ALL

                    SELECT
                            I_ETL_DT,
                            I_CALL_DT,
                            I_HOUR,
                            I_SERVICE_GRP,
                            I_CFC_GRP,
                            I_CFC_TYPE,
                            I_INOUT,
                            I_NET_CLS,
                            AMT_CALL,
                            i_out_ctn   i_ctn,
                            i_out_ctn   o_ctn,
                            'A' I_CDR_SEQ,
                            'A' I_AN_RELEASE_IND,
                            0 TOT_TIME_DURATION,
                            0 CATEGORY_TIME_DURATION,
                            0 ACC_UP_BYTE,
                            0 ACC_DOWN_BYTE,
                            0 CATEGORY_DOWN_BYTE,
                            0 CATEGORY_UP_BYTE,
                            0 CNT_CALL

                    from    SRF_WCD_VOICE
                    WHERE   i_call_dt||'' between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss')
                        AND i_etl_dt between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss')+1
                        AND (
                                (i_out_ctn in ('01036931998') AND i_inout in ('0'))
                                OR
                                (i_in_ctn in ('01036931998') AND i_inout in ('1'))
                            )
            ) A
            GROUP BY
                I_CTN,
                O_CTN,
                I_NET_CLS,
                I_SERVICE_GRP,
                I_CFC_TYPE,
                I_CFC_GRP,
                I_CDR_SEQ,
                I_AN_RELEASE_IND
) B,
SD_CODE,
SD_COM_MODEL,
SD_CODE SD,
(
    SELECT
            DISTINCT DECODE(SUBSTR(I_CTN,4,1),'0',SUBSTR(I_CTN,1,3)||SUBSTR(I_CTN,5,7),I_CTN) I_CTN,
            I_MODEL,
            I_CUST_GRADE

    FROM    SBF_SQD_CUST
    WHERE   (I_CTN,I_FIRST_OPEN_YYYYMMDD) IN
                                        (
                                            SELECT I_CTN,MAX(I_FIRST_OPEN_YYYYMMDD)
                                            FROM SBF_SQD_CUST
                                            WHERE I_CTN IN ('01036931998')
                                            GROUP BY I_CTN
                                        )
) VCUST

WHERE
        B.I_CTN = VCUST.I_CTN(+)
        AND VCUST.I_MODEL = SD_COM_MODEL.I_MODEL(+)
        AND SD_COM_MODEL.I_MODEL_CDMA = SD_CODE.I_CODE(+)
        AND SD_CODE.I_CODE_GRP(+)='022'
        AND VCUST.I_CUST_GRADE = SD.I_CODE(+)
        AND SD.I_CODE_GRP(+) = '005'
        AND ROWNUM >0

GROUP BY

        B.I_CTN,
        DECODE(SD_COM_MODEL.T_MODEL,NULL,'-',SD_COM_MODEL.T_MODEL),
        DECODE(SD_CODE.T_CODE, NULL,'-',SD_CODE.T_CODE),
        DECODE(SD.T_CODE, NULL,'-',SD.T_CODE)