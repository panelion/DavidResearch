
SELECT
            'NOTSUM' "NOTSUM",
            I_OUT_CTN "발신번호",
            to_char(I_ETL_DT, 'yyyy-mm-dd hh24:mi:ss') "처리일자",
            to_char(I_CALL_DT, 'yyyy-mm-dd hh24:mi:ss') "발신일자",
            I_HOUR "발신시간대",
            DECODE(I_WS_TYPE, '250', '일반Data', '지능망Data') "지능망서비스",
            DECODE(I_AN_RELEASE_IND, '1', '정상종료', '2', '절단', I_AN_RELEASE_IND) "접속종료원인",
            NVL(sd_com_cfc.U_CFC, A.I_CFC) "성공실패원인",
            NUM_ACTIVE "세션천이횟수",
            NVL(SS.T_CODE, A.I_SERV_NODE_IND) "서비스받은시스템",
            I_NAS_IP "PDSN_ID",
            NVL(SDB.T_SWITCH, A.I_PPP_START_SWITCH) "발신교환기",
            I_PPP_START_BSC "발신BSC",
            NVL(SDL.t_cell, A.I_PPP_START_CELL) "발신기지국",
            NVL(SDS.T_SERVICE, A.I_SERVICE) "서비스옵션",
            NVL(SF.T_CODE, 'Reserved') "순방향속도",
            NVL(SR.T_CODE, 'Reserved') "역방향속도",
            NVL(SC.T_CODE, I_CATEGORY_ID) "서비스군",
            ROUND(ACC_UP_BYTE/32 , 2) "A_Upload<br>(Packet)",
            ROUND(ACC_DOWN_BYTE/32 , 2) "A_Download<br>(Packet)",
            TOT_TIME_DURATION "S_사용시간",
            ROUND(CATEGORY_UP_BYTE/32 , 2) "C_Upload<br>(Packet)",
            ROUND(CATEGORY_DOWN_BYTE/32 , 2) "C_Download<br>(Packet)",
            CATEGORY_TIME_DURATION "C_사용시간"
FROM
            SRF_IPAS A,
            sd_com_code SS,
            sd_com_code SF,
            sd_com_code SR,
            sd_com_code SC,
            sd_com_service SDS,
            sbf_sqd_cust SDC,
            sd_com_model,
            sd_com_code,
            sd_com_switch SDB,
            sd_com_cell SDL,
            sd_com_cfc

WHERE       i_call_dt between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss')
            AND    i_etl_dt between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss')+1
            AND    I_OUT_CTN IN ('01036931998')
            AND    A.I_SERV_NODE_IND = SS.I_CODE(+)
            AND    SS.I_CODE_GRP(+)='026'
            AND    A.I_FMUX = SF.I_CODE(+)
            AND    SF.I_CODE_GRP(+)='027'
            AND    A.I_RMUX = SR.I_CODE(+)
            AND    SR.I_CODE_GRP(+)='028'
            AND    A.I_SERVICE = SDS.I_SERVICE(+)
            AND    SDS.I_SERVICE_GRP(+)='6'
            AND    A.I_CATEGORY_ID = SC.I_CODE(+)
            AND    SC.I_CODE_GRP(+)='029'
            AND    A.I_OUT_CTN = SDC.I_CTN(+)
            AND    SDC.I_MODEL = sd_com_model.I_MODEL(+)
            AND    sd_com_model.I_MODEL_CDMA = sd_com_code.I_CODE(+)
            AND    sd_com_code.I_CODE_GRP(+)='022'
            AND    A.I_PPP_START_SWITCH = SDB.I_SWITCH(+)
            AND    SDB.I_ENDT(+) ='99991231'
            AND    A.I_PPP_START_SWITCH = SDL.I_SWITCH(+)
            AND    A.I_PPP_START_BSC = SDL.I_BSC(+)
            AND    A.I_PPP_START_CELL = SDL.I_CELL(+)
            AND    SDL.I_ENDT(+) = to_date('99991230235959', 'yyyymmddhh24miss')
            AND    A.I_CFC = sd_com_cfc.I_CFC(+)
            AND    sd_com_cfc.I_SERVICE_GRP(+) ='6'

ORDER BY I_CALL_DT DESC