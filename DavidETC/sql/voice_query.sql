SELECT
            'NOTSUM' "NOTSUM",
            to_char(SV.I_CALL_DT, 'yyyy-mm-dd hh24:mi:ss') "기준일시",
            SV.I_OUT_CTN "발신번호",
            SV.I_IN_CTN "착신번호",
            decode(SV.I_INOUT, '0', '발신', '1', '착신', '2', '지능망발신', '4', 'Voice중계발신', '5', 'Voice중계착신') "발착신구분",
            nvl(sd_com_switch.T_SWITCH, SV.I_SWITCH) "교환기",
            SV.I_BSC "BSC",
            decode(sd_com_cell.u_cell, null, sv.i_cell, sd_com_cell.u_cell) "기지국",
            decode(sd_com_cell.t_cell, null, sv.i_cell, sd_com_cell.t_cell) "기지국코드",

            nvl(t_fa, '-') "CHANNEL",
            nvl(t_sec, '-') "SECTOR",

            sd_com_service.T_SERVICE "사용서비스종류",
            SV.AMT_CALL "호당통화누적시간",
            sd_com_cfc.T_CFC "불완료원인",
            sd_com_cfc.I_CFC "CFC 코드",
            위치,
            SV.I_CDR_TYPE,
            SV.I_CALL_ETYPE,
            SV.I_CALL_TYPE,
            SV.I_NET_CLS,
            SV.I_PREFIX,
            SV.I_LINE_NO,
            SV.I_MODEL_SER_CLS,
            SV.I_IN_ROUTE,
            SV.I_BONBU,
            SV.I_CALLED_SWITCH,
            SV.I_OUT_ROUTE,
            SV.I_CALLING_NUM,
            SV.I_CHARGE_CODE,
            SV.I_CALL_DIRECTION,
            SV.I_PORTABIL_NO,
            SV.I_PORTABIL_ORG,
            SV.I_ROUTING_NO,
            SV.I_TERMINAL_CAP,
            SV.I_SUBSCRIBER_TYPE,
            SV.I_CALL_END_TIME,
            SV.I_TARGET_ORG
FROM
            srf_voice SV
            sd_com_switch,
            sd_com_service,
            sd_com_cfc,
            sd_com_cell,
            sd_com_fa,
            sd_com_sec

WHERE
            i_call_dt||'' between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss')
    AND     i_etl_dt between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss') + 1
    AND     (
                    (i_inout in ('1','3','5') and i_in_ctn in ('01036931998'))
                OR
                    (i_inout in ('0','2','4') and i_out_ctn in ('01036931998'))
            )
    AND     i_service_grp <> '7'
    AND     i_fa = SV.I_FA_CHANNEL
    AND     i_sec = SV.I_SECTOR
    AND     SV.I_SWITCH = sd_com_switch.I_SWITCH(+)
    AND     sd_com_switch.I_ENDT(+) = '99991231'
    and     to_number( decode(sd_com_service.i_service, 'X', 0, '0A2', 0, sd_com_service.i_service) ) < 2000
    AND     SV.I_SERVICE_GRP = sd_com_service.I_SERVICE_GRP(+)
    AND     SV.I_SERVICE = sd_com_service.I_SERVICE(+)
    AND     SV.I_SERVICE_GRP = sd_com_cfc.I_SERVICE_GRP(+)
    AND     SV.I_CFC = sd_com_cfc.I_CFC(+)
    AND     SV.I_CELL = sd_com_cell.I_CELL(+)
    AND     SV.I_SWITCH = sd_com_cell.I_SWITCH(+)
    AND     SV.I_BSC = sd_com_cell.I_BSC(+)
    AND     sd_com_cell.I_ENDT(+) = '99991231'
ORDER BY 2 DESC