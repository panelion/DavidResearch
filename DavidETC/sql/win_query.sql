SELECT
            'NOTSUM' "NOTSUM",
            to_char(SV.I_CALL_DT, 'yyyy-mm-dd hh24:mi:ss') "기준일시",
            SV.I_OUT_CTN "발신번호",
            SV.I_IN_CTN "착신번호",
            decode(SV.I_INOUT, '2', '지능망발신', '3', '지능망착신') "발착신구분",
            sd_com_bonbu.T_BONBU "사업본부",
            nvl(sd_com_switch.T_SWITCH, SV.I_SWITCH) "교환기",
            nvl(sd_com_switch_DEV.T_SWITCH, SV.I_DEV_SWITCH) "장비명",
            decode(SV.I_NET_CLS, '1', '2G', '2', '1x/EvDo', 'X', '기타', SV.I_NET_CLS) "망구분",
            SV.I_IN_ROUTE "IN_ROUTE",
            nvl(decode(SV.I_PREFIX, 'X', '기타', SV.i_prefix), '-') || '  (' || decode(sd_prefix_org.i_service_org, 'X', '기타', sd_prefix_org.i_service_org) || ')' "PREFIX",
            nvl(SV.I_LINE_NO, '-') "국번",
            sd_com_service_grp.T_SERVICE_GRP "서비스그룹",
            sd_com_service.T_SERVICE "사용서비스종류",
            SV.AMT_CALL "호당통화누적시간",
            sd_com_cfc.T_CFC "불완료원인",
            sd_com_cfc.I_CFC "CFC 코드"

FROM    (
                SELECT
                            I_CALL_DT I_CALL_DT,
                            I_OUT_CTN I_OUT_CTN,
                            I_IN_CTN I_IN_CTN,
                            I_BONBU I_BONBU,
                            DECODE(I_INOUT, '0', I_CALLING_SWITCH, '2', I_CALLING_SWITCH, '4', I_CALLING_SWITCH, '1', I_CALLED_SWITCH, '3', I_CALLED_SWITCH, '5', I_CALLED_SWITCH) I_SWITCH,
                            I_DEV_SWITCH I_DEV_SWITCH,
                            I_IN_ROUTE I_IN_ROUTE,
                            I_INOUT I_INOUT,
                            I_NET_CLS I_NET_CLS,
                            I_PREFIX I_PREFIX,
                            I_LINE_NO I_LINE_NO,
                            I_OUT_ROUTE I_OUT_ROUTE,
                            I_SERVICE_GRP I_SERVICE_GRP,
                            I_SERVICE I_SERVICE,
                            AMT_CALL/10 AMT_CALL,
                            I_CFC_GRP I_CFC_GRP,
                            I_CFC I_CFC
                FROM        srf_win
                WHERE       i_call_dt between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss')
                    AND     i_etl_dt between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss') + 1
                    AND (
                            (i_inout in ('0', '2', '4') AND i_out_ctn in ('01036931998'))
                            OR
                            (i_inout in ('1', '3', '5') AND i_in_ctn in ('01036931998'))
                        )
) SV ,
sd_com_bonbu,
sd_com_switch sd_com_switch,
sd_com_switch sd_com_switch_dev,
sd_com_service_grp,
sd_com_service,
sd_com_cfc_grp,
sd_com_cfc,
sd_prefix_org

WHERE       SV.I_BONBU = sd_com_bonbu.I_BONBU(+)
    AND     SV.I_SWITCH = sd_com_switch.I_SWITCH(+)
    AND     sd_com_switch.I_ENDT(+) = '99991231'
    AND     SV.I_DEV_SWITCH = sd_com_switch_DEV.I_SWITCH(+)
    AND     sd_com_switch_DEV.I_ENDT(+) = '99991231'
    AND     sd_com_switch_DEV.I_SWITCH_CLS(+) = 'W'
    and     to_number( decode(sd_com_service.i_service, 'X', 0, '0A2', 0, sd_com_service.i_service) ) < 2000
    AND     SV.I_SERVICE_GRP = sd_com_service_grp.I_SERVICE_GRP(+)
    AND     SV.I_SERVICE_GRP = sd_com_service.I_SERVICE_GRP(+)
    AND     SV.I_SERVICE = sd_com_service.I_SERVICE(+)
    AND     SV.I_SERVICE_GRP = sd_com_cfc.I_SERVICE_GRP(+)
    AND     SV.I_CFC = sd_com_cfc.I_CFC(+)
    AND     SV.I_SERVICE_GRP = sd_com_cfc_GRP.I_SERVICE_GRP(+)
    AND     SV.I_CFC_GRP = sd_com_cfc_GRP.I_CFC_GRP(+)
    AND     SV.I_PREFIX = SD_PREFIX_ORG.I_PREFIX(+)

ORDER BY 2 DESC