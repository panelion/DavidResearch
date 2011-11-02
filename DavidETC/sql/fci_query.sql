SELECT
        'NOTSUM' "NOTSUM"
        ,TO_CHAR(A.I_CALL_DT,'yyyy-mm-dd hh24:mi:ss') "시화시각"
        ,A.I_CHARGING_NUM      "과금번호"
        ,DECODE(A.I_INOUT,'0','발신','1','착신','T','종합','') "발착신구분"
        ,A.I_CALLING_SWITCH    "발신교환기"
        ,A.I_BSC               "BSC"
        ,SD_COM_CELL.T_CELL    "기지국"
        ,A.I_CELL              "기지국코드"
        ,A.I_SECTOR            "SECTOR"
        ,A.I_CALL_TERM_TIME    "종료시각"
        ,B.T_SERVICE           "지능망서비스종류"
        ,A.I_CALL_ACCESS_TYPE  "호접속형태"
        ,A.I_CALL_TERM_CAUSE   "불완료호코드"
        ,C.T_CFC               "불완료원인명"
        ,A.I_WS_TYPE           "WS Type(발신서비스)"
        ,A.I_CHARGING_IND      "과금주체"
        ,A.I_AIR_TIME_CHARGE   "Air Time Charge"
        ,A.I_DISCOUNT_PLAN "할인유형"
        ,A.I_CALLING_SPC_INFO  "발신 SPC 정보"
FROM
        SRF_WCD_FCI A,
        SASCOMM.SD_COM_SERVICE B,
        SASCOMM.SD_COM_CFC C,
        SASCOMM.SD_COM_SWITCH,
        SASCOMM.SD_COM_CELL

WHERE
            A.I_CALL_DT BETWEEN to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss')
        AND A.I_ETL_DT between to_date('20101223000000', 'yyyymmddhh24miss')  and to_date('20101223235959',  'yyyymmddhh24miss') + 1
        AND (
            (A.i_inout in ('0') and A.i_out_ctn in ('01036931998'))
                OR
            (A.i_inout in ('1') and A.i_in_ctn in ('01036931998'))
        )
        AND B.I_SERVICE_GRP = '11'
        AND C.I_SERVICE_GRP = '11'
        AND A.I_SERVICE_KEY = B.I_SERVICE(+)
        AND A.I_CALLING_SWITCH = SD_COM_SWITCH.I_SWITCH(+)
        AND SD_COM_SWITCH.I_ENDT(+) = '99991231'
        AND A.I_CELL = SD_COM_CELL.I_CELL(+)
        AND A.I_CALLING_SWITCH = SD_COM_CELL.I_SWITCH(+)
        AND A.I_BSC = SD_COM_CELL.I_BSC(+)
        AND SD_COM_CELL.I_ENDT(+) = '99991231'
        AND A.I_CALL_TERM_CAUSE = C.I_CFC(+)

ORDER BY 2 DESC