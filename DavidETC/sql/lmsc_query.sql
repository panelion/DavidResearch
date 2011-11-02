SELECT
        'NOTSUM' "NOTSUM",
        SYSTEMNUM "시스템넘버",
        APPID "서비스타입",
        I_CALLING_CTN "발신번호",
        I_CALLED_CTN "착신번호",
        I_SERVICE_FLAG "Service Flag",
        I_REAL_CTN "Real Number",
        I_CALLBACK_NUM "발신자 Call Back 번호",
        I_MSG_STATUS "메시지상태",
        I_RESULT_FLAG "성공여부",
        RETRY_COUNT "재전송횟수",
        MESSAGE_SIZE "전체 사이즈",
        I_CHARGE_FLAG "과금대상",
        I_SOC "과금 SOC 종류",
        I_GET_TYPE "메시지 착신 방식",
        to_char(I_POST_START_TIME, 'yyyy-mm-dd hh24:mi:ss') "LMSC에서LMS메시지 수신시작시각",
        to_char(I_POST_END_TIME, 'yyyy-mm-dd hh24:mi:ss') "LMSC에서LMS메시지 수신종료시각",
        to_char(I_GET_START_TIME, 'yyyy-mm-dd hh24:mi:ss') "수신자 LMS메시지 수신시작시각",
        to_char(I_GET_END_TIME, 'yyyy-mm-dd hh24:mi:ss') "수신자 LMS메시지 수신종료시각",
        I_NCN "I_NCN",
        to_char(I_CALL_DT, 'yyyy-mm-dd hh24:mi:ss') "메시지 수신 시각"

FROM    SRF_LMSS_LOG

WHERE
            I_CALL_DT between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss')
    AND     I_ETL_DT between to_date('20101223000000', 'yyyymmddhh24miss')+4 and to_date('20101223235959', 'yyyymmddhh24miss') + 4

    AND     (
                    I_CALLING_CTN in ('01036931998')
                OR  I_CALLED_CTN in ('01036931998')
            )

ORDER BY 2 DESC