SELECT
            'NOTSUM' "NOTSUM",
            I_ORIGNATING_CTN "발신 번호",
            I_DESTINATION_CTN "착신 번호",
            DECODE(I_INOUT, '0', '발신', '1', '착신', '기타') "발착신구분",
            I_FORWARD_CTN "착신전환 번호",
            I_CALLBACK_NUMBER "CALLBACK번호",
            TO_CHAR(I_CALL_DT, 'YYYY-MM-DD HH24:MI:SS') "메시지 수신 시각",
            TO_CHAR(I_SUBMIT_TIME, 'YYYY-MM-DD HH24:MI:SS') "메시지 전송 시각",

            --I_EXPIRATION_TIME     "메시지 폐기 시각",
            I_TELESERVICE_ID "텔레서비스ID",
            RETRY_NUMBER "동일 이유에 대한 재전송 횟수",
            RETRY_NO_FOR_ERR "동일 에러로 인한 재전송 횟수",

            --MAX_TRY_NUMBER            "최대 재전송 횟수",
            I_CALLING_FEATURE_IND "지능망 코드",
            I_CALLED_FEATURE_IND "부가서비스",

            --CHARGE_AMT                "지능망 과금",

            --I_INTELLIGENT_SVC_ID  "제공",
            I_PORTABILITY_TYPE "번호 이동 지시자",
            I_PORTABILITY_OP "번호 이동 정보",
            I_ORIG_VAD_SVC "발신 부가 서비스",
            I_DEST_VAD_SVC "착신 부가 서비스",
            I_ORIG_MSC_PC "발신 교환기",
            I_DEST_MSC_PC "착신 교환기",
            I_HLR_INFO_PC "HLR",
            I_CHARGE_IND "과금 정보",
            I_BILL_TYPE "과금 여부",
            I_MSG_PROC_RESULT "처리결과",
            I_CFC "불완료원인",
            I_MSG_TYPE "메시지 타입",
            I_ROUTING_DIGIT "I_ROUTING_DIGIT",
            I_SERVICE_ORG "이전사업자",
            I_TARGET_ORG "타겟사업자",
            I_CALLING_SWITCH "발신교환기",
            I_CALLED_SWITCH "착신교환기",
            I_WCDMA_SWITCH "WCDMA교환기"

FROM  srf_smss_log

WHERE       i_call_dt between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss')
    AND     i_etl_dt between to_date('20101223000000', 'yyyymmddhh24miss') and to_date('20101223235959', 'yyyymmddhh24miss')+1
    AND (
                (i_inout in ('0') AND i_out_ctn in ('01036931998'))
            OR
                (i_inout in ('1') AND i_in_ctn in ('01036931998'))
        )

ORDER BY 2 DESC