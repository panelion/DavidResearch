#!/bin/sh

# JAVA_HOME PATH
JAVA_HOME=/usr/bin/java

# Nexr Search Indexer Home
CLIENT_HOME=/home/nexr/cdr/nexrsearch_client

# CDR DATA File Directory
DATA_HOME=/data1/nexrsearch_data

# Client Config File Path
CDR_CONFIG_FILE_PATH="$CLIENT_HOME/config/properties/CdrClient.conf"

# CDR Data File
#
# EX >
# 11/07/05,11/07/05,22,1,01051419472,410,01025039472,410,296,265001,255,319,1,11,0,441,X,450082960052070,0,0,,0,
# 인천,중구,북성동3가,10,0,0,16,1,4,01051,890,130,1,0,1,SWYOI4_FW3G5MSC_ID1339_T20110705221046.DAT,6015,486323592,
# N,KTF,1,2,1,5451,BIGIRWPRI,KTF,450082960052070,,91821025039472,,A101051419472,,,,91821029190319,3404,0858,,,,,
# ,,,,,2,11,,,,,,,,,,,,,,,,,,,,11,1107052208477,,,,,,,,,,,,,,,535884,201107052208459,201107052208585,201107052210268,
# 883,,,0,,11/07/05,,,,,,,,,,
#
# split : ","
# row split : "\n"
#
#
CDR_DATA_FILE_PATH="$DATA_HOME/srf_wcd_voice_data.csv"

# record to the Indexing Count by Time Per Second.
LOG_FILE_PATH="$CLIENT_HOME/logs/IndexTpsLog01.log"

# boolean CDR Or SDP
IS_CDR=true

# CDR Data Column Define File Path
CDR_COLUMN_FILE_PATH="$CLIENT_HOME/config/wcd_column.txt"

# Indexing Server IP Address
CDR_DATA_IP="172.31.200.46"

# Cdr sd_com_cell Table Data file Path
CDR_SC_FILE_PATH="$CLIENT_HOME/config/sd_com_cell.txt"

# Cdr sd_com_sec Table Data File Path
CDR_SCEC_FILE_PATH="$CLIENT_HOME/config/sd_com_sec.txt"

# Cdr Indexing Column Define File Path
CDR_USED_COLUMN_FILE_PATH="$CLIENT_HOME/config/wcd_used_column.txt"


# Function Client Indexer
run_client () {
        CLASSPATH="$CLASSPATH:$CLIENT_HOME/lib/NexrSearchClient.jar"

        echo "Start CDR Client"

        $JAVA_HOME -Xms2g -Xmx2g -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -classpath $CLASSPATH com.nexr.platform.search.ClientIndexer
        $CDR_CONFIG_FILE_PATH $CDR_DATA_FILE_PATH $LOG_FILE_PATH $IS_CDR $CDR_COLUMN_FILE_PATH $CDR_DATA_IP $CDR_SC_FILE_PATH $CDR_SCEC_FILE_PATH $CDR_USED_COLUMN_FILE_PATH
}

echo "***************************************"
echo "Nexr Search Client Indexing."
echo "***************************************"


# Run Client Indexer
run_client
