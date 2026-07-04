import sys
import os
import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.schemas import ALL_TOPICS

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description="DLQ Monitoring Job")
    parser.add_argument("--threshold", type=float, default=5.0, help="Ngưỡng cảnh báo DLQ (tính theo phần trăm, mặc định 5.0)")
    parser.add_argument("--hours", type=int, default=1, help="Số giờ cần quét lùi về quá khứ (mặc định 1)")
    args = parser.parse_args()

    threshold_percent = args.threshold
    hours_back = args.hours

    spark = SparkSession.builder \
        .appName("DLQMonitoring") \
        .master("spark://spark-master:7077") \
        .enableHiveSupport() \
        .getOrCreate()

    has_error = False
    logger.info(f"Bắt đầu quét DLQ trong {hours_back} giờ qua với ngưỡng cảnh báo {threshold_percent}%")

    for topic in ALL_TOPICS:
        main_table = f"lakehouse.bronze.{topic}"
        dlq_table = f"lakehouse.bronze.dlq_{topic}"
        
        # Kiểm tra bảng tồn tại
        if not spark.catalog.tableExists(main_table) or not spark.catalog.tableExists(dlq_table):
            logger.warning(f"Bỏ qua topic '{topic}' vì bảng gốc hoặc bảng DLQ chưa được khởi tạo.")
            continue
            
        time_filter = f"ingested_at >= current_timestamp() - INTERVAL {hours_back} HOURS"

        try:
            valid_count = spark.table(main_table).where(time_filter).count()
            dlq_count = spark.table(dlq_table).where(time_filter).count()
            
            total_count = valid_count + dlq_count
            
            if total_count == 0:
                logger.info(f"Topic '{topic}': Không có dữ liệu mới trong {hours_back} giờ qua.")
                continue
                
            error_rate = (dlq_count / total_count) * 100
            
            if error_rate > threshold_percent:
                logger.error(f"[ALERT] Topic '{topic}' vượt ngưỡng lỗi! "
                             f"Error Rate: {error_rate:.2f}% (Valid: {valid_count}, DLQ: {dlq_count})")
                has_error = True
            else:
                logger.info(f"[OK] Topic '{topic}' an toàn. "
                            f"Error Rate: {error_rate:.2f}% (Valid: {valid_count}, DLQ: {dlq_count})")
                
        except Exception as e:
            logger.error(f"Lỗi khi quét topic '{topic}': {str(e)}")
            has_error = True

    spark.stop()

    if has_error:
        logger.error("Phát hiện DLQ vượt ngưỡng. Quăng lỗi để kích hoạt báo động (Alert)!")
        sys.exit(1)
    else:
        logger.info("Tất cả các topic đều ở dưới ngưỡng cảnh báo an toàn.")
        sys.exit(0)

if __name__ == "__main__":
    main()
