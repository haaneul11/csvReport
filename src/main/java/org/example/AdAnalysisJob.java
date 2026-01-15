package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class AdAnalysisJob {
    public static void main(String[] args) {

        // 1. SparkSession 생성
        SparkSession spark = SparkSession.builder()
                .appName("Ad Effect Analysis Job")
                .master("local[*]")  // 로컬 모드로 실행 (클러스터에서는 적절한 마스터 URL로 변경)
                .getOrCreate();

        // 2. CSV 파일에서 로그 데이터를 읽어옴
        String inputPath = "/Users/jdaddy/sampleData";
        Dataset<Row> logs = spark.read()
                .option("header", "true")  // 헤더가 있는 CSV 파일
                .csv(inputPath);

        // 3. 전체 세션 수 계산
        long totalSessions = logs.count();

        // 4. 광고를 본 세션 필터링 ('/ad'가 포함된 세션)
        Dataset<Row> journeysWithAds = logs.filter(col("url_path").contains("/ad"));
        long adSessions = journeysWithAds.count();

        // 5. 광고를 본 후 '/checkout'까지 도달한 세션 계산
        Dataset<Row> journeysWithCheckout = journeysWithAds
                .withColumn("checkout_flag", col("url_path").contains("/checkout"));
        long successfulAdSessions = journeysWithCheckout
                .filter(col("checkout_flag").equalTo(true))
                .count();

        // 6. 비율 계산
        double adViewRate = (double) adSessions / totalSessions * 100;  // 전체에서 광고를 본 비율
        double adToPurchaseRate = (double) successfulAdSessions / adSessions * 100;  // 광고를 본 후 구매한 비율

        // 7. 결과 출력
        System.out.println("전체 세션 수: " + totalSessions);
        System.out.println("광고를 본 세션 수: " + adSessions);
        System.out.println("전체에서 광고를 본 비율: " + adViewRate + "%");
        System.out.println("광고를 본 후 구매한 비율: " + adToPurchaseRate + "%");

        // Spark 세션 종료
        spark.stop();
    }
}