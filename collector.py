"""
Railway Cron에서 30분마다 실행되는 뉴스 + 무료 공개 소셜 수집기입니다.
"""

from app import (
    AUTO_SUMMARY_DAILY_LIMIT,
    collect_all_categories,
    collect_social_posts,
    get_summary_quota_status,
    init_db,
    summarize_pending_articles,
)


def main():
    init_db()

    # 먼저 뉴스/소셜을 빠르게 저장
    news_stats = collect_all_categories(generate_summaries=False)
    social_stats = collect_social_posts()

    # 뉴스+소셜 전체 중 가장 최근 미요약 콘텐츠부터 하루 자동 10개
    auto_summary = summarize_pending_articles(
        limit=AUTO_SUMMARY_DAILY_LIMIT
    )
    quota = get_summary_quota_status()

    print("=== GPA NEWS + SOCIAL COLLECTOR COMPLETE ===")
    print(f"News feeds checked : {news_stats['feeds_checked']}")
    print(f"Matched articles   : {news_stats['matched_articles']}")
    print(f"New articles       : {news_stats['new_articles']}")
    print(f"Social profiles    : {social_stats['profiles_checked']}")
    print(f"Social posts seen  : {social_stats['matched_posts']}")
    print(f"New social posts   : {social_stats['new_posts']}")
    print(f"Auto summaries     : {auto_summary['summarized']}")
    print(f"Auto summary failed: {auto_summary['failed']}")
    print(f"Auto quota today   : {quota['auto_used']}/{quota['auto_limit']}")
    print(f"Manual quota today : {quota['manual_used']}/{quota['manual_limit']}")
    print(
        "External quota hit : "
        f"{auto_summary.get('external_quota_exhausted', False)}"
    )
    print(
        "Errors             : "
        f"{news_stats['errors'] + social_stats['errors']}"
    )


if __name__ == "__main__":
    main()
