"""
Railway Cron에서 30분마다 실행되는 V3 Hybrid 뉴스 + 공개 소셜 수집기입니다.
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

    # Hybrid news collection:
    # 직접 센싱 + Google News RSS 보조망
    news_stats = collect_all_categories(
        generate_summaries=False
    )
    social_stats = collect_social_posts()

    # 뉴스+소셜 전체 중 가장 최근 미요약 콘텐츠부터 하루 자동 10개
    auto_summary = summarize_pending_articles(
        limit=AUTO_SUMMARY_DAILY_LIMIT
    )
    quota = get_summary_quota_status()

    direct = news_stats.get("direct", {})

    print(
        "=== GPA V3 HYBRID NEWS + SOCIAL COLLECTOR COMPLETE ==="
    )
    print(
        f"Direct sources enabled : "
        f"{direct.get('sources_enabled', 0)}"
    )
    print(
        f"Direct sources checked : "
        f"{direct.get('sources_checked', 0)}"
    )
    print(
        f"Direct candidate URLs   : "
        f"{direct.get('candidate_urls', 0)}"
    )
    print(
        f"Direct pages checked    : "
        f"{direct.get('pages_checked', 0)}"
    )
    print(
        f"Direct matched articles : "
        f"{direct.get('matched_articles', 0)}"
    )
    print(
        f"Direct new articles     : "
        f"{direct.get('new_articles', 0)}"
    )
    print(
        f"Direct page failures    : "
        f"{direct.get('page_failures', 0)}"
    )
    print(
        f"Direct source failures  : "
        f"{direct.get('source_failures', 0)}"
    )
    print(
        f"Google feeds checked    : "
        f"{news_stats['feeds_checked']}"
    )
    print(
        f"Google matched articles : "
        f"{news_stats['matched_articles']}"
    )
    print(
        f"Google new articles     : "
        f"{news_stats.get('google_new_articles', 0)}"
    )
    print(
        "Google source rejected  : 0 (SAFE mode: 기존 Google 결과 유지)"
    )
    print(
        f"Total new articles      : "
        f"{news_stats['new_articles']}"
    )
    print(
        f"Social profiles         : "
        f"{social_stats['profiles_checked']}"
    )
    print(
        f"Social posts seen       : "
        f"{social_stats['matched_posts']}"
    )
    print(
        f"New social posts        : "
        f"{social_stats['new_posts']}"
    )
    print(
        f"Auto summaries          : "
        f"{auto_summary['summarized']}"
    )
    print(
        f"Auto summary failed     : "
        f"{auto_summary['failed']}"
    )
    print(
        f"Auto quota today        : "
        f"{quota['auto_used']}/{quota['auto_limit']}"
    )
    print(
        f"Manual quota today      : "
        f"{quota['manual_used']}/{quota['manual_limit']}"
    )
    print(
        "External quota hit      : "
        f"{auto_summary.get('external_quota_exhausted', False)}"
    )
    print(
        "Google RSS errors       : "
        f"{news_stats['errors']}"
    )
    print(
        "Social errors           : "
        f"{social_stats['errors']}"
    )


if __name__ == "__main__":
    main()
