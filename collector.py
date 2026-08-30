"""
Railway Cron에서 30분마다 실행할 파일입니다.
실행이 끝나면 바로 종료되므로 Cron Job 용도에 맞습니다.
"""

from app import init_db, collect_all_categories, summarize_pending_articles, get_summary_quota_status


def main():
    init_db()
    stats = collect_all_categories(generate_summaries=True)

    # 과거에 저장됐지만 요약이 비어 있는 기사도 매 실행마다 최대 10개 보충
    backfill = summarize_pending_articles(limit=10)
    quota = get_summary_quota_status()

    print("=== GPA NEWS COLLECTOR COMPLETE ===")
    print(f"Feeds checked      : {stats['feeds_checked']}")
    print(f"Matched articles   : {stats['matched_articles']}")
    print(f"New articles       : {stats['new_articles']}")
    print(f"New summaries      : {stats['summaries_created']}")
    print(f"Backfill checked   : {backfill['checked']}")
    print(f"Backfill summaries : {backfill['summarized']}")
    print(f"Backfill failed    : {backfill['failed']}")
    print(f"Auto quota today   : {quota['auto_used']}/{quota['auto_limit']}")
    print(f"Manual quota today : {quota['manual_used']}/{quota['manual_limit']}")
    print(f"External quota hit : {stats.get('auto_quota_exhausted', False) or backfill.get('external_quota_exhausted', False)}")
    print(f"Errors             : {stats['errors']}")


if __name__ == "__main__":
    main()
