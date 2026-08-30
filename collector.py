"""
Railway Cron에서 30분마다 실행할 파일입니다.
실행이 끝나면 바로 종료되므로 Cron Job 용도에 맞습니다.
"""

from app import init_db, collect_all_categories


def main():
    init_db()
    stats = collect_all_categories()

    print("=== GPA NEWS COLLECTOR COMPLETE ===")
    print(f"Feeds checked      : {stats['feeds_checked']}")
    print(f"Matched articles   : {stats['matched_articles']}")
    print(f"New articles       : {stats['new_articles']}")
    print(f"Summaries created  : {stats['summaries_created']}")
    print(f"Errors              : {stats['errors']}")


if __name__ == "__main__":
    main()
