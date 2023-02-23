import requests
import json
import pandas as pd
from multiprocessing.pool import ThreadPool
import os


def parse_item(item):
    keys = ['id', 'title', 'abstract', 'keywords', 'origin',
            'submittedDate', 'mainCategory', 'asset', 'authors', 'citationsCount']
    parsed_item = {k: item[k] for k in keys}
    parsed_item['mainCategory'] = parsed_item['mainCategory']['name']
    parsed_item['asset'] = parsed_item['asset']['original']['url']
    parsed_item['authors'] = [a['firstName']+' '+a['lastName']
                              for a in parsed_item['authors']]
    return item['id'], parsed_item


def get_data(skip, output_dir):
    headers = {
        'accept': 'application/json, text/plain, */*',
        'content-type': 'application/json',
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en;q=0.9,',
        'cookie': 'orp_chemrxiv_sess=s%3Ac3TzBf5JLpLxI_BXEZTSSqS4zbcVmzBS.E0pCSjlLpqSHlNLRZ4NxV8croXlN%2BS8%2BTkriBClGpKA; _ga=GA1.2.204378878.1676638482; _gid=GA1.2.1606288349.1676638482; ln_or=eyIxMzQ4MzQ4IjoiZCJ9; _hjFirstSeen=1; _hjIncludedInSessionSample_2726500=0; _hjSession_2726500=eyJpZCI6IjhhZTgzZDc2LWI0MWMtNDE1NS05YmJiLWNmZTNjZGQxNjkzNSIsImNyZWF0ZWQiOjE2NzY2Mzg0ODE5NzQsImluU2FtcGxlIjpmYWxzZX0=; _hjIncludedInPageviewSample=1; _hjAbsoluteSessionInProgress=0; ELOQUA=GUID=F9383A9B1E5F49259F6D63A4C2634226; site24x7rumID=9870242232781788.1676638516955.1676638516959; _hjSessionUser_2726500=eyJpZCI6ImM3Yzc4MDZmLTRmNDYtNTUxNS04NWY0LWNkZjM1NjExZmI4MSIsImNyZWF0ZWQiOjE2NzY2Mzg0ODE5NDksImV4aXN0aW5nIjp0cnVlfQ==',
        'origin': 'https://chemrxiv.org',
        'referer': 'https://chemrxiv.org/engage/chemrxiv/search-dashboard',
        'sec-ch-ua': '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': ''"Windows",
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'x-api-key': 'y6nWHrymZysXc'
    }

    req_json = {
        'query': "query searchDashboardPageLoad(\n  $text: String = \"\",\n  $subjects: [String!],\n  $categories: [String!],\n  $events: [String!],\n  $publishedDates: [String!],\n  $partners: [String!],\n  $contents: [String!],\n  $keywords: [String!],\n  $authors: String = \"\",\n  $skip: Int = 0,\n  $limit: Int = 10,\n  $sortBy: SortByEnum = RELEVANT_DESC\n  ) {\n  viewer {\n    usageEventsDisabled\n\n    user {\n      ...userRoleFragment\n    }\n\n    searchItems(\n      searchTerm: $text,\n      subjectKeys: $subjects,\n      categoryKeys: $categories,\n      eventKeys: $events,\n      publishedDateKeys: $publishedDates,\n      partnerKeys: $partners,\n      contentTypeKeys: $contents,\n      keywordsKeys: $keywords,\n      searchAuthor: $authors,\n      skip: $skip,\n      limit: $limit,\n      sortBy: $sortBy,\n      includeBuckets: true\n      ) {\n      totalCount\n\n      results: itemHits {\n        highlight {\n          text\n          matchPositions {\n            start\n            end\n          }\n        }\n\n        item {\n          ...itemMatchFragment\n        }\n      }\n\n      subjectBuckets {\n        ...searchBucketFragment\n      }\n\n      categoryBuckets {\n        ...searchBucketFragment\n      }\n\n      eventBuckets {\n        ...searchBucketFragment\n      }\n\n      partnerBuckets {\n        ...searchBucketFragment\n      }\n\n      publishedDateBuckets {\n        ...searchBucketFragment\n      }\n\n      contentBuckets: contentTypeBuckets {\n        ...searchBucketFragment\n      }\n\n      dateBuckets: publishedDateBuckets {\n        ...searchBucketFragment\n      }\n    }\n\n    subjectTypes: subjects {\n      ...subjectTypeFragment\n    }\n\n    contentTypes {\n      ...contentTypeFragment\n    }\n\n    categoryTypes: categories {\n      ...categoryTypeFragment\n    }\n  }\n}\n\nfragment userRoleFragment on User {\n  __typename\n  id\n  sessionExpiresAt\n  titleTypeId: title\n  firstName\n  lastName\n  emailAddress : email\n  orcid\n  roles\n  accountType\n}\n\nfragment itemMatchFragment on MainItem {\n  __typename\n  id\n  title\n  abstract\n  keywords\n  origin\n  submittedDate\n  subjectType: subject {\n    ...subjectTypeFragment\n  }\n  contentType {\n    ...contentTypeFragment\n  }\n  categoryTypes: categories {\n    ...categoryTypeFragment\n  }\n  mainCategory {\n    name\n  }\n  asset{\n    mimeType\n    original{\n      url\n    }\n  }\n  authors {\n    title\n    firstName\n    lastName\n    authorConfirmationId\n    displayOrder\n  }\n  metrics {\n    metricType\n    description\n    value\n    unit\n  }\n  citationsCount\n  community {\n    id\n    name\n  }\n}\n\nfragment searchBucketFragment on SearchBucket {\n  __typename\n  count\n  key\n  label\n}\n\nfragment subjectTypeFragment on Subject {\n  __typename\n  id\n  name\n  description\n}\n\nfragment contentTypeFragment on ContentType {\n  __typename\n  id\n  name\n  allowSubmission\n  allowJournalSubmission\n  allowCommunitySubmission\n  allowResearchDirectionSubmission\n  videoAllowedCheck\n  allowedFileTypes\n  allowedVideoFileTypes\n}\n\nfragment categoryTypeFragment on Category {\n  __typename\n  id\n  name\n  description\n  parentId\n}\n",
        'variables': {
            'categories': [],
            'contents': [],
            'events': [],
            'keywords': [],
            'partners': [],
            'publishedDates': [],
            'skip': skip,
            'subjects': []
        }
    }

    url = 'https://chemrxiv.org/engage/api-gateway/chemrxiv/graphql'
    res = requests.post(url, headers=headers, json=req_json)
    data = res.json()['data']['viewer']['searchItems']['results']
    items = {parse_item(d['item'])[0]: parse_item(d['item'])[1] for d in data}
    df = pd.read_json(json.dumps(items), orient='index')
    df.to_parquet(f'{output_dir}/{skip}.parquet', index=False)


if __name__ == '__main__':

    output_dir = 'CHEMRXIV'
    os.makedirs(output_dir, exist_ok=True)
    skips = [i for i in range(0, 16531, 10)]
    num_procs = 5
    with ThreadPool(num_procs) as p:
        p.starmap(get_data, [(skip, output_dir) for skip in skips])
