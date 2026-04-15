"""
HTML/XML templates for API responses.

Templates use f-string style placeholders that need to be formatted at runtime.
"""

from html import escape

# === RSS Templates ===

RSS_ITEM_TEMPLATE = """
    <item>
      <title>{file_name}</title>
      <link>{download_url}</link>
      <description>Size: {size_str}</description>
      <pubDate>{pub_date}</pubDate>
    </item>"""

RSS_XML_TEMPLATE = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>TgToFileSystem Search: {keyword}</title>
    <link>{exposed_url}</link>
    <description>Telegram media search results</description>
    <language>zh-CN</language>
{rss_items}
  </channel>
</rss>"""

RSS_ERROR_TEMPLATE = "<error>{error}</error>"


# === Ani Search Templates ===

ANI_SEARCH_HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>TgToFileSystem Search: {keyword}</title>
</head>
<body>
  <div class="module-search">
    <div class="module-card-list">
    <div class="module-card-item">
      <div class="module-card-item-info">
        <div class="module-card-item-title">
          <a href="{detail_url}" title="{keyword}">{keyword}</a>
        </div>
        <div class="module-card-item-desc">点击查看所有匹配结果</div>
      </div>
    </div>
    </div>
  </div>
</body>
</html>"""

ANI_SEARCH_ERROR_TEMPLATE = "<html><body><error>{error}</error></body></html>"


# === Ani Detail Templates ===

ANI_DETAIL_ITEM_TEMPLATE = """    <div class="module-card-item">
      <div class="module-card-item-info">
        <div class="module-card-item-title">
          <a href="{download_url}" title="{file_name}">{file_name}</a>
        </div>
        <div class="module-card-item-desc">{size_str}</div>
      </div>
    </div>"""

ANI_DETAIL_HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>TgToFileSystem Detail: {keyword}</title>
</head>
<body>
  <div class="module-play">
    <div class="module-card-list">
{html_items}
    </div>
  </div>
</body>
</html>"""

ANI_DETAIL_ERROR_TEMPLATE = "<html><body><error>{error}</error></body></html>"


# === Helper Functions ===

def format_rss_item(file_name: str, download_url: str, size_str: str, pub_date: str) -> str:
    """Format RSS item with HTML escaping."""
    return RSS_ITEM_TEMPLATE.format(
        file_name=escape(file_name),
        download_url=download_url,
        size_str=size_str,
        pub_date=pub_date,
    )


def format_rss_xml(keyword: str, exposed_url: str, rss_items: str) -> str:
    """Format RSS XML with HTML escaping."""
    return RSS_XML_TEMPLATE.format(
        keyword=escape(keyword),
        exposed_url=exposed_url,
        rss_items=rss_items,
    )


def format_ani_search_html(keyword: str, detail_url: str) -> str:
    """Format ani search HTML with HTML escaping."""
    return ANI_SEARCH_HTML_TEMPLATE.format(
        keyword=escape(keyword),
        detail_url=detail_url,
    )


def format_ani_detail_item(file_name: str, download_url: str, size_str: str) -> str:
    """Format ani detail item HTML with HTML escaping."""
    return ANI_DETAIL_ITEM_TEMPLATE.format(
        file_name=escape(file_name),
        download_url=download_url,
        size_str=size_str,
    )


def format_ani_detail_html(keyword: str, html_items: str) -> str:
    """Format ani detail HTML with HTML escaping."""
    return ANI_DETAIL_HTML_TEMPLATE.format(
        keyword=escape(keyword),
        html_items=html_items,
    )