"""
LiveJournal Post Archiver — curl_cffi edition
==============================================
Без прокси, без UI-браузера. Используем curl_cffi с Chrome TLS impersonation
для обхода антибот-защиты ЖЖ и JSON-данные из Site.page для комментариев.

Установка:
    pip install curl_cffi lxml

Запуск:
    python lj_scraper_scrapling.py
"""

import re
import json
import random
import hashlib
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, unquote
from lxml import etree
from curl_cffi.requests import AsyncSession

# Подавляем INFO-логи от curl_cffi
logging.getLogger("curl_cffi").setLevel(logging.WARNING)
logging.getLogger().setLevel(logging.WARNING)

# ═══════════════════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════════════════

DELAY_MIN = 0.3   # минимальная пауза между запросами (сек)
DELAY_MAX = 0.8   # максимальная пауза

# Сколько thread-URL загружать одновременно (async)
PARALLEL_THREADS = 8

# Сколько постов скачивать параллельно (потоки)
PARALLEL_POSTS = 4

# Диапазон допустимых годов
YEAR_MIN = 2000
YEAR_MAX = 2030

_HTML_PARSER = etree.HTMLParser(encoding="utf-8")


# ═══════════════════════════════════════════════════════════════
#  HTTP-УТИЛИТЫ (curl_cffi для fetch, lxml для parse)
#  Единая AsyncSession на весь прогон — как настоящий браузер
# ═══════════════════════════════════════════════════════════════

_CHROME_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,ru;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Ch-Ua": '"Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
}


def _parse_html(body_bytes: bytes) -> etree._Element:
    """Парсит HTML-байты в lxml-дерево с правильной кодировкой."""
    return etree.fromstring(body_bytes, _HTML_PARSER)


async def _async_get(session: AsyncSession, url: str, retries: int = 3, timeout: int = 20):
    """Асинхронный GET с retry и exponential backoff."""
    for attempt in range(retries):
        try:
            resp = await session.get(url, timeout=timeout)
            return resp
        except Exception:
            if attempt < retries - 1:
                wait = 2 ** attempt + random.uniform(0.5, 1.5)
                await asyncio.sleep(wait)
            else:
                raise


async def async_fetch_and_parse(session: AsyncSession, url: str,
                                 retries: int = 3, timeout: int = 20) -> tuple:
    """Асинхронный GET + parse. Возвращает (doc, body_text, status)."""
    resp = await _async_get(session, url, retries=retries, timeout=timeout)
    raw = resp.content
    text = raw.decode("utf-8", errors="ignore")
    doc = _parse_html(raw) if resp.status_code == 200 else None
    return doc, text, resp.status_code


async def async_fetch_text(session: AsyncSession, url: str,
                            retries: int = 3, timeout: int = 20) -> str:
    """Асинхронный GET, возвращает UTF-8 текст."""
    resp = await _async_get(session, url, retries=retries, timeout=timeout)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} for {url}")
    return resp.content.decode("utf-8", errors="ignore")


# ═══════════════════════════════════════════════════════════════
#  УТИЛИТЫ
# ═══════════════════════════════════════════════════════════════

def extract_post_id(url: str) -> str | None:
    m = re.search(r'/(\d+)\.html', url)
    return m.group(1) if m else None


def extract_site_page(body_text: str) -> dict | None:
    """Извлекает объект Site.page из JavaScript на странице ЖЖ."""
    m = re.search(r'Site\.page\s*=\s*\{', body_text)
    if not m:
        return None
    json_start = body_text.index('{', m.start())
    depth = 0
    for ci, c in enumerate(body_text[json_start:], json_start):
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
        if depth == 0:
            try:
                return json.loads(body_text[json_start:ci + 1])
            except json.JSONDecodeError:
                return None
    return None


def _el_text(el) -> str:
    """Извлекает весь текст из lxml-элемента (включая вложенные)."""
    if el is None:
        return ""
    return "".join(el.itertext()).strip()


def _el_inner_html(el) -> str:
    """Извлекает inner HTML lxml-элемента."""
    if el is None:
        return ""
    html = etree.tostring(el, encoding="unicode", method="html")
    # Убираем внешний тег
    tag = el.tag
    html = re.sub(rf'^<{tag}[^>]*>', '', html, count=1)
    html = re.sub(rf'</{tag}>$', '', html, count=1)
    return html


# Маппинг Content-Type → расширение файла
_MIME_TO_EXT = {
    "image/jpeg": ".jpg", "image/jpg": ".jpg", "image/png": ".png",
    "image/gif": ".gif", "image/webp": ".webp", "image/svg+xml": ".svg",
    "image/bmp": ".bmp", "image/tiff": ".tiff",
}


def _ext_from_bytes(data: bytes) -> str:
    if data[:2] == b'\xff\xd8':           return ".jpg"
    if data[:8] == b'\x89PNG\r\n\x1a\n': return ".png"
    if data[:6] in (b'GIF87a', b'GIF89a'): return ".gif"
    if data[:4] == b'RIFF' and data[8:12] == b'WEBP': return ".webp"
    if data[:5] == b'<?xml' or data[:4] == b'<svg': return ".svg"
    return ""


def safe_filename(url: str, ext_hint: str = "") -> str:
    parsed = urlparse(url)
    name = Path(unquote(parsed.path)).name
    if not name or len(name) > 80:
        name = hashlib.md5(url.encode()).hexdigest()[:12]
    name = name.split("?")[0]
    stem = Path(name).stem or name
    url_ext = Path(name).suffix.lower()
    if ext_hint:
        return stem + ext_hint
    if url_ext in (".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".tiff"):
        return stem + url_ext
    return stem


async def download_image(session: AsyncSession, url: str, dest_dir: Path) -> str | None:
    if url.startswith("//"):
        url = "https:" + url
    if not url.startswith("http"):
        return None

    base_name = safe_filename(url)
    for existing in dest_dir.glob(f"{base_name}.*"):
        return existing.name
    if (dest_dir / base_name).exists() and Path(base_name).suffix:
        return base_name

    try:
        resp = await _async_get(session, url, timeout=20, retries=2)
        if resp.status_code != 200:
            return None
        data = resp.content
        if not data:
            return None
    except Exception:
        return None

    ct = resp.headers.get("content-type", "")
    ext = _MIME_TO_EXT.get(ct.split(";")[0].strip().lower(), "")
    if not ext:
        ext = _ext_from_bytes(data)

    filename = safe_filename(url, ext_hint=ext)
    (dest_dir / filename).write_bytes(data)
    return filename


async def process_html_images_and_links(session: AsyncSession, html_str: str,
                                         archive_dir: Path,
                                         author_base: str = "") -> str:
    """Обрабатывает HTML-фрагмент: скачивает картинки, локализует ссылки."""
    if not html_str or not html_str.strip():
        return html_str

    wrapped = f"<div>{html_str}</div>"
    try:
        parser = etree.HTMLParser(encoding="utf-8")
        doc = etree.fromstring(wrapped.encode("utf-8"), parser)
    except Exception:
        return html_str

    # Скачиваем картинки
    for img in doc.iter("img"):
        src = img.get("src") or img.get("data-src") or img.get("data-lazy-src")
        if not src or src.startswith("data:"):
            continue
        local = await download_image(session, src, archive_dir)
        if local:
            img.set("src", local)
            for attr in ("data-src", "data-lazy-src", "loading"):
                if img.get(attr) is not None:
                    del img.attrib[attr]
        await asyncio.sleep(random.uniform(0.05, 0.15))

    # Локализуем внутренние ссылки
    if author_base:
        parsed = urlparse(author_base)
        host = parsed.netloc  # e.g. palaman.livejournal.com
        author = host.split(".")[0]  # e.g. palaman
        # Основной паттерн: palaman.livejournal.com/12345.html
        # + альтернативный: www.livejournal.com/users/palaman/12345.html
        pattern = re.compile(
            r'(?:https?:)?//(?:'
            + re.escape(host)
            + r'|(?:www\.)?livejournal\.com/users/' + re.escape(author)
            + r')/(\d+)\.html'
        )
        for a in doc.iter("a"):
            href = a.get("href", "")
            m = pattern.search(href)
            if m:
                a.set("href", f"{m.group(1)}.html")

    # Извлекаем содержимое <body><div>...</div></body>
    body = doc.find(".//body")
    if body is not None:
        div = body.find("div")
        if div is not None:
            inner = etree.tostring(div, encoding="unicode", method="html")
            inner = re.sub(r'^<div>', '', inner)
            inner = re.sub(r'</div>$', '', inner)
            return inner
    return html_str


# ═══════════════════════════════════════════════════════════════
#  ПАРСИНГ КОММЕНТАРИЕВ (из JSON Site.page)
# ═══════════════════════════════════════════════════════════════

async def _fetch_thread_comments(session: AsyncSession, thread_url: str) -> list[dict]:
    """Загружает thread URL и возвращает список комментариев из Site.page JSON."""
    try:
        resp = await _async_get(session, thread_url, timeout=20, retries=2)
        if resp.status_code != 200:
            return []
        body_text = resp.content.decode("utf-8", errors="ignore")
        data = extract_site_page(body_text)
        if not data:
            return []
        return data.get("comments", [])
    except Exception as e:
        print(f"    thread error: {e}")
        return []


async def scrape_all_comments(session: AsyncSession, post_url: str,
                               archive_dir: Path, author_base: str = "") -> list[dict]:
    """
    Полный сбор всех комментариев поста.

    Алгоритм:
      1. Загружаем страницу поста → извлекаем Site.page JSON
      2. Из JSON берём список комментариев
      3. Для каждого top-level комментария загружаем thread_url
         (асинхронно, пачками по PARALLEL_THREADS)
      4. Thread URL возвращает ВСЕ вложенные комментарии с полным содержимым
      5. Собираем, дедуплицируем, сортируем
    """
    try:
        body_text = await async_fetch_text(session, post_url)
    except RuntimeError:
        return []

    data = extract_site_page(body_text)
    if not data:
        return []

    reply_count = data.get("replycount", 0)

    if reply_count == 0:
        return []

    base_comments = data.get("comments", [])

    # Если на первой странице не все комментарии — пробуем page=2
    if len(base_comments) < reply_count:
        try:
            body2 = await async_fetch_text(session, post_url + "?page=2")
            data2 = extract_site_page(body2)
            if data2:
                comments2 = data2.get("comments", [])
                if len(comments2) > len(base_comments):
                    base_comments = comments2
        except Exception:
            pass

    # Определяем top-level треды
    top_threads = []
    for c in base_comments:
        if c.get("parent") == 0:
            turl = c.get("thread_url", "")
            if turl:
                top_threads.append(turl)

    all_comments = {}  # talkid -> comment_dict

    # Сначала добавляем уже загруженные
    for c in base_comments:
        tid = c.get("talkid")
        if tid and c.get("loaded") == 1 and c.get("article"):
            all_comments[tid] = c

    # Загружаем thread URLs пачками
    failed_urls = []
    for i in range(0, len(top_threads), PARALLEL_THREADS):
        batch = top_threads[i:i + PARALLEL_THREADS]
        tasks = [_fetch_thread_comments(session, turl) for turl in batch]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        errors_in_batch = 0
        for j, result in enumerate(results):
            if isinstance(result, Exception):
                failed_urls.append(batch[j])
                errors_in_batch += 1
                print(f"    thread error: {result}")
                continue
            for c in result:
                tid = c.get("talkid")
                if tid and c.get("article"):
                    if tid not in all_comments:
                        all_comments[tid] = c

        if errors_in_batch == len(batch) and len(batch) > 1:
            print(f"    ⚠ all {len(batch)} threads failed — pausing 10s...")
            await asyncio.sleep(10)
        elif i + PARALLEL_THREADS < len(top_threads):
            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # Retry failed threads поштучно
    for turl in failed_urls:
        await asyncio.sleep(1)
        try:
            result = await _fetch_thread_comments(session, turl)
            for c in result:
                tid = c.get("talkid")
                if tid and c.get("article"):
                    if tid not in all_comments:
                        all_comments[tid] = c
        except Exception:
            pass

    # Добавляем deleted/без содержимого
    for c in base_comments:
        tid = c.get("talkid")
        if tid and tid not in all_comments:
            all_comments[tid] = c

    # Сортируем по talkid (≈ хронология)
    sorted_comments = sorted(all_comments.values(), key=lambda c: c.get("talkid", 0))

    # Конвертируем в наш формат + обрабатываем картинки
    result = []
    for c in sorted_comments:
        article = c.get("article") or ""
        level = c.get("level", 0)
        if level > 0:
            level -= 1  # JSON level начинается с 1 для top-level

        uname = c.get("uname", "anonymous")
        etime = c.get("etime") or c.get("ctime") or ""
        dtalkid = c.get("dtalkid", c.get("thread", 0))
        deleted = c.get("deleted", 0)

        body_html = None
        if article and not deleted:
            body_html = await process_html_images_and_links(
                session, article, archive_dir, author_base
            )

        result.append({
            "id": f"ljcmt{dtalkid}",
            "level": level,
            "username": uname,
            "date": etime,
            "body_html": body_html,
        })

    return result


# ═══════════════════════════════════════════════════════════════
#  РЕНДЕР КОММЕНТАРИЕВ
# ═══════════════════════════════════════════════════════════════

def render_comments_html(comments: list[dict]) -> str:
    if not comments:
        return "<p class='no-comments'>Комментариев нет.</p>"
    lines = ['<div class="comments-list">']
    for cmt in comments:
        indent = cmt["level"] * 32
        body = cmt["body_html"] or "<em class='deleted'>[комментарий удалён]</em>"
        lines.append(f'''
  <div class="comment" style="margin-left:{indent}px" id="{cmt["id"]}">
    <div class="comment-header">
      <span class="comment-author">{cmt["username"]}</span>
      <span class="comment-date">{cmt["date"]}</span>
    </div>
    <div class="comment-text">{body}</div>
  </div>''')
    lines.append('</div>')
    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
#  HTML ШАБЛОН
# ═══════════════════════════════════════════════════════════════

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="ru" data-year="{year}" data-tags="{tags_data}" data-postid="{post_id}" data-date="{post_date}">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      background: #f5f5f0;
      font-family: Georgia, "Times New Roman", serif;
      font-size: 16px;
      line-height: 1.7;
      color: #222;
    }}
    .page-wrap {{
      max-width: 820px;
      margin: 40px auto;
      background: #fff;
      border-radius: 4px;
      box-shadow: 0 1px 6px rgba(0,0,0,.12);
      padding: 40px 48px 56px;
    }}
    .post-nav {{
      font-family: Arial, sans-serif;
      font-size: 0.82rem;
      color: #bbb;
      margin: 0 0 24px;
    }}
    .post-nav a {{ color: #999; text-decoration: none; }}
    .post-nav a:hover {{ color: #1a6ea8; text-decoration: underline; }}
    .post-title {{
      font-family: Arial, sans-serif;
      font-size: 1.75rem;
      font-weight: 700;
      line-height: 1.25;
      color: #111;
      margin: 0 0 4px;
    }}
    .post-date {{
      font-family: Arial, sans-serif;
      font-size: 0.82rem;
      color: #bbb;
      margin: 0 0 12px;
    }}
    .post-tags {{
      margin: 0 0 28px;
      font-size: 0.85rem;
      color: #888;
    }}
    .post-tags::before {{ content: "Теги: "; font-weight: 600; color: #bbb; font-family: Arial, sans-serif; }}
    .post-tags a {{
      display: inline-block;
      background: #f0f0f0;
      border-radius: 3px;
      padding: 2px 8px;
      margin: 0 4px 4px 0;
      color: #555;
      text-decoration: none;
      font-size: 0.82rem;
    }}
    .post-tags a:hover {{ background: #e0e8f0; color: #1a6ea8; }}
    hr.divider {{
      border: none;
      border-top: 1px solid #e8e8e8;
      margin: 0 0 28px;
    }}
    .post-content {{ word-break: break-word; }}
    .post-content p   {{ margin: 0 0 1em; }}
    .post-content img {{
      max-width: 100%; height: auto;
      display: block; margin: 1em auto; border-radius: 3px;
    }}
    .post-content a       {{ color: #1a6ea8; text-decoration: none; }}
    .post-content a:hover {{ text-decoration: underline; }}
    .post-content blockquote {{
      border-left: 3px solid #ccc;
      margin: 1em 0; padding: 0.4em 1em;
      color: #555; background: #fafafa;
    }}
    .lj-spoiler-body {{ display: none; padding: 8px 0; }}
    .lj-spoiler-head {{ cursor: pointer; color: #1a6ea8; }}
    .comments-section {{ margin-top: 48px; }}
    .comments-section h2 {{
      font-family: Arial, sans-serif;
      font-size: 1.1rem; font-weight: 700; color: #555;
      border-bottom: 1px solid #e8e8e8;
      padding-bottom: 8px; margin: 0 0 24px;
    }}
    .comment {{
      background: #fafafa;
      border-left: 3px solid #dde4ec;
      border-radius: 0 4px 4px 0;
      padding: 10px 14px;
      margin-bottom: 8px;
    }}
    .comment:hover {{ border-left-color: #7aa5c8; }}
    .comment-header {{
      display: flex; align-items: baseline; gap: 10px; margin-bottom: 5px;
    }}
    .comment-author {{
      font-family: Arial, sans-serif;
      font-weight: 700; font-size: 0.9rem; color: #1a6ea8;
    }}
    .comment-date {{ font-size: 0.78rem; color: #aaa; }}
    .comment-text {{ font-size: 0.95rem; line-height: 1.6; }}
    .comment-text img {{
      max-width: 100%; height: auto;
      display: block; margin: 6px 0; border-radius: 3px;
    }}
    .comment-text a {{ color: #1a6ea8; }}
    .no-comments {{ color: #aaa; font-style: italic; }}
    .post-source {{
      margin-top: 36px; padding-top: 16px;
      border-top: 1px solid #eee;
      font-size: 0.82rem; color: #bbb;
    }}
    .post-source a {{ color: #bbb; }}
  </style>
</head>
<body>
<div class="page-wrap">
  <div class="post-nav"><a href="../01.MAIN.html">&larr; Главная</a> &middot; <a href="year_{year}.html">{year}</a></div>
  <h1 class="post-title">{title}</h1>
  <div class="post-date">{post_date}</div>
  <div class="post-tags">{tags_html}</div>
  <hr class="divider">
  <div class="post-content">{content_html}</div>
  <div class="post-source">
    Источник: <a href="{source_url}">{source_url}</a>
  </div>
  <div class="comments-section">
    <h2>Комментарии ({comment_count})</h2>
    {comments_html}
  </div>
</div>
<script>
  document.querySelectorAll('.lj-spoiler-head').forEach(function(h) {{
    h.addEventListener('click', function() {{
      var b = h.parentElement.querySelector('.lj-spoiler-body');
      if (b) b.style.display = b.style.display === 'block' ? 'none' : 'block';
    }});
  }});
</script>
</body>
</html>
"""


def _tag_filename(tag: str) -> str:
    safe = re.sub(r"[^\w\u0400-\u04FF]", "_", tag)
    safe = re.sub(r"_+", "_", safe).strip("_")
    return f"tag_{safe}.html"


def _tag_href(tag: str) -> str:
    import urllib.parse as _up
    return _up.quote(_tag_filename(tag), safe="./")


def build_html(title, tags, content_html, source_url, comments_html, comment_count,
               year: str = "", post_id: str = "", post_date: str = ""):
    if tags:
        tags_html = "".join(
            f'<a href="{_tag_href(t)}">{t}</a>' for t in tags
        )
    else:
        tags_html = "<span style='color:#bbb'>нет тегов</span>"
    tags_data = "|".join(tags)
    return HTML_TEMPLATE.format(
        title=title, tags_html=tags_html, tags_data=tags_data,
        content_html=content_html, source_url=source_url,
        comments_html=comments_html, comment_count=comment_count,
        year=year, post_id=post_id, post_date=post_date,
    )


# ═══════════════════════════════════════════════════════════════
#  ПАРСИНГ ДАТЫ ПОСТА
# ═══════════════════════════════════════════════════════════════

MONTHS_RU = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
}
MONTHS_EN = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}


def parse_post_date(doc) -> tuple[str, str]:
    """Извлекает дату и год из lxml-документа поста."""
    # Ищем элемент с датой разными способами (p и div, т.к. LJ использует <p>)
    candidates = []
    candidates.extend(doc.findall(".//p[@class='aentry-head__date']"))
    candidates.extend(doc.findall(".//div[@class='aentry-head__date']"))
    candidates.extend(doc.xpath(".//*[contains(@class,'aentry-post__date')]"))
    candidates.extend(doc.findall(".//time"))
    candidates.extend(doc.findall(".//span[@class='entry-date']"))
    for el in candidates:
        if el is None:
            continue
        raw = _el_text(el)
        if not raw:
            continue
        # Формат: "January 20 2024, 13:23" (англ) или "20 января 2024, 13:23" (рус)
        # Английский формат: Month DD YYYY, HH:MM
        dm_en = re.search(r"(\w+)\s+(\d{1,2})\s+(\d{4})[,\s]+(\d{1,2}):(\d{2})", raw)
        if dm_en:
            mon, d, y, hh, mm = dm_en.groups()
            mon_num = MONTHS_EN.get(mon.lower(), 0) or MONTHS_RU.get(mon.lower(), 0)
            if mon_num:
                return f"{y}.{mon_num:02d}.{int(d):02d} {int(hh):02d}:{mm}", y
        # Русский формат: DD месяц YYYY, HH:MM
        dm_ru = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})[,\s]+(\d{1,2}):(\d{2})", raw)
        if dm_ru:
            d, mon, y, hh, mm = dm_ru.groups()
            mon_num = MONTHS_RU.get(mon.lower(), 0) or MONTHS_EN.get(mon.lower(), 0)
            if mon_num:
                return f"{y}.{mon_num:02d}.{int(d):02d} {int(hh):02d}:{mm}", y
        ym = re.search(r"20\d{2}|19\d{2}", raw)
        if ym:
            return "", ym.group(0)
    return "", ""


# ═══════════════════════════════════════════════════════════════
#  ОСНОВНАЯ ФУНКЦИЯ — СКАЧИВАНИЕ ПОСТА
# ═══════════════════════════════════════════════════════════════

async def scrape_post(session: AsyncSession, url: str, archive_dir: Path,
                       author_base: str = "") -> bool:
    post_id = extract_post_id(url)
    if not post_id:
        print(f"  Cannot extract ID from: {url}")
        return False

    out_file = archive_dir / f"{post_id}.html"

    # 1. Основной контент
    try:
        doc, body_text, status = await async_fetch_and_parse(session, url, timeout=20)
    except Exception as e:
        print(f"  [{post_id}] Error: {e}")
        return False

    if status != 200 or doc is None:
        print(f"  [{post_id}] HTTP {status}")
        return False

    # Заголовок
    title_span = doc.find(".//*[@class='aentry-post__title-text']")
    if title_span is not None:
        title = _el_text(title_span)
    else:
        title_el = doc.find(".//*[@class='aentry-post__title']")
        title = _el_text(title_el) if title_el is not None else f"Post {post_id}"

    # Теги — только из блока aentry-tags (не из текста поста)
    tags = []
    tags_container = doc.xpath(".//*[contains(@class,'aentry-tags')]")
    for container in tags_container:
        for a in container.iter("a"):
            href = a.get("href", "")
            if "/tag/" in href:
                t = (a.text or "").strip().lstrip("#")
                if t and t not in tags:
                    tags.append(t)

    # Контент поста
    content_el = doc.find(".//*[@class='aentry-post__content']")
    if content_el is None:
        print(f"  [{post_id}] .aentry-post__content not found")
        return False

    content_html = _el_inner_html(content_el)
    content_html = await process_html_images_and_links(session, content_html, archive_dir, author_base)

    # Дата
    post_date, year_str = parse_post_date(doc)
    if not year_str:
        ym = re.search(r"20\d{2}|19\d{2}", body_text[:3000])
        year_str = ym.group(0) if ym else ""

    # 2. Комментарии (из JSON в body_text)
    comments = await scrape_all_comments(session, url, archive_dir, author_base)

    # 3. Сборка HTML
    comments_html = render_comments_html(comments)
    html = build_html(title, tags, content_html, url,
                      comments_html, len(comments),
                      year=year_str, post_id=post_id, post_date=post_date)
    out_file.write_text(html, encoding="utf-8")

    tags_str = ", ".join(tags) if tags else "-"
    print(f"  {post_id}  {title[:60]}  |  {post_date or year_str}  |  tags: {tags_str}  |  comments: {len(comments)}  |  OK")

    return True


# ═══════════════════════════════════════════════════════════════
#  СБОР СПИСКА ПОСТОВ ПО ГОДАМ
# ═══════════════════════════════════════════════════════════════

def parse_years_input(text: str) -> list[int]:
    years = set()
    for part in text.replace(" ", "").split(","):
        if "-" in part:
            bounds = part.split("-")
            if len(bounds) == 2 and bounds[0].isdigit() and bounds[1].isdigit():
                y1, y2 = int(bounds[0]), int(bounds[1])
                years.update(range(min(y1, y2), max(y1, y2) + 1))
        elif part.isdigit():
            years.add(int(part))
    return sorted(y for y in years if YEAR_MIN <= y <= YEAR_MAX)


async def collect_month_urls(session: AsyncSession, year_url: str, author_base: str) -> list[str]:
    """Загружает страницу года → список URL месяцев с "View Subjects"."""
    try:
        doc, _, status = await async_fetch_and_parse(session, year_url, timeout=20)
    except Exception:
        return []
    if status != 200 or doc is None:
        return []

    month_urls = []

    # Способ 1: ищем "View Subjects" ссылки внутри #alpha-inner (классический шаблон)
    alpha = doc.find(".//*[@id='alpha-inner']")
    search_root = alpha if alpha is not None else doc
    for a in search_root.iter("a"):
        text = _el_text(a)
        if "View Subjects" in text:
            href = a.get("href", "")
            if href.startswith(author_base):
                month_urls.append(href)

    # Способ 2 (fallback): ищем ссылки на месяцы вида author_base/YYYY/MM/
    if not month_urls:
        # Извлекаем год из URL
        year_match = re.search(r'/(\d{4})/?$', year_url.rstrip('/'))
        if year_match:
            year_str = year_match.group(1)
            month_pat = re.compile(
                r'^' + re.escape(author_base.rstrip('/')) + r'/' + re.escape(year_str) + r'/\d{2}$'
            )
            seen = set()
            for a in doc.iter("a"):
                href = (a.get("href") or "").rstrip("/")
                if month_pat.match(href) and href not in seen:
                    seen.add(href)
                    # Сохраняем с trailing slash (формат "View Subjects" ссылок)
                    month_urls.append(href + "/")
            month_urls.sort()

    return month_urls


def collect_post_urls_from_month_doc(doc, author_base: str) -> list[str]:
    """Извлекает ссылки на посты из lxml-документа страницы месяца."""
    post_urls = []

    # Способ 1: ищем внутри элементов с классом 'viewsubjects'
    for el in doc.xpath(".//*[contains(@class, 'viewsubjects')]"):
        for a in el.iter("a"):
            href = a.get("href", "")
            if href.startswith(author_base) and re.search(r'/\d+\.html', href):
                post_urls.append(href)

    # Способ 2 (fallback): ищем все ссылки на посты по всему документу
    if not post_urls:
        seen = set()
        post_pat = re.compile(r'^' + re.escape(author_base.rstrip('/')) + r'/\d+\.html$')
        for a in doc.iter("a"):
            href = a.get("href", "")
            if post_pat.match(href) and href not in seen:
                seen.add(href)
                post_urls.append(href)

    return post_urls


async def collect_all_post_urls(session: AsyncSession, author_base: str,
                                 years: list[int]) -> list[str]:
    """Проход по годам → месяцам → постам."""
    print(f"\n{'='*60}")
    print(f"  Collecting posts: {author_base}")
    print(f"  Years: {years}")
    print(f"{'='*60}")

    all_post_urls = []
    seen = set()

    for year in years:
        year_url = f"{author_base.rstrip('/')}/{year}"
        print(f"\n  [{year}] {year_url}")
        await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        month_urls = await collect_month_urls(session, year_url, author_base)
        if not month_urls:
            print(f"    No posts")
            continue
        print(f"    Months with posts: {len(month_urls)}")

        # Загружаем месяцы пачками
        month_results = {}
        for i in range(0, len(month_urls), 6):
            batch = month_urls[i:i + 6]
            tasks = [async_fetch_and_parse(session, murl, timeout=20) for murl in batch]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

            for murl, resp in zip(batch, responses):
                if isinstance(resp, Exception):
                    continue
                mdoc, _, mstatus = resp
                if mstatus != 200 or mdoc is None:
                    continue
                urls = collect_post_urls_from_month_doc(mdoc, author_base)
                month_results[murl] = urls

            if i + 6 < len(month_urls):
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        for murl in month_urls:
            urls = month_results.get(murl, [])
            label = "/".join(murl.rstrip("/").split("/")[-2:])
            print(f"    {label}: {len(urls)} posts")
            for u in urls:
                if u not in seen:
                    seen.add(u)
                    all_post_urls.append(u)

    # Сортируем по числовому ID
    all_post_urls.sort(
        key=lambda u: int(m.group(1)) if (m := re.search(r'/(\d+)\.html', u)) else 0
    )

    print(f"\n  Total posts found: {len(all_post_urls)}")
    return all_post_urls


# ═══════════════════════════════════════════════════════════════
#  ИНТЕРАКТИВНЫЙ ВВОД ПАРАМЕТРОВ
# ═══════════════════════════════════════════════════════════════

def extract_nickname(url: str) -> str:
    """Извлекает никнейм автора из URL ЖЖ."""
    parsed = urlparse(url)
    host = parsed.netloc or ""
    # palaman.livejournal.com → palaman
    if host.endswith(".livejournal.com"):
        nick = host.split(".")[0]
        if nick and nick != "www":
            return nick
    # www.livejournal.com/users/palaman/ → palaman
    m = re.search(r'/users/([\w-]+)', parsed.path)
    if m:
        return m.group(1)
    return host.split(".")[0]


def _validate_lj_url(raw: str) -> str | None:
    """Валидирует и нормализует один URL ЖЖ. Возвращает URL или None."""
    raw = raw.strip()
    if not raw:
        return None
    if not raw.startswith("http"):
        raw = "https://" + raw
    # Одиночный пост
    if re.search(r'/\d+\.html$', raw):
        return raw
    if not raw.endswith("/"):
        raw += "/"
    if re.match(r'https?://[\w-]+\.livejournal\.com/', raw):
        return raw
    return None


def ask_authors() -> list[dict]:
    """Запрашивает одного или нескольких авторов.
    Возвращает список: [{"base_url": str, "nickname": str, "single_post_url": str|None}]"""
    while True:
        raw = input(
            "\nURL автора (или нескольких через запятую)\n"
            "  Архив:  https://palaman.livejournal.com/\n"
            "  Несколько: https://palaman.livejournal.com/, https://krylov.livejournal.com/\n"
            "  Пост:   https://palaman.livejournal.com/12345.html\n"
            "> "
        ).strip()
        if not raw:
            continue

        parts = [p.strip() for p in raw.split(",") if p.strip()]
        authors = []
        all_valid = True
        for part in parts:
            url = _validate_lj_url(part)
            if not url:
                print(f"  Bad URL: {part}")
                all_valid = False
                break

            single_post_url = None
            if re.search(r'/\d+\.html$', url):
                single_post_url = url
                base_url = re.match(r'(https?://[^/]+/)', url).group(1)
            else:
                base_url = url

            nick = extract_nickname(base_url)
            authors.append({
                "base_url": base_url,
                "nickname": nick,
                "single_post_url": single_post_url,
            })

        if all_valid and authors:
            nicks = [a["nickname"] for a in authors]
            print(f"  Authors: {', '.join(nicks)}")
            return authors


def ask_archive_dir() -> Path:
    while True:
        raw = input("Save folder (e.g. D:\\Archive): ").strip()
        if not raw:
            continue
        p = Path(raw)
        try:
            p.mkdir(parents=True, exist_ok=True)
            return p
        except Exception as e:
            print(f"  Cannot create folder: {e}")


def ask_years() -> list[int]:
    current_year = datetime.now().year
    print("Years to download:")
    print(f"  Examples: 2019  |  2017,2018,2019  |  2015-2019  |  2012-2014,2018")
    print(f"  Leave empty to check all years ({YEAR_MIN}-{current_year})")
    raw = input("Years: ").strip()
    if not raw:
        years = list(range(YEAR_MIN, current_year + 1))
        print(f"  Will check all years: {YEAR_MIN}-{current_year}")
        return years
    years = parse_years_input(raw)
    if years:
        print(f"  Selected: {years}")
        return years
    # Fallback: все годы
    print(f"  Cannot parse — will check all years: {YEAR_MIN}-{current_year}")
    return list(range(YEAR_MIN, current_year + 1))


def check_existing_archives(archive_dir: Path, authors: list[dict]) -> dict[str, bool]:
    """Проверяет существующие папки авторов, показывает статистику.
    Возвращает dict {nickname: skip_existing (True=пропускать, False=перекачать)}."""
    skip_map = {}
    for author in authors:
        nick = author["nickname"]
        content_dir = archive_dir / nick / "Content"
        if not content_dir.exists():
            skip_map[nick] = True
            continue

        # Считаем посты и годы
        years_found = set()
        post_count = 0
        parser = etree.HTMLParser(encoding="utf-8")
        for fpath in content_dir.glob("*.html"):
            if not fpath.stem.isdigit():
                continue
            post_count += 1
            try:
                raw = fpath.read_bytes()[:2000]
                doc = etree.fromstring(raw, parser)
                html_root = doc.getroottree().getroot()
                yr = (html_root.get("data-year", "") or "")[:4]
                if yr.isdigit():
                    years_found.add(yr)
            except Exception:
                pass

        if post_count == 0:
            skip_map[nick] = True
            continue

        years_str = ", ".join(sorted(years_found)) if years_found else "?"
        print(f"\n  Author '{nick}' already has {post_count} posts (years: {years_str})")
        choice = input("  Re-download existing posts or skip? [s=skip / r=re-download, default=skip]: ").strip().lower()
        skip_map[nick] = (choice != "r")
        if skip_map[nick]:
            print(f"    → Will skip existing posts for {nick}")
        else:
            print(f"    → Will re-download all posts for {nick}")

    return skip_map


# ═══════════════════════════════════════════════════════════════
#  ГЕНЕРАЦИЯ НАВИГАЦИОННОГО ИНДЕКСА
# ═══════════════════════════════════════════════════════════════

NAV_CSS = """
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: #f5f5f0;
  font-family: Georgia, "Times New Roman", serif;
  font-size: 16px;
  line-height: 1.7;
  color: #222;
  min-height: 100vh;
}
.wrap {
  max-width: 820px;
  margin: 0 auto;
  background: #fff;
  border-radius: 4px;
  box-shadow: 0 1px 6px rgba(0,0,0,.1);
  margin: 40px auto;
  padding: 40px 48px 60px;
}
.site-header {
  border-bottom: 1px solid #e8e8e8;
  padding-bottom: 24px;
  margin-bottom: 36px;
}
.site-title {
  font-family: Arial, sans-serif;
  font-size: 2rem;
  font-weight: 700;
  color: #111;
  line-height: 1.1;
}
.site-subtitle {
  margin-top: 6px;
  font-family: Arial, sans-serif;
  font-size: 0.82rem;
  color: #aaa;
  letter-spacing: 0.06em;
  text-transform: uppercase;
}
.back-link {
  display: inline-block;
  margin-bottom: 28px;
  font-family: Arial, sans-serif;
  font-size: 0.82rem;
  color: #999;
  text-decoration: none;
}
.back-link:hover { color: #1a6ea8; text-decoration: underline; }
.section-label {
  font-family: Arial, sans-serif;
  font-size: 0.7rem;
  letter-spacing: 0.16em;
  text-transform: uppercase;
  color: #bbb;
  margin-bottom: 16px;
}
.years-grid { display: flex; flex-wrap: wrap; gap: 12px; margin-bottom: 44px; }
.year-card {
  display: flex; flex-direction: column; align-items: center;
  justify-content: center; width: 100px; height: 80px;
  background: #fafafa; border: 1px solid #e0e0e0;
  border-radius: 4px; text-decoration: none;
  transition: border-color .15s, background .15s;
}
.year-card:hover { background: #f0f4f8; border-color: #1a6ea8; }
.year-card .yr { font-family: Arial, sans-serif; font-size: 1.4rem; color: #222; font-weight: 700; }
.year-card .cnt { font-family: Arial, sans-serif; font-size: 0.7rem; color: #aaa; margin-top: 3px; }
.tags-cloud { display: flex; flex-wrap: wrap; gap: 8px; }
.tag-chip {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 4px 12px; background: #f0f0f0; border: 1px solid #e0e0e0;
  border-radius: 3px; text-decoration: none; color: #555;
  font-size: 0.85rem; transition: background .15s, color .15s;
}
.tag-chip:hover { background: #e0e8f0; color: #1a6ea8; border-color: #b0c8e0; }
.tag-chip .tag-count {
  font-family: Arial, sans-serif; font-size: 0.7rem; color: #999;
  background: #e4e4e4; padding: 1px 5px; border-radius: 20px;
}
.post-list { list-style: none; }
.post-item {
  border-bottom: 1px solid #f0f0f0; padding: 14px 0;
  display: flex; align-items: baseline; gap: 14px;
}
.post-item:first-child { border-top: 1px solid #f0f0f0; }
.post-num {
  font-family: Arial, sans-serif; font-size: 0.72rem; color: #ccc;
  min-width: 28px; text-align: right; flex-shrink: 0;
}
.post-link {
  color: #222; text-decoration: none; font-size: 1rem; line-height: 1.4; flex: 1;
}
.post-link:hover { color: #1a6ea8; }
.post-meta {
  font-family: Arial, sans-serif; font-size: 0.72rem; color: #bbb; flex-shrink: 0;
}
.post-tags-inline { margin-top: 4px; display: flex; flex-wrap: wrap; gap: 5px; }
.post-tag-mini {
  font-family: Arial, sans-serif; font-size: 0.7rem; color: #999;
  background: #f0f0f0; padding: 1px 7px; border-radius: 3px; text-decoration: none;
}
.post-tag-mini:hover { color: #1a6ea8; background: #e0e8f0; }
.empty-note { color: #bbb; font-style: italic; padding: 20px 0; }
.search-box { display: flex; gap: 8px; margin-bottom: 14px; align-items: center; margin-top: 4px; }
.search-input {
  flex: 1; padding: 9px 14px; font-family: Arial, sans-serif; font-size: 0.95rem;
  border: 1px solid #d8d8d8; border-radius: 4px; outline: none; color: #222;
  background: #fafafa; transition: border-color .15s;
}
.search-input:focus { border-color: #1a6ea8; background: #fff; }
.search-btn {
  padding: 9px 20px; font-family: Arial, sans-serif; font-size: 0.9rem;
  background: #1a6ea8; color: #fff; border: none; border-radius: 4px;
  cursor: pointer; white-space: nowrap; transition: background .15s;
}
.search-btn:hover:not(:disabled) { background: #155a8a; }
.search-btn:disabled { background: #b0c4d8; cursor: default; }
.clear-btn {
  padding: 9px 14px; font-family: Arial, sans-serif; font-size: 0.85rem;
  background: #f0f0f0; color: #666; border: 1px solid #d8d8d8;
  border-radius: 4px; cursor: pointer; white-space: nowrap; transition: background .15s;
}
.clear-btn:hover { background: #e0e0e0; color: #333; }
.search-status { min-height: 24px; margin-bottom: 8px; font-family: Arial, sans-serif; font-size: 0.85rem; }
.search-progress { color: #888; }
.search-count { color: #1a6ea8; font-weight: 600; }
.search-none { color: #bbb; font-style: italic; }
"""

SEARCH_BLOCK_TEMPLATE = '\n<div class="section-label" style="margin-top:40px">Поиск по архиву</div>\n<div class="search-box">\n  <input type="text" id="searchInput" class="search-input"\n         placeholder="Введите текст для поиска (минимум 5 символов)…"\n         oninput="onSearchInput()"\n         onkeydown="if(event.key===\'Enter\' && !document.getElementById(\'searchBtn\').disabled)doSearch()">\n  <button id="searchBtn" class="search-btn" onclick="doSearch()" disabled>Найти</button>\n  <button class="clear-btn" onclick="clearSearch()">&#x2715; Очистить</button>\n</div>\n<div id="searchStatus" class="search-status"></div>\n<div id="searchResults"></div>\n<script src="Content/search_index.js"></script>\n<script>\n(function() {\n  function onSearchInput() {\n    var q = document.getElementById(\'searchInput\').value;\n    document.getElementById(\'searchBtn\').disabled = (q.length < 5);\n  }\n  function clearSearch() {\n    document.getElementById(\'searchInput\').value = \'\';\n    document.getElementById(\'searchBtn\').disabled = true;\n    document.getElementById(\'searchStatus\').innerHTML = \'\';\n    document.getElementById(\'searchResults\').innerHTML = \'\';\n  }\n  function doSearch() {\n    var query = document.getElementById(\'searchInput\').value.trim();\n    if (query.length < 5) return;\n    var statusEl  = document.getElementById(\'searchStatus\');\n    var resultsEl = document.getElementById(\'searchResults\');\n    if (typeof LJ_SEARCH_INDEX === \'undefined\') {\n      statusEl.innerHTML = \'<span class="search-none">Индекс поиска не загружен.</span>\';\n      return;\n    }\n    var queryLow = query.toLowerCase();\n    var found = [];\n    for (var i = 0; i < LJ_SEARCH_INDEX.length; i++) {\n      if (LJ_SEARCH_INDEX[i].text.indexOf(queryLow) !== -1) found.push(LJ_SEARCH_INDEX[i]);\n    }\n    if (!found.length) {\n      statusEl.innerHTML = \'<span class="search-none">Ничего не найдено по запросу «\' + escHtml(query) + \'»</span>\';\n      resultsEl.innerHTML = \'\';\n      return;\n    }\n    var cnt = found.length;\n    var m10 = cnt % 10, m100 = cnt % 100;\n    var word = (m100 >= 11 && m100 <= 19) ? \'постов\' : m10 === 1 ? \'пост\' : (m10 >= 2 && m10 <= 4) ? \'поста\' : \'постов\';\n    statusEl.innerHTML = \'<span class="search-count">Найдено: \' + cnt + \' \' + word + \'</span>\';\n    var items = \'\';\n    for (var j = 0; j < found.length; j++) {\n      var p = found[j];\n      var meta = p.date || p.year || \'\';\n      items += \'<li class="post-item"><span class="post-num">\' + (j+1) + \'</span><div style="flex:1"><a class="post-link" href="Content/\' + escHtml(p.file) + \'">\' + escHtml(p.title) + \'</a>\' + (meta ? \'<div class="post-tags-inline"><span class="post-meta">\' + escHtml(meta) + \'</span></div>\' : \'\') + \'</div></li>\';\n    }\n    resultsEl.innerHTML = \'<ul class="post-list">\' + items + \'</ul>\';\n  }\n  function escHtml(s) { return String(s).replace(/&/g,\'&amp;\').replace(/</g,\'&lt;\').replace(/>/g,\'&gt;\').replace(/"/g,\'&quot;\'); }\n  window.onSearchInput = onSearchInput;\n  window.doSearch = doSearch;\n  window.clearSearch = clearSearch;\n})();\n</script>\n'


def _nav_page(title: str, body_html: str, back_href: str = "", back_label: str = "") -> str:
    back = ""
    if back_href:
        back = f'<a class="back-link" href="{back_href}">&larr; {back_label}</a>'
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>{NAV_CSS}</style>
</head>
<body>
<div class="wrap">
  {back}
  {body_html}
</div>
</body>
</html>"""


def generate_index(archive_dir: Path, author_base: str = "", content_dir: Path = None,
                    parent_link: str = ""):
    from collections import defaultdict

    if content_dir is None:
        content_dir = archive_dir / "Content"
    content_dir.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 60)
    print("  Generating navigation index...")
    print("=" * 60)

    posts = []
    html_files = sorted(content_dir.glob("*.html"),
                        key=lambda p: int(p.stem) if p.stem.isdigit() else 0)

    parser = etree.HTMLParser(encoding="utf-8")

    for fpath in html_files:
        if not fpath.stem.isdigit():
            continue
        try:
            raw = fpath.read_bytes()
            doc = etree.fromstring(raw, parser)

            title_el = doc.find(".//h1")
            title = _el_text(title_el) if title_el is not None else f"Post {fpath.stem}"

            year = ""
            for a in doc.iter("a"):
                href = a.get("href", "")
                m = re.match(r"year_(\d{4})\.html", href)
                if m:
                    year = m.group(1)
                    break

            html_root = doc.getroottree().getroot()
            post_date = html_root.get("data-date", "") if html_root is not None else ""

            # Теги берём из data-tags атрибута, убираем # в начале
            tags_raw = html_root.get("data-tags", "") if html_root is not None else ""
            tags = [t.strip().lstrip("#") for t in tags_raw.split("|") if t.strip()]

            posts.append({
                "post_id": fpath.stem,
                "title": title or f"Post {fpath.stem}",
                "year": year,
                "tags": tags,
                "file": fpath.name,
                "post_date": post_date,
            })
        except Exception as e:
            print(f"  Skip {fpath.name}: {e}")

    print(f"  Post files found: {len(posts)}")

    by_year = defaultdict(list)
    by_tag = defaultdict(list)
    for p in posts:
        yr = p["year"] or "?"
        by_year[yr].append(p)
        for t in p["tags"]:
            by_tag[t].append(p)

    # Страницы по годам
    for year, year_posts in sorted(by_year.items()):
        items_html = "\n".join(
            f'''<li class="post-item">
              <span class="post-num">{i}</span>
              <div style="flex:1">
                <a class="post-link" href="{p["file"]}">{p["title"] or "Без заголовка"}</a>
                <div class="post-tags-inline">
                  {f'<span class="post-meta">{p["post_date"]}</span>' if p.get("post_date") else ""}
                  {"".join(f'<a class="post-tag-mini" href="{_tag_href(t)}">{t}</a>' for t in p["tags"])}
                </div>
              </div>
            </li>'''
            for i, p in enumerate(year_posts, 1)
        )
        cnt = len(year_posts)
        body = f'''<div class="site-header">
  <div class="site-title">{year}</div>
  <div class="site-subtitle">{cnt} {"пост" if cnt == 1 else "постов"}</div>
</div>
<ul class="post-list">{items_html}</ul>'''
        page = _nav_page(f"{year} — Архив", body, "../01.MAIN.html", "На главную")
        (content_dir / f"year_{year}.html").write_text(page, encoding="utf-8")

    print(f"  Year pages: {len(by_year)}")

    # Страницы по тегам
    for tag, tag_posts in sorted(by_tag.items()):
        items_html = "\n".join(
            f'''<li class="post-item">
              <span class="post-num">{i}</span>
              <div style="flex:1">
                <a class="post-link" href="{p["file"]}">{p["title"] or "Без заголовка"}</a>
                <div class="post-tags-inline">
                  <span class="post-meta">{p.get("post_date") or p["year"]}</span>
                  {"".join(f'<a class="post-tag-mini" href="{_tag_href(t)}">{t}</a>' for t in p["tags"])}
                </div>
              </div>
            </li>'''
            for i, p in enumerate(tag_posts, 1)
        )
        cnt = len(tag_posts)
        body = f'''<div class="site-header">
  <div class="site-title">#{tag}</div>
  <div class="site-subtitle">{cnt} {"пост" if cnt == 1 else "постов"}</div>
</div>
<ul class="post-list">{items_html}</ul>'''
        page = _nav_page(f"#{tag} — Архив", body, "../01.MAIN.html", "На главную")
        (content_dir / _tag_filename(tag)).write_text(page, encoding="utf-8")

    print(f"  Tag pages: {len(by_tag)}")

    # 01.MAIN.html
    years_html = "\n".join(
        f'''<a class="year-card" href="Content/year_{yr}.html">
  <span class="yr">{yr}</span>
  <span class="cnt">{len(ps)} постов</span>
</a>'''
        for yr, ps in sorted(by_year.items())
    )
    tags_html_main = "\n".join(
        f'''<a class="tag-chip" href="Content/{_tag_href(t)}">
  {t}<span class="tag-count">{len(ps)}</span>
</a>'''
        for t, ps in sorted(by_tag.items(), key=lambda x: -len(x[1]))
    )

    author_name = author_base.rstrip("/").split("//")[-1].split(".")[0] if author_base else "Архив"
    total = len(posts)

    # Поисковый индекс
    print("  Building search index...")
    index_entries = []
    for p in posts:
        try:
            fpath = content_dir / p["file"]
            raw = fpath.read_bytes()
            doc = etree.fromstring(raw, parser)
            for el in doc.iter("script", "style"):
                el.getparent().remove(el)
            plain = etree.tostring(doc, method="text", encoding="unicode")
            plain = re.sub(r"\s+", " ", plain).lower()
        except Exception:
            plain = p["title"].lower()
        index_entries.append({
            "file": p["file"], "title": p["title"],
            "date": p.get("post_date", ""), "year": p["year"],
            "text": plain,
        })

    index_js = "var LJ_SEARCH_INDEX = " + json.dumps(
        index_entries, ensure_ascii=False, separators=(",", ":")
    ) + ";"
    (content_dir / "search_index.js").write_text(index_js, encoding="utf-8")
    size_kb = (content_dir / "search_index.js").stat().st_size // 1024
    print(f"  search_index.js ready ({size_kb} KB, {len(index_entries)} posts)")

    search_html = SEARCH_BLOCK_TEMPLATE

    body_parts = [
        '<div class="site-header">',
        f'  <div class="site-title">{author_name}</div>',
        f'  <div class="site-subtitle">Личный архив · {total} постов</div>',
        '</div>',
        '<div class="section-label" style="margin-top:36px">Годы</div>',
        f'<div class="years-grid" style="margin-bottom:40px">{years_html}</div>',
        '<div class="section-label">Теги</div>',
        f'<div class="tags-cloud">{tags_html_main or "<span class=empty-note>Теги не найдены</span>"}</div>',
        search_html,
    ]
    body = "\n".join(body_parts)

    back_href = parent_link if parent_link else ""
    back_label = "Все авторы" if parent_link else ""
    main_page = _nav_page(f"{author_name} — Архив", body,
                           back_href=back_href, back_label=back_label)
    (archive_dir / "01.MAIN.html").write_text(main_page, encoding="utf-8")

    print(f"\n  {author_name}/01.MAIN.html created")
    print(f"\n  Content: {content_dir}")
    print("=" * 60)


# ═══════════════════════════════════════════════════════════════
#  ЗАГРУЗКА ПОСТОВ ОДНОГО АВТОРА
# ═══════════════════════════════════════════════════════════════

async def download_author_posts(session: AsyncSession, post_urls: list[str],
                                 content_dir: Path, author_base: str,
                                 skip_existing: bool = True) -> dict:
    """Скачивает посты одного автора. Возвращает статистику."""
    success_count = 0
    skip_count = 0
    error_count = 0
    consecutive_errors = 0
    failed_items = []

    # Фильтруем уже скачанные
    to_download = []
    for i, purl in enumerate(post_urls, 1):
        post_id = extract_post_id(purl)
        out_file = content_dir / f"{post_id}.html"
        if skip_existing and out_file.exists():
            print(f"  [{i}/{len(post_urls)}] {post_id} -- already exists, skip")
            skip_count += 1
        else:
            to_download.append((i, purl))

    if not to_download:
        print("\n  All posts already downloaded.")
    else:
        print(f"\n  Downloading {len(to_download)} posts (semaphore={PARALLEL_POSTS})...\n")

    sem = asyncio.Semaphore(PARALLEL_POSTS)

    async def _download_one(idx: int, purl: str):
        nonlocal success_count, error_count, consecutive_errors
        pid = extract_post_id(purl)
        async with sem:
            print(f"\n  [{idx}/{len(post_urls)}] Post {pid}")
            try:
                ok = await scrape_post(session, purl, content_dir, author_base)
                if ok:
                    success_count += 1
                    consecutive_errors = 0
                else:
                    error_count += 1
                    consecutive_errors += 1
                    failed_items.append((idx, purl))
            except Exception as e:
                print(f"  [{pid}] Error: {e}")
                error_count += 1
                consecutive_errors += 1
                failed_items.append((idx, purl))

            if consecutive_errors >= 5:
                print(f"\n  ⚠ {consecutive_errors} consecutive errors — pausing 10s...")
                await asyncio.sleep(10)
                consecutive_errors = 0
            else:
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    tasks = [_download_one(idx, purl) for idx, purl in to_download]
    await asyncio.gather(*tasks)

    # Retry
    for retry_round in range(1, 3):
        if not failed_items:
            break
        retry_list = list(failed_items)
        failed_items.clear()
        print(f"\n  RETRY round {retry_round}: {len(retry_list)} failed posts (waiting 10s...)")
        await asyncio.sleep(10)
        error_count -= len(retry_list)
        consecutive_errors = 0
        retry_sem = asyncio.Semaphore(max(1, PARALLEL_POSTS // (retry_round + 1)))

        async def _retry_one(idx: int, purl: str):
            nonlocal success_count, error_count, consecutive_errors
            pid = extract_post_id(purl)
            async with retry_sem:
                print(f"\n  [retry] Post {pid}")
                try:
                    ok = await scrape_post(session, purl, content_dir, author_base)
                    if ok:
                        success_count += 1
                        consecutive_errors = 0
                    else:
                        error_count += 1
                        consecutive_errors += 1
                        failed_items.append((idx, purl))
                except Exception as e:
                    print(f"  [{pid}] Error: {e}")
                    error_count += 1
                    failed_items.append((idx, purl))
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

        retry_tasks = [_retry_one(idx, purl) for idx, purl in retry_list]
        await asyncio.gather(*retry_tasks)

    return {
        "success": success_count, "skipped": skip_count,
        "errors": error_count, "failed": failed_items,
    }


# ═══════════════════════════════════════════════════════════════
#  КРОСС-АВТОРСКИЕ ССЫЛКИ + СБОР ВНЕШНИХ ССЫЛОК
# ═══════════════════════════════════════════════════════════════

# Регекс для LJ-ссылок на посты
_LJ_POST_URL_RE = re.compile(
    r'href="((?:https?:)?//(?:(\w[\w-]*)\.livejournal\.com'
    r'|(?:www\.)?livejournal\.com/users/(\w[\w-]*))'
    r'/(\d+)\.html[^"]*)"'
)


def collect_and_rewrite_cross_links(archive_dir: Path, authors: list[dict]) -> list[tuple]:
    """
    Проход 2: перезаписывает кросс-авторские ссылки в HTML-файлах.
    Возвращает список внешних ссылок: [(referencing_nick, ext_nick, post_url, post_id)]
    """
    nick_set = {a["nickname"] for a in authors}
    external_links = []  # (referencing_nick, ext_nick, full_url, post_id)
    seen_external = set()

    for author in authors:
        nick = author["nickname"]
        content_dir = archive_dir / nick / "Content"
        if not content_dir.exists():
            continue

        for fpath in content_dir.glob("*.html"):
            if not fpath.stem.isdigit():
                continue
            try:
                html = fpath.read_text(encoding="utf-8")
            except Exception:
                continue

            modified = False

            def _replace_link(m):
                nonlocal modified
                full_href = m.group(1)
                target_nick = m.group(2) or m.group(3)
                post_id = m.group(4)

                if not target_nick:
                    return m.group(0)

                target_nick = target_nick.lower()

                # Свой же автор — ссылка на локальный файл
                if target_nick == nick:
                    modified = True
                    return f'href="{post_id}.html"'

                # Другой скачиваемый автор — кросс-ссылка
                if target_nick in nick_set:
                    modified = True
                    return f'href="../../{target_nick}/Content/{post_id}.html"'

                # Внешний автор — запоминаем для скачивания
                # Восстанавливаем полный URL
                url = full_href
                if url.startswith("//"):
                    url = "https:" + url
                key = (nick, target_nick, post_id)
                if key not in seen_external:
                    seen_external.add(key)
                    external_links.append((nick, target_nick, url, post_id))
                return m.group(0)  # пока не меняем — поменяем после скачивания

            new_html = _LJ_POST_URL_RE.sub(_replace_link, html)
            if modified:
                fpath.write_text(new_html, encoding="utf-8")

    return external_links


# ═══════════════════════════════════════════════════════════════
#  СКАЧИВАНИЕ ВНЕШНИХ ПОСТОВ (один уровень глубины)
# ═══════════════════════════════════════════════════════════════

async def scrape_external_post(session: AsyncSession, url: str,
                                dest_dir: Path, filename: str,
                                ext_author_base: str) -> bool:
    """Скачивает один внешний пост без follow-up ссылок."""
    out_file = dest_dir / filename
    if out_file.exists():
        return True

    try:
        doc, body_text, status = await async_fetch_and_parse(session, url, timeout=20)
    except Exception as e:
        print(f"    [ext] Error fetching {url}: {e}")
        return False

    if status != 200 or doc is None:
        print(f"    [ext] HTTP {status} for {url}")
        return False

    title_span = doc.find(".//*[@class='aentry-post__title-text']")
    if title_span is not None:
        title = _el_text(title_span)
    else:
        title_el = doc.find(".//*[@class='aentry-post__title']")
        title = _el_text(title_el) if title_el is not None else filename

    tags = []
    tags_container = doc.xpath(".//*[contains(@class,'aentry-tags')]")
    for container in tags_container:
        for a in container.iter("a"):
            href = a.get("href", "")
            if "/tag/" in href:
                t = (a.text or "").strip().lstrip("#")
                if t and t not in tags:
                    tags.append(t)

    content_el = doc.find(".//*[@class='aentry-post__content']")
    if content_el is None:
        return False

    content_html = _el_inner_html(content_el)
    # Скачиваем картинки, но НЕ переписываем ссылки (rewrite_links=False не нужен,
    # просто передаём пустой author_base чтобы не матчить ссылки)
    content_html = await process_html_images_and_links(session, content_html, dest_dir, author_base="")

    post_date, year_str = parse_post_date(doc)
    if not year_str:
        ym = re.search(r"20\d{2}|19\d{2}", body_text[:3000])
        year_str = ym.group(0) if ym else ""

    # Комментарии
    comments = await scrape_all_comments(session, url, dest_dir, ext_author_base)
    comments_html = render_comments_html(comments)

    post_id = extract_post_id(url) or filename.replace(".html", "")
    html = build_html(title, tags, content_html, url,
                      comments_html, len(comments),
                      year=year_str, post_id=post_id, post_date=post_date)
    out_file.write_text(html, encoding="utf-8")

    ext_nick = extract_nickname(url)
    print(f"    [ext] {ext_nick}/{post_id}  {title[:50]}  |  OK")
    return True


async def scrape_external_posts(session: AsyncSession, archive_dir: Path,
                                 authors: list[dict],
                                 external_links: list[tuple]):
    """Скачивает все внешние посты и перезаписывает ссылки на них."""
    if not external_links:
        return

    # Дедупликация: один и тот же пост может быть у нескольких авторов
    # Группируем: для каждого (ref_nick, ext_nick, post_id) → скачать в ref_nick/Content/
    print(f"\n{'='*60}")
    print(f"  Downloading {len(external_links)} external posts (one level deep)...")
    print(f"{'='*60}")

    total_ext = len(external_links)
    counter = {"done": 0}
    sem = asyncio.Semaphore(PARALLEL_POSTS)

    async def _dl_one(ref_nick, ext_nick, url, post_id):
        async with sem:
            counter["done"] += 1
            num = counter["done"]
            print(f"\n  [{num}/{total_ext}] ext {ext_nick}/{post_id}")
            dest_dir = archive_dir / ref_nick / "Content"
            filename = f"ext_{ext_nick}_{post_id}.html"
            ext_base = f"https://{ext_nick}.livejournal.com/"
            ok = await scrape_external_post(session, url, dest_dir, filename, ext_base)
            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            return ok, ref_nick, ext_nick, post_id

    tasks = [_dl_one(*link) for link in external_links]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Теперь перезаписываем ссылки на скачанные внешние посты
    downloaded = set()
    for r in results:
        if isinstance(r, Exception):
            continue
        ok, ref_nick, ext_nick, post_id = r
        if ok:
            downloaded.add((ref_nick, ext_nick, post_id))

    if not downloaded:
        return

    # Проходим по файлам и заменяем ссылки на внешние посты
    for author in authors:
        nick = author["nickname"]
        content_dir = archive_dir / nick / "Content"
        if not content_dir.exists():
            continue

        for fpath in content_dir.glob("*.html"):
            if not fpath.stem.isdigit():
                continue
            try:
                html = fpath.read_text(encoding="utf-8")
            except Exception:
                continue

            modified = False

            def _replace_ext(m):
                nonlocal modified
                target_nick = (m.group(2) or m.group(3) or "").lower()
                post_id = m.group(4)
                if (nick, target_nick, post_id) in downloaded:
                    modified = True
                    return f'href="ext_{target_nick}_{post_id}.html"'
                return m.group(0)

            new_html = _LJ_POST_URL_RE.sub(_replace_ext, html)
            if modified:
                fpath.write_text(new_html, encoding="utf-8")

    print(f"  External posts downloaded: {len(downloaded)}")


# ═══════════════════════════════════════════════════════════════
#  ГЕНЕРАЦИЯ ОБЩЕГО ИНДЕКСА (ТОП-УРОВЕНЬ)
# ═══════════════════════════════════════════════════════════════

def generate_top_level_index(archive_dir: Path, authors: list[dict]):
    """Генерирует общий 01.MAIN.html со всеми авторами, годами, тегами и поиском."""
    from collections import defaultdict

    print("\n" + "=" * 60)
    print("  Generating top-level index...")
    print("=" * 60)

    parser = etree.HTMLParser(encoding="utf-8")
    all_posts = []  # (nick, post_data)
    by_year = defaultdict(list)
    by_tag = defaultdict(list)
    by_author = defaultdict(list)

    for author in authors:
        nick = author["nickname"]
        content_dir = archive_dir / nick / "Content"
        if not content_dir.exists():
            continue

        for fpath in sorted(content_dir.glob("*.html"),
                            key=lambda p: int(p.stem) if p.stem.isdigit() else 0):
            if not fpath.stem.isdigit():
                continue
            try:
                raw = fpath.read_bytes()
                doc = etree.fromstring(raw, parser)
                title_el = doc.find(".//h1")
                title = _el_text(title_el) if title_el is not None else f"Post {fpath.stem}"

                html_root = doc.getroottree().getroot()
                year = (html_root.get("data-year", "") or "")[:4]
                post_date = html_root.get("data-date", "") if html_root is not None else ""
                tags_raw = html_root.get("data-tags", "") if html_root is not None else ""
                tags = [t.strip().lstrip("#") for t in tags_raw.split("|") if t.strip()]

                p = {
                    "post_id": fpath.stem, "title": title or f"Post {fpath.stem}",
                    "year": year, "tags": tags, "file": fpath.name,
                    "post_date": post_date, "nick": nick,
                    "rel_path": f"{nick}/Content/{fpath.name}",
                }
                all_posts.append(p)
                by_author[nick].append(p)
                by_year[year or "?"].append(p)
                for t in tags:
                    by_tag[t].append(p)
            except Exception:
                pass

    # Секция авторов
    author_cards = []
    for author in authors:
        nick = author["nickname"]
        cnt = len(by_author.get(nick, []))
        author_cards.append(
            f'<a class="year-card" href="{nick}/01.MAIN.html" style="min-width:180px">'
            f'<span class="yr">{nick}</span>'
            f'<span class="cnt">{cnt} постов</span></a>'
        )
    authors_html = "\n".join(author_cards)

    # Генерируем страницы по годам (верхний уровень)
    top_content_dir = archive_dir / "Content"
    top_content_dir.mkdir(parents=True, exist_ok=True)

    for year, year_posts in sorted(by_year.items()):
        items_html = "\n".join(
            f'''<li class="post-item">
              <span class="post-num">{i}</span>
              <div style="flex:1">
                <a class="post-link" href="../{p["rel_path"]}">{p["title"] or "Без заголовка"}</a>
                <div class="post-tags-inline">
                  {f'<span class="post-meta">[{p["nick"]}]</span>' }
                  {f'<span class="post-meta">{p["post_date"]}</span>' if p.get("post_date") else ""}
                  {"".join(f'<a class="post-tag-mini" href="{_tag_href(t)}">{t}</a>' for t in p["tags"])}
                </div>
              </div>
            </li>'''
            for i, p in enumerate(year_posts, 1)
        )
        cnt = len(year_posts)
        body = f'''<div class="site-header">
  <div class="site-title">{year}</div>
  <div class="site-subtitle">{cnt} {"пост" if cnt == 1 else "постов"} · все авторы</div>
</div>
<ul class="post-list">{items_html}</ul>'''
        page = _nav_page(f"{year} — Архив", body, "../01.MAIN.html", "На главную")
        (top_content_dir / f"year_{year}.html").write_text(page, encoding="utf-8")

    # Генерируем страницы по тегам (верхний уровень)
    for tag, tag_posts in sorted(by_tag.items()):
        items_html = "\n".join(
            f'''<li class="post-item">
              <span class="post-num">{i}</span>
              <div style="flex:1">
                <a class="post-link" href="../{p["rel_path"]}">{p["title"] or "Без заголовка"}</a>
                <div class="post-tags-inline">
                  <span class="post-meta">[{p["nick"]}]</span>
                  <span class="post-meta">{p.get("post_date") or p["year"]}</span>
                  {"".join(f'<a class="post-tag-mini" href="{_tag_href(t)}">{t}</a>' for t in p["tags"])}
                </div>
              </div>
            </li>'''
            for i, p in enumerate(tag_posts, 1)
        )
        cnt = len(tag_posts)
        body = f'''<div class="site-header">
  <div class="site-title">#{tag}</div>
  <div class="site-subtitle">{cnt} {"пост" if cnt == 1 else "постов"} · все авторы</div>
</div>
<ul class="post-list">{items_html}</ul>'''
        page = _nav_page(f"#{tag} — Архив", body, "../01.MAIN.html", "На главную")
        (top_content_dir / _tag_filename(tag)).write_text(page, encoding="utf-8")

    print(f"  Top-level year pages: {len(by_year)}, tag pages: {len(by_tag)}")

    # Секция годов
    years_html = "\n".join(
        f'<a class="year-card" href="Content/year_{yr}.html">'
        f'<span class="yr">{yr}</span>'
        f'<span class="cnt">{len(ps)} постов</span></a>'
        for yr, ps in sorted(by_year.items())
    )

    # Секция тегов
    tags_html = "\n".join(
        f'<a class="tag-chip" href="Content/{_tag_href(t)}">'
        f'{t}<span class="tag-count">{len(ps)}</span></a>'
        for t, ps in sorted(by_tag.items(), key=lambda x: -len(x[1]))
    )

    total = len(all_posts)

    # Объединённый поисковый индекс
    print("  Building combined search index...")
    index_entries = []
    for p in all_posts:
        nick = p["nick"]
        content_dir = archive_dir / nick / "Content"
        try:
            fpath = content_dir / p["file"]
            raw = fpath.read_bytes()
            doc = etree.fromstring(raw, parser)
            for el in doc.iter("script", "style"):
                el.getparent().remove(el)
            plain = etree.tostring(doc, method="text", encoding="unicode")
            plain = re.sub(r"\s+", " ", plain).lower()
        except Exception:
            plain = p["title"].lower()
        index_entries.append({
            "file": p["rel_path"], "title": f"[{nick}] {p['title']}",
            "date": p.get("post_date", ""), "year": p["year"],
            "text": plain,
        })

    index_js = "var LJ_SEARCH_INDEX = " + json.dumps(
        index_entries, ensure_ascii=False, separators=(",", ":")
    ) + ";"
    (archive_dir / "search_index.js").write_text(index_js, encoding="utf-8")
    size_kb = (archive_dir / "search_index.js").stat().st_size // 1024
    print(f"  Combined search_index.js: {size_kb} KB, {len(index_entries)} posts")

    # Поиск — с путём к объединённому индексу
    search_html = SEARCH_BLOCK_TEMPLATE.replace(
        'src="Content/search_index.js"', 'src="search_index.js"'
    ).replace(
        'href="Content/', 'href="'
    )

    body_parts = [
        '<div class="site-header">',
        f'  <div class="site-title">Архив LiveJournal</div>',
        f'  <div class="site-subtitle">{len(authors)} авторов · {total} постов</div>',
        '</div>',
        '<div class="section-label" style="margin-top:36px">Авторы</div>',
        f'<div class="years-grid" style="margin-bottom:40px">{authors_html}</div>',
        '<div class="section-label">Годы (все авторы)</div>',
        f'<div class="years-grid" style="margin-bottom:40px">{years_html}</div>',
        '<div class="section-label">Теги (все авторы)</div>',
        f'<div class="tags-cloud">{tags_html or "<span class=empty-note>Теги не найдены</span>"}</div>',
        search_html,
    ]
    body = "\n".join(body_parts)
    main_page = _nav_page("Архив LiveJournal", body)
    (archive_dir / "01.MAIN.html").write_text(main_page, encoding="utf-8")
    print(f"  Top-level 01.MAIN.html created")


# ═══════════════════════════════════════════════════════════════
#  ТОЧКА ВХОДА
# ═══════════════════════════════════════════════════════════════

def main():
    print("=" * 60)
    print("  LiveJournal Archive Scraper")
    print("  Multi-author edition")
    print("=" * 60)

    # 1. Ввод параметров
    authors = ask_authors()
    archive_dir = ask_archive_dir()

    # Если все авторы — одиночные посты, годы не нужны
    all_single = all(a["single_post_url"] for a in authors)
    years = [] if all_single else ask_years()

    # 2. Проверка существующих архивов
    skip_existing = check_existing_archives(archive_dir, authors)

    # 3. Запуск async-пайплайна
    asyncio.run(_async_main(archive_dir, authors, years, skip_existing))


async def _async_main(archive_dir: Path, authors: list[dict],
                       years: list[int], skip_existing: dict[str, bool]):
    """Основной async-пайплайн: скачивание → кросс-ссылки → внешние посты → индексы."""

    async with AsyncSession(impersonate="chrome", headers=_CHROME_HEADERS) as session:

        # ── Фаза 1: Сбор URL и скачивание постов для каждого автора ──
        total_stats = {"success": 0, "skipped": 0, "errors": 0}

        for author in authors:
            nick = author["nickname"]
            base_url = author["base_url"]
            author_dir = archive_dir / nick
            content_dir = author_dir / "Content"
            content_dir.mkdir(parents=True, exist_ok=True)

            print(f"\n{'='*60}")
            print(f"  Author: {nick}")
            print(f"{'='*60}")

            if author["single_post_url"]:
                post_urls = [author["single_post_url"]]
            else:
                post_urls = await collect_all_post_urls(session, base_url, years)

            if not post_urls:
                print(f"\n  No posts found for {nick}.")
                continue

            print(f"\n  Found {len(post_urls)} posts for {nick}")

            stats = await download_author_posts(
                session, post_urls, content_dir, base_url,
                skip_existing=skip_existing.get(nick, True)
            )

            total_stats["success"] += stats["success"]
            total_stats["skipped"] += stats["skipped"]
            total_stats["errors"] += stats["errors"]

            if stats["failed"]:
                ids = ", ".join(extract_post_id(u) or "?" for _, u in stats["failed"])
                print(f"  Failed: {ids}")

        # ── Фаза 2: Кросс-авторские ссылки ──
        print(f"\n{'='*60}")
        print(f"  Rewriting cross-author links...")
        print(f"{'='*60}")
        external_links = collect_and_rewrite_cross_links(archive_dir, authors)
        print(f"  Found {len(external_links)} external post links")

        # ── Фаза 3: Скачивание внешних постов (один уровень) ──
        if external_links:
            await scrape_external_posts(session, archive_dir, authors, external_links)

    # ── Фаза 4: Генерация индексов (вне сессии — только файловые операции) ──

    # Per-author sub-main
    for author in authors:
        nick = author["nickname"]
        author_dir = archive_dir / nick
        content_dir = author_dir / "Content"
        if content_dir.exists():
            generate_index(author_dir, author["base_url"],
                          content_dir=content_dir,
                          parent_link="../01.MAIN.html")

    # Top-level main (только если >1 автора или всегда для единообразия)
    generate_top_level_index(archive_dir, authors)

    # ── Итоги ──
    print("\n" + "=" * 60)
    print(f"  ALL DONE")
    print(f"  Downloaded: {total_stats['success']}")
    print(f"  Skipped:    {total_stats['skipped']}")
    print(f"  Errors:     {total_stats['errors']}")
    print(f"  Archive:    {archive_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
