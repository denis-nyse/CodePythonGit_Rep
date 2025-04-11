import requests
from bs4 import BeautifulSoup
import asyncio
import json
import time
import logging
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from telegram.ext import Application, CommandHandler
from collections import defaultdict

# Настройка логгирования
LOG_FILE = 'C:\\parcer_site\\log.txt'
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Загрузка конфигурации
with open('C:\\parcer_site\\config.json') as config_file:
    config = json.load(config_file)

TOKEN = config['TOKEN']
CHAT_ID = config['CHAT_ID']

# Пути к файлам
URLS_FILE = 'C:\\parcer_site\\urls.txt'
LINKS_FILE = 'C:\\parcer_site\\links.txt'
ARTICLES_FILE = 'C:\\parcer_site\\articl.txt'

# Храним артикулы, чтобы не было повторов
seen_articles = set()

# Загрузка уже сохранённых артикулов, если файл существует
try:
    with open(ARTICLES_FILE, 'r') as f:
        for line in f:
            parts = line.strip().split(' - ')
            if len(parts) == 2:
                seen_articles.add(parts[1])
except FileNotFoundError:
    pass

site_article_count = defaultdict(int)

def fetch_page(url):
    try:
        logging.info(f"Запрашиваем страницу: {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        logging.error(f"Ошибка при запросе страницы {url}: {e}")
        return None

def extract_links(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    links = soup.find_all('a', class_='preview-card__img-wrapp-link')
    logging.info(f"Найдено {len(links)} ссылок.")
    return links

def save_links_to_file(links, file_path):
    with open(file_path, 'a') as file:
        for link in links:
            href = link.get('href')
            if href:
                file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {href}\n")
    logging.info(f"Ссылки сохранены в файл: {file_path}")

def extract_articles(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    articles = soup.find_all('span', class_='product-description__code')
    return [article.get_text().replace('Артикул:', '').strip() for article in articles]

def save_articles_to_file(articles, file_path, source_url):
    with open(file_path, 'a') as file:
        for article in articles:
            if article not in seen_articles:
                seen_articles.add(article)
                file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - {article}\n")
                if '.by' in source_url:
                    site_article_count['by'] += 1
                elif '.ru' in source_url:
                    site_article_count['ru'] += 1
    logging.info(f"Сохранено {len(articles)} новых артикулов из {source_url}")

async def send_links_to_telegram(links, chat_id, application):
    for link in links:
        href = link.get('href')
        if href:
            try:
                logging.info(f"Отправляем ссылку: {href}")
                await application.bot.send_message(chat_id=chat_id, text=href)
                await asyncio.sleep(10)
            except Exception as e:
                logging.error(f"Ошибка при отправке в Telegram: {e}")


def get_all_pagination_urls(first_url, html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    pagination_block = soup.find('div', class_='paging')
    if not pagination_block:
        return [first_url]

    page_links = pagination_block.find_all('a', class_='paging__link')
    page_numbers = set()
    for link in page_links:
        page_id = link.get('data-page-id')
        if page_id and page_id.isdigit():
            page_numbers.add(int(page_id))

    sorted_pages = sorted(page_numbers)

    parsed_url = urlparse(first_url)
    base_url = parsed_url.scheme + "://" + parsed_url.netloc + parsed_url.path
    original_params = parse_qs(parsed_url.query)

    all_urls = []
    for page_number in sorted_pages:
        params = original_params.copy()
        if page_number > 1:
            params['page'] = [str(page_number)]
        query_string = urlencode(params, doseq=True)
        full_url = base_url + '?' + query_string
        all_urls.append(full_url)

    logging.info(f"Найдено {len(all_urls)} страниц пагинации.")
    return all_urls


async def parse_site(base_url, application):
    logging.info(f"Начинаем парсинг: {base_url}")
    html_content = fetch_page(base_url)
    if not html_content:
        return False

    pagination_urls = get_all_pagination_urls(base_url, html_content)

    for url in pagination_urls:
        logging.info(f"Парсим страницу: {url}")
        content = fetch_page(url)
        if not content:
            continue

        links = extract_links(content)
        if links:
            save_links_to_file(links, LINKS_FILE)
            await send_links_to_telegram(links, CHAT_ID, application)

            for link in links:
                href = link.get('href')
                if href:
                    product_html = fetch_page(href)
                    if product_html:
                        articles = extract_articles(product_html)
                        if articles:
                            save_articles_to_file(articles, ARTICLES_FILE, href)
        else:
            logging.info(f"Ссылки не найдены на странице: {url}")
    return True

def load_urls(file_path):
    with open(file_path, 'r') as file:
        return [line.strip() for line in file if line.strip()]

async def parse_all_sites(application):
    urls = load_urls(URLS_FILE)
    any_links_found = False
    for url in urls:
        result = await parse_site(url, application)
        if result:
            any_links_found = True
    if not any_links_found:
        logging.info("Товары с нулевыми ценами не найдены.")

async def send_summary_to_telegram(application):
    summary = f"Парсинг завершён.\nАртикулов на .by сайтах: {site_article_count['by']}\nАртикулов на .ru сайтах: {site_article_count['ru']}"
    logging.info(summary)
    try:
        await application.bot.send_message(chat_id=CHAT_ID, text=summary)
    except Exception as e:
        logging.error(f"Ошибка при отправке итогового сообщения: {e}")

async def start(update, context):
    await update.message.reply_text('Привет! Я бот для парсинга сайта.')

def main():
    async def run_parsing(application):
        logging.info("Запуск бота")
        await parse_all_sites(application)
        await send_summary_to_telegram(application)
        logging.info("Завершение работы бота")

    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    asyncio.run(run_parsing(application))
    application.run_polling()

if __name__ == '__main__':
    main()