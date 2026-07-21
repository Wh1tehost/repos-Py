#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL-скрипт для автоматизации процесса скачивания, парсинга и сопоставления
данных прайса и складской справки. (Версия 13.0 - Отчет об ошибках в Excel + чистые логи)

Сайт: https://kvin.ru/cable/
"""

import os
import re
import logging
import requests
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Any
from urllib.parse import urljoin
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

# ═══════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════

BASE_URL = "https://kvin.ru/cable/"
TEMP_DIR = Path("./temp")
OUTPUT_DIR = Path("./output")
DOWNLOAD_TIMEOUT = 60  # секунды

KNOWN_BRANDS = [
    'ВВГНГ', 'ВВГНГ-LS', 'ВВГНГ-FRLS', 'ВВГНГ-LS-П', 'ВВГНГ-LSLT',
    'ВББШНГ', 'ВББШНГ-LS', 'ВБШВ', 'ВБШВНГ',
    'АВВГ', 'АВВГНГ-LS', 'АВБШВ', 'АПВБШВ', 'АПВВНГ',
    'КГ', 'КГ-ХЛ', 'КГЭ-ХЛ', 'КГТП', 'КОГ',
    'КВВГНГ-LS', 'КВВГЭНГ-LS', 'КВББШВ', 'КВБВНГ',
    'СИП-2', 'СИП-3', 'СИП-4',
    'ААБЛ', 'ААШВ', 'АСБ', 'АСБЛ', 'АСШВ',
    'ПУГВ', 'ПУВ', 'ПУВНГ',
    'РПШ', 'ПВС', 'ПНСВ', 'РКГМ',
    'ППГНГ', 'СБ', 'СБПУ', 'МКЭК', 'МКЭШ',
    'ТОФЛЕКС', 'КСК',
]

# ═══════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ═══════════════════════════════════════════════════════════════

def setup_logging() -> logging.Logger:
    logger = logging.getLogger('ETL_Process')
    logger.setLevel(logging.INFO)
    if logger.handlers:
        return logger
    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fh = logging.FileHandler('etl_process.log', encoding='utf-8', mode='w')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger

logger = setup_logging()

# ═══════════════════════════════════════════════════════════════
# ШАГ 1: СКАЧИВАНИЕ
# ═══════════════════════════════════════════════════════════════

def get_download_links() -> Tuple[str, str]:
    logger.info("Получение ссылок на файлы через Playwright")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        try:
            page.goto(BASE_URL, timeout=60000)
            page.wait_for_load_state('networkidle')
            logger.info(f"Открыт сайт: {BASE_URL}")
            
            price_link = warehouse_link = None
            links = page.query_selector_all('aside.sidebar a')
            
            for link in links:
                href = link.get_attribute('href')
                text = link.inner_text().lower()
                if href and ('price' in href.lower() or 'прайс' in text):
                    price_link = href
                elif href and ('sklad' in href.lower() or 'склад' in text):
                    warehouse_link = href
            
            if not price_link or not warehouse_link:
                raise ValueError(f"Не найдены ссылки. Прайс: {price_link}, Склад: {warehouse_link}")
            
            return urljoin(BASE_URL, price_link), urljoin(BASE_URL, warehouse_link)
        finally:
            browser.close()

def download_file(url: str, dest_path: Path) -> Path:
    logger.info(f"Скачивание файла: {url}")
    response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(dest_path, 'wb') as f:
        f.write(response.content)
    logger.info(f"Файл сохранён: {dest_path} ({len(response.content)} байт)")
    return dest_path

def download_files() -> Tuple[Path, Path]:
    logger.info("Начало скачивания файлов")
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    price_url, warehouse_url = get_download_links()
    return download_file(price_url, TEMP_DIR / price_url.split('/')[-1]), \
           download_file(warehouse_url, TEMP_DIR / warehouse_url.split('/')[-1])

# ═══════════════════════════════════════════════════════════════
# ШАГ 2: ПАРСИНГ СКЛАДА
# ═══════════════════════════════════════════════════════════════

def parse_warehouse_file(filepath: Path) -> pd.DataFrame:
    logger.info(f"Парсинг файла склада: {filepath}")
    all_sheets = pd.read_excel(filepath, sheet_name=None)
    
    main_dfs = []
    plan_dfs = []
    
    for sheet_name, df in all_sheets.items():
        is_plan_sheet = (
            'план' in str(sheet_name).lower() or 
            'Дата поступления' in df.columns or 
            'Закуплено' in df.columns
        )
        
        if is_plan_sheet:
            logger.info(f"Лист '{sheet_name}' определен как План поставок")
            df.columns = [str(col).strip() for col in df.columns]
            if 'Закуплено' in df.columns:
                df = df.rename(columns={'Закуплено': 'Количество'})
            
            if 'Номенклатура' in df.columns and 'Количество' in df.columns:
                cols_to_keep = ['Номенклатура', 'Количество']
                if 'Ед. изм.' in df.columns:
                    cols_to_keep.append('Ед. изм.')
                
                plan_df = df[cols_to_keep].copy()
                plan_df['Город'] = 'План поставок'
                if 'Ед. изм.' not in plan_df.columns:
                    plan_df['Ед. изм.'] = 'км'
                plan_dfs.append(plan_df)
        else:
            df = df[df['Номенклатура'].astype(str) != 'Номенклатура']
            df = df[~df['Номенклатура'].astype(str).str.contains(r'^[-—]+$', na=False)]
            df = df[~df['Номенклатура'].astype(str).str.contains('План поставок', na=False)]
            if 'Номенклатура' in df.columns and 'Количество' in df.columns:
                main_dfs.append(df)
    
    main_df = pd.concat(main_dfs, ignore_index=True) if main_dfs else pd.DataFrame(columns=['Номенклатура', 'Город', 'Ед. изм.', 'Количество'])
    plan_df = pd.concat(plan_dfs, ignore_index=True) if plan_dfs else pd.DataFrame(columns=['Номенклатура', 'Город', 'Ед. изм.', 'Количество'])
    
    combined_df = pd.concat([main_df, plan_df], ignore_index=True)
    combined_df['Количество'] = pd.to_numeric(combined_df['Количество'], errors='coerce').fillna(0)
    combined_df['Город'] = combined_df['Город'].astype(str).str.strip()
    combined_df['Номенклатура'] = combined_df['Номенклатура'].astype(str).str.strip()
    combined_df['Ед. изм.'] = combined_df['Ед. изм.'].astype(str).str.strip()
    
    combined_df = combined_df[combined_df['Номенклатура'].str.len() > 2]
    combined_df = combined_df[combined_df['Номенклатура'] != 'nan']
    
    grouped = combined_df.groupby(['Номенклатура', 'Город', 'Ед. изм.'])['Количество'].sum().reset_index()
    grouped.rename(columns={'Количество': 'Общий_остаток'}, inplace=True)
    
    logger.info(f"Склад: {grouped['Номенклатура'].nunique()} уникальных номенклатур, {len(grouped)} записей (включая план)")
    return grouped

# ═══════════════════════════════════════════════════════════════
# ШАГ 3: ПАРСИНГ ПРАЙСА
# ═══════════════════════════════════════════════════════════════

def is_header_row(row: pd.Series) -> bool:
    row_str = ' '.join(row.astype(str).values).upper()
    brand_count = sum(1 for brand in KNOWN_BRANDS if brand in row_str)
    
    first_cell = str(row.iloc[0]).strip().upper()
    second_cell = str(row.iloc[1]).strip().upper() if len(row) > 1 else ''
    
    is_data_row = (
        re.match(r'^\d+[ХX*]\d+', first_cell) or 
        first_cell in ('1', '6', '10') or
        re.match(r'^\d+[ХX*]\d+', second_cell)
    )
    
    if is_data_row and brand_count >= 1:
        return False
    return ('РАЗМЕР' in row_str and brand_count >= 1) or (brand_count >= 2)

def parse_price_file(filepath: Path) -> Dict[str, Dict[str, float]]:
    logger.info(f"Парсинг файла прайса: {filepath}")
    df = pd.read_excel(filepath, header=None).fillna('').astype(str)
    
    header_rows = [idx for idx, row in df.iterrows() if is_header_row(row)]
    logger.info(f"Найдено строк-заголовков: {len(header_rows)}")
    
    result = {}
    for i, header_idx in enumerate(header_rows):
        header_row = df.iloc[header_idx]
        next_header_idx = header_rows[i+1] if i+1 < len(header_rows) else len(df)
        
        size_cols = [col_idx for col_idx, cell in enumerate(header_row) if 'размер' in str(cell).lower()]
        if not size_cols:
            size_cols = [0]
            
        for j, size_col in enumerate(size_cols):
            next_size_col = size_cols[j+1] if j+1 < len(size_cols) else len(header_row)
            
            valid_brands = []
            for col_idx in range(size_col + 1, next_size_col):
                cell = str(header_row.iloc[col_idx]).strip()
                if not cell or cell.lower() in ('nan', 'none', ''):
                    continue
                if re.match(r'^\d+[хxХX*]\d+', cell):
                    continue
                if re.search(r'[A-Za-zА-Яа-я]', cell) and not re.match(r'^\d+\.?\d*$', cell.replace(',', '.')):
                    brand = re.sub(r'\s+', ' ', cell.upper()).strip()
                    valid_brands.append((col_idx, brand))
            
            for col_idx, brand in valid_brands:
                brand_data = {}
                for row_idx in range(header_idx + 1, next_header_idx):
                    row = df.iloc[row_idx]
                    target_size_col = size_col if size_col < len(row) else 0
                    size = str(row.iloc[target_size_col]).strip()
                    
                    if not size or size.lower() in ('nan', 'none', '') or re.match(r'^\d+\.?\d*$', size.replace(',', '.')):
                        continue
                    
                    price = None
                    for search_col in range(col_idx, len(row)):
                        val = str(row.iloc[search_col]).strip().replace(' ', '').replace(',', '.')
                        try:
                            num = float(val)
                            if num > 100:
                                price = num
                                break
                        except ValueError:
                            continue
                    
                    if price is not None:
                        brand_data[size] = price
                
                if brand_data:
                    if brand in result:
                        result[brand].update(brand_data)
                    else:
                        result[brand] = brand_data

    logger.info(f"Всего марок в прайсе: {len(result)}")
    return result

# ═══════════════════════════════════════════════════════════════
# ШАГ 4: ИЗВЛЕЧЕНИЕ МАРКИ И РАЗМЕРА
# ═══════════════════════════════════════════════════════════════

def normalize_size(size: str) -> str:
    if not size: return ''
    size = size.upper().replace('X', 'Х').replace('x', 'Х').replace('*', 'Х')
    size = re.sub(r'\s*([Х\+\/\-])\s*', r'\1', size)
    return re.sub(r'\s+', ' ', size).strip()

def parse_nomenclature(name: str) -> Tuple[str, str]:
    name = name.strip()
    name = re.sub(r'\s*\(А\)', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*\([NnНн](?:\s*,\s*[PРpр][EЕeе])?\)', '', name)
    name = re.sub(r'\s*\([PРpр][EЕeе]\)', '', name)
    
    match = re.match(r'^([A-Za-zА-Яа-я0-9\-\s]+?)\s+(\d+[xхXХ*].*)$', name)
    if not match:
        return re.sub(r'\s+', ' ', re.sub(r'-\d+\s*', ' ', name.upper()).strip()), ''
    
    brand = match.group(1).strip()
    size_part = match.group(2).strip()
    
    class_match = re.search(r'\s+(-?\d+(?:[.,]\d+)?(?:/\d+)?)$', size_part)
    size_class = class_match.group(1) if class_match else None
    if size_class:
        size_part = size_part[:class_match.start()].strip()
    
    suffix_match = re.search(r'\s+(мс|ок|мк|ос|ож|мн)$', size_part, re.IGNORECASE)
    suffix = suffix_match.group(1).lower() if suffix_match else None
    if suffix:
        size_part = size_part[:suffix_match.start()].strip()
    
    brand_upper = brand.upper()
    brand_upper = re.sub(r'-\d+\s*', ' ', brand_upper).strip()
    brand_upper = re.sub(r'\s+', ' ', brand_upper)
    
    if 'АСБЛ' in brand_upper:
        brand_upper = brand_upper.replace('АСБЛ', 'АСБ')
    if 'ЦААБЛ' in brand_upper:
        brand_upper = brand_upper.replace('ЦААБЛ', 'ААБЛ')
    
    if suffix in ['ож', 'мн']:
        brand_upper = f"{brand_upper} {suffix.upper()}"
    
    size_normalized = normalize_size(size_part)
    if size_class:
        if re.match(r'^(КГ|КГ-ХЛ|КГЭ)', brand_upper) and size_class.lstrip('-').replace(',','.') in ('380', '660'):
            brand_upper = f"{brand_upper} {size_class.lstrip('-')}"
        else:
            size_normalized = f"{size_normalized} {size_class}"
    
    return re.sub(r'\s+', ' ', brand_upper).strip(), size_normalized

# ═══════════════════════════════════════════════════════════════
# ШАГ 5: СОПОСТАВЛЕНИЕ
# ═══════════════════════════════════════════════════════════════

def normalize_brand_for_search(brand: str) -> str:
    return re.sub(r'\s+', ' ', brand.upper()).strip()

def find_brand_in_price(brand: str, price_data: Dict[str, Dict[str, float]], original_nomenclature: str = '') -> Optional[str]:
    brand_norm = normalize_brand_for_search(brand)
    if brand_norm in price_data: return brand_norm
    
    suffix_from_name = None
    if original_nomenclature:
        suffix_match = re.search(r'\s+(мс|ок|мк|ос|ож|мн)(?:\(|$|\s)', original_nomenclature, re.IGNORECASE)
        if suffix_match:
            suffix_from_name = suffix_match.group(1).lower()
    
    suffix_mapping = {'ок': 'ОЖ', 'ос': 'ОЖ', 'мс': 'МН', 'мк': 'МН', 'ож': 'ОЖ', 'мн': 'МН'}
    target_suffix = suffix_mapping.get(suffix_from_name) if suffix_from_name else None
    
    if target_suffix:
        brand_with_suffix = f"{brand_norm} {target_suffix}"
        if brand_with_suffix in price_data: return brand_with_suffix
        if f"{brand_norm}{target_suffix}" in price_data: return f"{brand_norm}{target_suffix}"
    
    for pb in price_data.keys():
        if normalize_brand_for_search(pb) == brand_norm: return pb

    brand_no_voltage = re.sub(r'-\d+\s*', ' ', brand_norm).strip()
    brand_no_voltage = re.sub(r'\s+', ' ', brand_no_voltage)
    if brand_no_voltage in price_data: return brand_no_voltage
    for pb in price_data.keys():
        if normalize_brand_for_search(pb) == brand_no_voltage: return pb

    brand_no_suffix = re.sub(r'\s+(ОЖ|МН)$', '', brand_no_voltage).strip()
    if brand_no_suffix in price_data: return brand_no_suffix
    for pb in price_data.keys():
        if normalize_brand_for_search(pb) == brand_no_suffix: return pb

    brand_base = re.sub(r'[\d\-\s]+', '', brand_norm)
    matches = []
    for pb in price_data.keys():
        pb_base = re.sub(r'[\d\-\s]+', '', normalize_brand_for_search(pb))
        if brand_base in pb_base or pb_base in brand_base:
            matches.append(pb)
    
    if len(matches) == 1: return matches[0]
    if len(matches) > 1:
        if target_suffix:
            for m in matches:
                if target_suffix in m.upper(): return m
        return matches[0]
    return None

def find_price(brand: str, size: str, price_data: Dict[str, Dict[str, float]], original_nomenclature: str = '') -> Tuple[Optional[float], str]:
    price_brand = find_brand_in_price(brand, price_data, original_nomenclature)
    if not price_brand:
        return None, 'NO_BRAND'
    
    brand_data = price_data[price_brand]
    size_norm = normalize_size(size)
    size_no_class = re.sub(r'-?[\d.,/]+$', '', size_norm).strip()
    
    search_variants = list(dict.fromkeys([size_norm, size_no_class, size_norm.replace('-', ''), size_no_class.replace('-', '')]))
    search_variants = [v for v in search_variants if v]
    
    for variant in search_variants:
        if variant in brand_data:
            price = brand_data[variant]
            return (None, 'ZERO_PRICE') if price <= 0 else (price, 'OK')
            
    for price_size, price in brand_data.items():
        price_size_norm = normalize_size(price_size)
        price_size_no_class = re.sub(r'-?[\d.,/]+$', '', price_size_norm).strip()
        if price_size_norm in search_variants or price_size_no_class in search_variants:
            return (None, 'ZERO_PRICE') if price <= 0 else (price, 'OK')
            
    return None, 'NO_SIZE'

def merge_data(warehouse_df: pd.DataFrame, price_data: Dict[str, Dict[str, float]]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Начало сопоставления данных")
    found_count = not_found_count = 0
    errors_list = []
    
    prices = []
    for _, row in warehouse_df.iterrows():
        brand, size = parse_nomenclature(row['Номенклатура'])
        price, reason = find_price(brand, size, price_data, original_nomenclature=row['Номенклатура'])
        
        if price is not None: 
            found_count += 1
            prices.append(price)
        else:
            not_found_count += 1
            prices.append(None)
            
            if reason == 'NO_BRAND':
                reason_text = "Марка отсутствует в прайсе"
            elif reason == 'NO_SIZE':
                reason_text = "Размер отсутствует для данной марки"
            else:
                reason_text = "Цена равна 0 или не указана"
                
            errors_list.append({
                'Номенклатура': row['Номенклатура'],
                'Город': row['Город'],
                'Ед. изм.': row['Ед. изм.'],
                'Общий_остаток': row['Общий_остаток'],
                'Причина': reason_text
            })
    
    warehouse_df['Цена'] = prices
    warehouse_df['Стоимость'] = warehouse_df.apply(
        lambda r: r['Общий_остаток'] * r['Цена'] if pd.notna(r['Цена']) else None, axis=1
    )
    warehouse_df = warehouse_df.sort_values(['Номенклатура', 'Город']).reset_index(drop=True)
    
    errors_df = pd.DataFrame(errors_list)
    if not errors_df.empty:
        errors_df = errors_df.sort_values(['Причина', 'Номенклатура', 'Город']).reset_index(drop=True)
    
    logger.info(f"Сопоставление завершено. Найдено: {found_count}, Не найдено: {not_found_count}")
    return warehouse_df, errors_df

# ═══════════════════════════════════════════════════════════════
# ШАГ 6: СОХРАНЕНИЕ И ГЛАВНАЯ ФУНКЦИЯ
# ═══════════════════════════════════════════════════════════════

def save_result(df: pd.DataFrame, output_path: Path) -> None:
    logger.info(f"Сохранение основного результата в {output_path}")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False, engine='openpyxl')
    logger.info(f"Основной результат успешно сохранён. Всего строк: {len(df)}")

def main() -> None:
    logger.info("=" * 60 + "\nНАЧАЛО ETL-ПРОЦЕССА\n" + "=" * 60)
    try:
        price_path, warehouse_path = download_files()
        warehouse_df = parse_warehouse_file(warehouse_path)
        price_data = parse_price_file(price_path)
        
        result_df, errors_df = merge_data(warehouse_df, price_data)
        
        # 1. Сохраняем основной результат
        output_path = OUTPUT_DIR / f"merged_price_warehouse_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
        save_result(result_df, output_path)
        
        # 2. Сохраняем отчет об ошибках (если он не пустой)
        if not errors_df.empty:
            error_report_path = OUTPUT_DIR / f"not_found_report_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
            logger.info(f"Сохранение отчета о ненайденных позициях в {error_report_path}")
            errors_df.to_excel(error_report_path, index=False, engine='openpyxl')
            logger.info(f"Отчет успешно сохранён. Всего ненайденных строк: {len(errors_df)}")
        else:
            logger.info("Все позиции успешно сопоставлены, отчет об ошибках не требуется.")
            
        logger.info("=" * 60 + "\nETL-ПРОЦЕСС ЗАВЕРШЁН УСПЕШНО\n" + "=" * 60)
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)

if __name__ == "__main__":
    main()