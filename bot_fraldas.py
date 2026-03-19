"""
Bot Monitor de Preços de Fraldas
Otimizado para PythonAnywhere — v3

Novidades v3:
- Média móvel de 21 dias por marca/tamanho (preço/unidade)
- Alerta Telegram quando preço está ABAIXO da MM21 (oportunidade)
- Alerta Telegram quando preço está ACIMA da MM21 (aviso)
- Coluna mm21 salva no CSV para o dashboard
- Nunca para de monitorar, independente do sinal

v2 (mantido):
- Busca Pampers E Huggies, tamanhos P/M/G/GG
- Extrai quantidade do pacote do nome do produto
- Calcula preço por unidade para comparação justa

SETUP:
1. Upload em Files > /home/SEU_USER/bot_fraldas.py
2. Console Bash:
       pip3.10 install --user requests beautifulsoup4 pandas lxml
3. Edite USERNAME abaixo
4. Teste: python3.10 bot_fraldas.py
5. Tasks (menu superior):
       08:00 → /home/SEU_USER/.local/bin/python3.10 /home/SEU_USER/bot_fraldas.py
       20:00 → (mesmo comando)
"""

import os, re, json, time, random, logging, requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────
#  CONFIGURAÇÕES — edite aqui
# ─────────────────────────────────────────────

USERNAME     = "SEU_USER"   # ⚠️ troque pelo seu username do PythonAnywhere
ARQUIVO_CSV  = f"/home/{USERNAME}/precos_fraldas.csv"
LOG_FILE     = f"/home/{USERNAME}/bot_fraldas.log"

PRECO_ALERTA_POR_UNIDADE = 1.20   # alerta se preço/unidade <= R$ 1,20
INTERVALO_HORAS          = 6

# Média Móvel de 21 dias
MM_JANELA      = 21    # janela em dias
MM_LIMIAR_PERC = 0.03  # ignora desvios < 3% para evitar spam

PAGINAS_ML     = 2
PAGINAS_AMAZON = 2

# Telegram (opcional)
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN",   "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

# ScraperAPI (opcional — melhora Amazon)
SCRAPERAPI_KEY = os.environ.get("SCRAPERAPI_KEY", "")

# ─────────────────────────────────────────────
#  COMBINAÇÕES DE BUSCA
#  Todas as combinações marca × tamanho
# ─────────────────────────────────────────────

MARCAS   = ["pampers", "huggies"]
TAMANHOS = ["P", "M", "G", "GG"]

BUSCAS = [f"fralda {marca} {tam}" for marca in MARCAS for tam in TAMANHOS]
# Resulta em:
# "fralda pampers P", "fralda pampers M", "fralda pampers G", "fralda pampers GG",
# "fralda huggies P", "fralda huggies M", "fralda huggies G", "fralda huggies GG"

# ─────────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  EXTRATOR DE INFORMAÇÕES DO NOME DO PRODUTO
# ─────────────────────────────────────────────

# Padrões para extrair quantidade de unidades do nome
_QTDE_PATTERNS = [
    # "com 96 unidades", "c/ 96 un", "96 fraldas"
    r"(?:com|c/|com\s)\s*(\d+)\s*(?:unidades?|un\.?|fraldas?)",
    # "pacote 96", "pct 96", "kit 96"
    r"(?:pacote|pct|kit|embalagem|emb)\.?\s+(\d+)",
    # "96un", "96unid", "96pcs"
    r"(\d+)\s*(?:un\.?|unid\.?|pcs?|fraldas?)\b",
    # standalone: número entre 8 e 200 que provavelmente é quantidade
    r"\b(8|10|12|16|18|20|24|26|28|30|32|34|36|40|44|48|50|52|56|60|64|68|72|80|88|92|96|100|112|120|144|168|192|200)\b",
]

_TAMANHO_PATTERNS = {
    "RN": r"\b(RN|recem[- ]?nascido|rec[eé]m[- ]?nascido)\b",
    "P":  r"\b(P|pequeno|small)\b",
    "M":  r"\b(M|medio|m[eé]dio|medium)\b",
    "G":  r"\b(G|grande|large)\b(?!\s*G)",   # G mas não GG
    "GG": r"\b(GG|XG|extra[- ]?grande|extra[- ]?large|XL|XXL)\b",
    "XG": r"\b(XG)\b",
    "XXG":r"\b(XXG|XXXG)\b",
}

_MARCA_PATTERNS = {
    "Pampers":  r"\bpampers\b",
    "Huggies":  r"\bhuggies\b",
    "Turma da Mônica": r"\bturma\s+da\s+m[oô]nica\b",
    "MamyPoko": r"\bmamypoko\b",
    "Babysec":  r"\bbabysec\b",
}


def extrair_quantidade(nome: str) -> int | None:
    """Extrai a quantidade de unidades do pacote a partir do nome."""
    nome_lower = nome.lower()
    for pat in _QTDE_PATTERNS:
        m = re.search(pat, nome_lower, re.IGNORECASE)
        if m:
            qtde = int(m.group(1))
            # Sanidade: fraldas vêm em pacotes de 8 a 200 unidades
            if 8 <= qtde <= 200:
                return qtde
    return None


def extrair_tamanho(nome: str) -> str:
    """Extrai o tamanho (P/M/G/GG/etc.) do nome do produto."""
    nome_upper = nome.upper()
    for tam, pat in _TAMANHO_PATTERNS.items():
        if re.search(pat, nome_upper, re.IGNORECASE):
            return tam
    return "?"


def extrair_marca(nome: str) -> str:
    """Identifica a marca no nome do produto."""
    nome_lower = nome.lower()
    for marca, pat in _MARCA_PATTERNS.items():
        if re.search(pat, nome_lower, re.IGNORECASE):
            return marca
    return "Outra"


def enriquecer(registro: dict) -> dict:
    """Adiciona campos calculados a um registro de produto."""
    nome = registro.get("nome", "")
    preco = registro.get("preco")

    qtde   = extrair_quantidade(nome)
    tam    = extrair_tamanho(nome)
    marca  = extrair_marca(nome)
    preco_un = round(preco / qtde, 4) if (preco and qtde) else None

    return {
        **registro,
        "marca":       marca,
        "tamanho":     tam,
        "qtde_un":     qtde,
        "preco_un":    preco_un,
        "preco_pacote": preco,
    }


# ─────────────────────────────────────────────
#  HELPERS DE REDE
# ─────────────────────────────────────────────

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]


def get_headers(referer="https://www.google.com.br"):
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }


def pausa(a=2.0, b=5.0):
    time.sleep(random.uniform(a, b))


def limpar_preco(txt: str) -> float | None:
    if not txt:
        return None
    txt = re.sub(r"[^\d,.]", "", txt.strip())
    if "," in txt and "." in txt:
        txt = txt.replace(".", "").replace(",", ".")
    elif "," in txt:
        txt = txt.replace(",", ".")
    try:
        v = float(txt)
        return v if 0.5 < v < 5000 else None
    except ValueError:
        return None


def enviar_telegram(msg: str):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"Telegram: {e}")


# ─────────────────────────────────────────────
#  SCRAPERS
# ─────────────────────────────────────────────

def scraper_mercadolivre(busca: str, paginas=PAGINAS_ML) -> list[dict]:
    resultados = []
    for pagina in range(1, paginas + 1):
        offset = (pagina - 1) * 48 + 1
        slug = busca.replace(" ", "-")
        url = f"https://lista.mercadolivre.com.br/{slug}_Desde_{offset}_NoIndex_True"
        try:
            r = requests.get(url, headers=get_headers("https://www.mercadolivre.com.br"), timeout=15)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            items = soup.select(".ui-search-result__wrapper") or soup.select("[class*='ui-search-result']")
            for item in items:
                try:
                    nome_el = item.select_one(".ui-search-item__title, h2")
                    if not nome_el:
                        continue
                    nome = nome_el.get_text(strip=True)
                    inteiro = item.select_one(".andes-money-amount__fraction")
                    centavos = item.select_one(".andes-money-amount__cents")
                    if not inteiro:
                        continue
                    ptxt = inteiro.get_text(strip=True).replace(".", "")
                    if centavos:
                        ptxt += "." + centavos.get_text(strip=True)
                    preco = limpar_preco(ptxt)
                    link_el = item.select_one("a.ui-search-link, a[href*='mercadolivre']")
                    link = link_el["href"].split("?")[0] if link_el else ""
                    if nome and preco:
                        resultados.append({"site": "Mercado Livre", "nome": nome, "preco": preco, "link": link, "busca": busca})
                except Exception:
                    pass
        except Exception as e:
            log.error(f"[ML] '{busca}' p{pagina}: {e}")
        pausa()
    return resultados


def scraper_drogasil(busca: str) -> list[dict]:
    resultados = []
    url = f"https://www.drogasil.com.br/busca?q={busca.replace(' ', '+')}"
    try:
        r = requests.get(url, headers=get_headers("https://www.drogasil.com.br"), timeout=15)
        r.raise_for_status()

        # Estratégia 1: __NEXT_DATA__
        match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', r.text, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                props = data.get("props", {}).get("pageProps", {})
                prods = props.get("searchResult", {}).get("products", []) or props.get("products", [])
                for p in prods[:30]:
                    nome = p.get("name") or p.get("title", "")
                    preco = float(p.get("price") or p.get("salePrice") or 0)
                    slug = p.get("slug") or p.get("url", "")
                    link = f"https://www.drogasil.com.br/{slug}" if slug and not slug.startswith("http") else slug
                    if nome and preco > 0:
                        resultados.append({"site": "Drogasil", "nome": nome, "preco": preco, "link": link, "busca": busca})
            except Exception:
                pass

        # Estratégia 2: CSS fallback
        if not resultados:
            soup = BeautifulSoup(r.text, "lxml")
            for item in soup.select(".product-card, [class*='ProductCard']")[:20]:
                try:
                    nome = item.select_one("[class*='name'], h2").get_text(strip=True)
                    preco = limpar_preco(item.select_one("[class*='price'], .value").get_text(strip=True))
                    link_el = item.select_one("a")
                    link = "https://www.drogasil.com.br" + link_el["href"] if link_el else ""
                    if nome and preco:
                        resultados.append({"site": "Drogasil", "nome": nome, "preco": preco, "link": link, "busca": busca})
                except Exception:
                    pass
    except Exception as e:
        log.error(f"[Drogasil] '{busca}': {e}")
    pausa()
    return resultados


def scraper_amazon(busca: str, paginas=PAGINAS_AMAZON) -> list[dict]:
    resultados = []
    headers_amz = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "pt-BR,pt;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
    }
    for pagina in range(1, paginas + 1):
        target = f"https://www.amazon.com.br/s?k={busca.replace(' ', '+')}&page={pagina}"
        url = f"http://api.scraperapi.com?api_key={SCRAPERAPI_KEY}&url={target}&country_code=br" if SCRAPERAPI_KEY else target
        try:
            r = requests.get(url, headers=headers_amz, timeout=20)
            if "Robot Check" in r.text or r.status_code in (503, 429):
                log.warning("[Amazon] Bloqueio — configure SCRAPERAPI_KEY para contornar")
                break
            soup = BeautifulSoup(r.text, "lxml")
            items = soup.select('[data-component-type="s-search-result"]')
            if not items:
                break
            for item in items:
                try:
                    nome_el = item.select_one("h2 span")
                    if not nome_el:
                        continue
                    nome = nome_el.get_text(strip=True)
                    inteiro = item.select_one(".a-price-whole")
                    frac = item.select_one(".a-price-fraction")
                    if not inteiro:
                        continue
                    ptxt = inteiro.get_text(strip=True).replace(".", "").replace(",", "")
                    ptxt += "." + (frac.get_text(strip=True) if frac else "00")
                    preco = limpar_preco(ptxt)
                    link_el = item.select_one("h2 a")
                    link = "https://www.amazon.com.br" + link_el["href"].split("?")[0] if link_el else ""
                    if nome and preco:
                        resultados.append({"site": "Amazon", "nome": nome, "preco": preco, "link": link, "busca": busca})
                except Exception:
                    pass
        except Exception as e:
            log.error(f"[Amazon] '{busca}' p{pagina}: {e}")
        pausa(3, 6)
    return resultados


def scraper_shopee(busca: str, limite=20) -> list[dict]:
    resultados = []
    url = "https://shopee.com.br/api/v4/search/search_items"
    params = {
        "by": "relevancy", "keyword": busca, "limit": limite,
        "newest": 0, "order": "desc", "page_type": "search",
        "scenario": "PAGE_GLOBAL_SEARCH", "version": 2,
        "country": "BR", "currency": "BRL", "region": "BR",
    }
    headers_sh = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json",
        "Referer": f"https://shopee.com.br/search?keyword={busca.replace(' ', '+')}",
        "X-API-SOURCE": "pc",
        "X-Shopee-Language": "pt-BR",
    }
    try:
        r = requests.get(url, headers=headers_sh, params=params, timeout=15)
        r.raise_for_status()
        for item in r.json().get("items", []):
            try:
                info = item.get("item_basic", {})
                nome = info.get("name", "").strip()
                preco = round((info.get("price") or info.get("price_min") or 0) / 100000, 2)
                shop_id = info.get("shopid", "")
                item_id = info.get("itemid", "")
                link = f"https://shopee.com.br/product/{shop_id}/{item_id}"
                if nome and 0.5 < preco < 5000:
                    resultados.append({"site": "Shopee", "nome": nome, "preco": preco, "link": link, "busca": busca})
            except Exception:
                pass
    except Exception as e:
        log.error(f"[Shopee] '{busca}': {e}")
    pausa()
    return resultados


# ─────────────────────────────────────────────
#  COLETA COMPLETA (todas as buscas × todos os sites)
# ─────────────────────────────────────────────

def coletar_tudo() -> pd.DataFrame:
    todos = []
    total_buscas = len(BUSCAS)

    for i, busca in enumerate(BUSCAS, 1):
        log.info(f"[{i}/{total_buscas}] Buscando: '{busca}'")
        todos += scraper_mercadolivre(busca)
        todos += scraper_drogasil(busca)
        todos += scraper_amazon(busca)
        todos += scraper_shopee(busca)
        # Pausa maior entre buscas para não sobrecarregar
        if i < total_buscas:
            pausa(4, 8)

    if not todos:
        log.warning("Nenhum produto coletado.")
        return pd.DataFrame()

    # Enriquecer cada registro com marca, tamanho, qtde, preço/un
    enriquecidos = [enriquecer(r) for r in todos]
    df = pd.DataFrame(enriquecidos)
    df["data"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Filtros de qualidade
    df = df.dropna(subset=["preco"])
    df = df[df["preco"] > 0]

    # Remove duplicatas exatas (mesmo nome + site + preço)
    df = df.drop_duplicates(subset=["site", "nome", "preco"])

    # Ordena por tamanho > marca > preco_un
    ordem_tam = {"RN": 0, "P": 1, "M": 2, "G": 3, "GG": 4, "XG": 5, "XXG": 6, "?": 7}
    df["_tam_ord"] = df["tamanho"].map(ordem_tam).fillna(7)
    df = df.sort_values(["_tam_ord", "marca", "preco_un"]).drop(columns=["_tam_ord"])
    df = df.reset_index(drop=True)

    return df



# ─────────────────────────────────────────────
#  MÉDIA MÓVEL DE 21 DIAS
# ─────────────────────────────────────────────

def calcular_mm21(arquivo: str = ARQUIVO_CSV) -> pd.DataFrame:
    """
    Lê o CSV histórico e calcula a média móvel de 21 dias
    do menor preço/unidade por (marca, tamanho, site) em cada dia.

    Retorna DataFrame indexado por (marca, tamanho, site) com colunas:
        mm21       — valor atual da MM21 (preço/un)
        mm21_data  — data do último ponto calculado
        n_dias     — quantos dias distintos compõem a janela
    """
    try:
        hist = pd.read_csv(arquivo)
    except FileNotFoundError:
        return pd.DataFrame()

    hist["data"]     = pd.to_datetime(hist["data"], errors="coerce")
    hist["preco_un"] = pd.to_numeric(hist["preco_un"], errors="coerce")
    hist = hist.dropna(subset=["data", "preco_un", "marca", "tamanho"])
    hist["dia"] = hist["data"].dt.date

    # Menor preço/un por (dia, marca, tamanho, site)
    diario = (
        hist.groupby(["dia", "marca", "tamanho", "site"])["preco_un"]
        .min()
        .reset_index()
    )

    resultados = []
    for (marca, tamanho, site), grupo in diario.groupby(["marca", "tamanho", "site"]):
        grupo = grupo.sort_values("dia")
        # Janela dos últimos MM_JANELA dias
        ultimo_dia = grupo["dia"].max()
        corte = pd.Timestamp(ultimo_dia) - pd.Timedelta(days=MM_JANELA)
        janela = grupo[grupo["dia"] >= corte.date()]
        if janela.empty:
            continue
        mm21_val  = round(janela["preco_un"].mean(), 6)
        n_dias    = janela["dia"].nunique()
        resultados.append({
            "marca":     marca,
            "tamanho":   tamanho,
            "site":      site,
            "mm21":      mm21_val,
            "mm21_data": str(ultimo_dia),
            "n_dias":    n_dias,
        })

    return pd.DataFrame(resultados) if resultados else pd.DataFrame()


def sinal_mm21(preco_atual: float, mm21: float) -> tuple[str, float]:
    """
    Compara preço atual com a MM21.
    Retorna (sinal, desvio_percentual).
      sinal: 'abaixo' | 'acima' | 'neutro'
    """
    if mm21 <= 0:
        return "neutro", 0.0
    desvio = (preco_atual - mm21) / mm21   # negativo = abaixo da MM
    if desvio <= -MM_LIMIAR_PERC:
        return "abaixo", desvio
    if desvio >= MM_LIMIAR_PERC:
        return "acima", desvio
    return "neutro", desvio


def alertas_mm21(df_novo: pd.DataFrame):
    """
    Cruza os melhores preços da coleta atual com a MM21 histórica
    e dispara alertas Telegram para cada combinação relevante.
    Sempre monitora — nunca para por causa do sinal.
    """
    df_mm = calcular_mm21()
    if df_mm.empty:
        log.info("[MM21] Histórico insuficiente para calcular média móvel.")
        return

    # Melhor preço/un desta coleta por (marca, tamanho, site)
    df_v = df_novo[df_novo["preco_un"].notna()].copy()
    if df_v.empty:
        return

    melhores = (
        df_v.sort_values("preco_un")
        .groupby(["marca", "tamanho", "site"])
        .first()
        .reset_index()
    )

    alertas_enviados = 0
    for _, row in melhores.iterrows():
        match = df_mm[
            (df_mm["marca"]   == row["marca"]) &
            (df_mm["tamanho"] == row["tamanho"]) &
            (df_mm["site"]    == row["site"])
        ]
        if match.empty:
            continue

        mm21_val = match.iloc[0]["mm21"]
        n_dias   = match.iloc[0]["n_dias"]
        sinal, desvio = sinal_mm21(row["preco_un"], mm21_val)

        if sinal == "neutro":
            continue

        desvio_pct = abs(desvio) * 100
        qtde_str   = f"{int(row['qtde_un'])}un" if pd.notna(row.get("qtde_un")) else "?"

        if sinal == "abaixo":
            emoji  = "📉"
            titulo = "ABAIXO da média — oportunidade!"
            cor    = "▼"
        else:
            emoji  = "📈"
            titulo = "ACIMA da média — preço elevado"
            cor    = "▲"

        msg = (
            f"{emoji} <b>{titulo}</b>\n"
            f"\n"
            f"<b>{row['marca']} {row['tamanho']} — {qtde_str}</b>\n"
            f"🏪 {row['site']}\n"
            f"\n"
            f"Preço atual:  <b>R$ {row['preco_un']:.4f}/un</b>\n"
            f"MM21 ({n_dias}d): R$ {mm21_val:.4f}/un\n"
            f"Desvio:  {cor} {desvio_pct:.1f}%\n"
            f"\n"
            f"📦 {row['nome'][:55]}\n"
            f"💰 R$ {row['preco_pacote']:.2f} no pacote\n"
            f"🔗 {row['link']}"
        )

        nivel = "ABAIXO" if sinal == "abaixo" else "ACIMA"
        log.info(
            f"[MM21] {nivel} {desvio_pct:.1f}% — "
            f"{row['marca']} {row['tamanho']} {qtde_str} "
            f"{row['site']} — R${row['preco_un']:.4f}/un "
            f"(MM21 R${mm21_val:.4f})"
        )
        enviar_telegram(msg)
        alertas_enviados += 1

    if alertas_enviados == 0:
        log.info("[MM21] Todos os preços dentro da banda neutra (±{:.0f}%)".format(MM_LIMIAR_PERC * 100))


def enriquecer_com_mm21(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona coluna mm21 ao DataFrame da coleta atual.
    Útil para o dashboard — mantém o dado disponível no CSV.
    """
    df_mm = calcular_mm21()
    if df_mm.empty:
        df["mm21"] = None
        return df

    df = df.merge(
        df_mm[["marca", "tamanho", "site", "mm21"]],
        on=["marca", "tamanho", "site"],
        how="left",
    )
    return df


# ─────────────────────────────────────────────
#  RELATÓRIO E ALERTAS
# ─────────────────────────────────────────────

def imprimir_ranking(df: pd.DataFrame):
    """Imprime o ranking de melhores preços por tamanho e marca."""
    log.info("\n" + "═" * 65)
    log.info("  RANKING — MELHOR PREÇO/UNIDADE POR TAMANHO E MARCA")
    log.info("═" * 65)

    df_com_qtde = df[df["qtde_un"].notna() & df["preco_un"].notna()]

    if df_com_qtde.empty:
        log.info("  Sem produtos com quantidade identificada.")
        return

    for tam in ["RN", "P", "M", "G", "GG", "XG", "XXG"]:
        bloco = df_com_qtde[df_com_qtde["tamanho"] == tam]
        if bloco.empty:
            continue
        log.info(f"\n  Tamanho {tam}:")
        log.info(f"  {'Marca':<12} {'Site':<16} {'Pacote':>6} {'Preço':>8} {'R$/un':>7}  Nome")
        log.info(f"  {'-'*12} {'-'*16} {'-'*6} {'-'*8} {'-'*7}  {'-'*30}")
        top = bloco.sort_values("preco_un").head(10)
        for _, row in top.iterrows():
            log.info(
                f"  {row['marca']:<12} {row['site']:<16} "
                f"{int(row['qtde_un']):>5}un  "
                f"R${row['preco_pacote']:>6.2f}  "
                f"R${row['preco_un']:>5.4f}  "
                f"{row['nome'][:45]}"
            )

    log.info("\n" + "═" * 65)


def verificar_alertas(df: pd.DataFrame):
    """Alerta para produtos com preço/unidade abaixo do limite."""
    df_v = df[df["preco_un"].notna()]
    ofertas = df_v[df_v["preco_un"] <= PRECO_ALERTA_POR_UNIDADE].sort_values("preco_un")

    if ofertas.empty:
        log.info(f"Nenhuma oferta abaixo de R$ {PRECO_ALERTA_POR_UNIDADE:.2f}/un")
        return

    log.info(f"\n*** {len(ofertas)} oferta(s) abaixo de R$ {PRECO_ALERTA_POR_UNIDADE:.2f}/un ***")
    for _, row in ofertas.head(8).iterrows():
        msg = (
            f"🍼 <b>Oferta de fralda!</b>\n"
            f"<b>{row['marca']} — Tam. {row['tamanho']} — {int(row['qtde_un'])}un</b>\n"
            f"💰 R$ {row['preco_pacote']:.2f} no pacote  |  "
            f"<b>R$ {row['preco_un']:.4f}/un</b>\n"
            f"🏪 {row['site']}\n"
            f"📦 {row['nome'][:60]}\n"
            f"🔗 {row['link']}"
        )
        log.info(f"ALERTA: {row['marca']} {row['tamanho']} {int(row['qtde_un'])}un — "
                 f"R${row['preco_un']:.4f}/un — {row['site']}")
        enviar_telegram(msg)


def salvar_csv(df: pd.DataFrame):
    try:
        historico = pd.read_csv(ARQUIVO_CSV)
        historico = pd.concat([historico, df], ignore_index=True)
    except FileNotFoundError:
        historico = df
    historico.to_csv(ARQUIVO_CSV, index=False, encoding="utf-8-sig")
    log.info(f"CSV salvo: {ARQUIVO_CSV} ({len(historico)} registros totais)")


# ─────────────────────────────────────────────
#  ORQUESTRADOR
# ─────────────────────────────────────────────

def rodar_coleta():
    inicio = datetime.now()
    log.info(f"\n{'='*55}")
    log.info(f"Coleta iniciada — {inicio.strftime('%d/%m/%Y %H:%M:%S')}")
    log.info(f"Buscas: {len(BUSCAS)} combinações ({len(MARCAS)} marcas × {len(TAMANHOS)} tamanhos)")
    log.info(f"{'='*55}")

    df = coletar_tudo()
    if df.empty:
        return None

    log.info(f"\nTotal bruto: {len(df)} produtos")
    log.info(f"Com tamanho identificado: {(df['tamanho'] != '?').sum()}")
    log.info(f"Com qtde identificada:    {df['qtde_un'].notna().sum()}")
    log.info(f"Com preço/un calculado:   {df['preco_un'].notna().sum()}")

    imprimir_ranking(df)
    verificar_alertas(df)
    df = enriquecer_com_mm21(df)
    alertas_mm21(df)
    salvar_csv(df)

    duracao = (datetime.now() - inicio).seconds
    log.info(f"\nColeta concluída em {duracao}s")
    return df


def modo_unico():
    rodar_coleta()


def modo_loop():
    log.info(f"Modo loop — intervalo: {INTERVALO_HORAS}h")
    ciclo = 0
    while True:
        ciclo += 1
        log.info(f"\n>>> Ciclo #{ciclo}")
        try:
            rodar_coleta()
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.error(f"Erro no ciclo {ciclo}: {e}")
            time.sleep(300)
            continue
        proxima = datetime.fromtimestamp(time.time() + INTERVALO_HORAS * 3600)
        log.info(f"Próxima coleta: {proxima.strftime('%d/%m %H:%M')}")
        try:
            time.sleep(INTERVALO_HORAS * 3600)
        except KeyboardInterrupt:
            break


# ─────────────────────────────────────────────
#  UTILITÁRIO — análise do CSV salvo
# ─────────────────────────────────────────────

def analisar_historico(arquivo=ARQUIVO_CSV):
    """
    Mostra análise do histórico. Rode no console PA:
        python3.10 bot_fraldas.py --analisar
    """
    try:
        df = pd.read_csv(arquivo)
    except FileNotFoundError:
        print("CSV não encontrado. Rode a coleta primeiro.")
        return

    df["preco_un"] = pd.to_numeric(df["preco_un"], errors="coerce")
    df["qtde_un"]  = pd.to_numeric(df["qtde_un"],  errors="coerce")
    df["data"]     = pd.to_datetime(df["data"], errors="coerce")

    print(f"\n{'═'*65}")
    print(f"  HISTÓRICO — {arquivo}")
    print(f"  {len(df)} registros | {df['data'].nunique()} coletas | "
          f"{df['data'].min().strftime('%d/%m')} a {df['data'].max().strftime('%d/%m')}")
    print(f"{'═'*65}")

    df_v = df[df["preco_un"].notna()]

    for tam in ["P", "M", "G", "GG"]:
        bloco = df_v[df_v["tamanho"] == tam]
        if bloco.empty:
            continue
        print(f"\n  Tamanho {tam} — melhor preço/unidade histórico:")
        print(f"  {'Marca':<12} {'Min R$/un':>9} {'Méd R$/un':>9} {'Site':<16} {'Data'}")
        print(f"  {'-'*12} {'-'*9} {'-'*9} {'-'*16} {'-'*10}")
        for marca in ["Pampers", "Huggies"]:
            sub = bloco[bloco["marca"] == marca]
            if sub.empty:
                continue
            idx_min = sub["preco_un"].idxmin()
            melhor = sub.loc[idx_min]
            media = sub["preco_un"].mean()
            print(
                f"  {marca:<12} "
                f"R${melhor['preco_un']:>6.4f}   "
                f"R${media:>6.4f}   "
                f"{melhor['site']:<16} "
                f"{pd.to_datetime(melhor['data']).strftime('%d/%m %H:%M')}"
            )

    print(f"\n{'═'*65}\n")


# ─────────────────────────────────────────────
#  ENTRADA PRINCIPAL
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    args = sys.argv[1:]

    if "--loop" in args:
        modo_loop()
    elif "--analisar" in args:
        analisar_historico()
    elif "--mm21" in args:
        df_mm = calcular_mm21()
        if df_mm.empty:
            print("Sem dados suficientes para MM21.")
        else:
            print(df_mm.sort_values(["marca", "tamanho", "site"]).to_string(index=False))
    else:
        modo_unico()

    # Uso:
    #   python3.10 bot_fraldas.py              → coleta única (use nas Tasks do PA)
    #   python3.10 bot_fraldas.py --loop       → loop contínuo (console Bash)
    #   python3.10 bot_fraldas.py --analisar   → relatório do histórico
    #   python3.10 bot_fraldas.py --mm21       → exibe tabela MM21 atual

# ─────────────────────────────────────────────
#  REFERÊNCIA — SCHEDULED TASKS PYTHONANYWHERE
# ─────────────────────────────────────────────
#
#  Plano gratuito (2 tasks/dia):
#    Task 1:  08:00  →  /home/SEU_USER/.local/bin/python3.10 /home/SEU_USER/bot_fraldas.py
#    Task 2:  20:00  →  (mesmo comando)
#
#  Plano pago (cron):
#    0 */6 * * *  /home/SEU_USER/.local/bin/python3.10 /home/SEU_USER/bot_fraldas.py
#
#  Ver log em tempo real:
#    tail -f /home/SEU_USER/bot_fraldas.log
#
#  Analisar histórico:
#    python3.10 /home/SEU_USER/bot_fraldas.py --analisar
