import re
from bs4 import BeautifulSoup

_PROMO_PATTERNS = [
    r"(?i)\bđây là sản phẩm\b.*",
    r"(?i)\bshop\b.*",
    r"(?i)\bchi(ỉ|́|̣)?\s+co(́|́)?\b.*",
    r"(?i)\btặng kèm\b.*",
    r"(?i)\bkhuyến mãi\b.*",
    r"(?i)\bcam kết\b.*",
    r"(?i)\bmiễn phí\b.*",
    r"(?i)\bmiễn phí vận chuyển\b.*",
    r"(?i)\bliên hệ\b.*",
    r"(?i)\bgiao hàng\b.*",
    r"(?i)\bthanh toán\b.*",
    r"(?i)http[s]?://\S+",
    r"(?i)www\.\S+",
    r"(?i)\bhot\b.*",
    r"(?i)\bnew\b.*",
    r"(?i)\bbest seller\b.*",
]


def remove_marketing_sentences(text: str) -> str:
    if not text:
        return text

    sents = re.split(r'(?<=[.!?])\s+', str(text))
    kept = []

    for s in sents:
        s_stripped = s.strip()
        if not s_stripped:
            continue

        skip = False
        for pat in _PROMO_PATTERNS:
            if re.search(pat, s_stripped):
                skip = True
                break

        if not skip:
            kept.append(s_stripped)

    return " ".join(kept).strip()


def clean_description(html_text, max_len: int = 400) -> str:
    if not html_text:
        return ""

    if isinstance(html_text, (dict, list)):
        html_text = str(html_text)

    soup = BeautifulSoup(html_text, "html.parser")
    text = soup.get_text(separator=" ", strip=True)
    text = " ".join(text.split())

    parts = re.split(r'(?<=[.!?])\s+', text, maxsplit=2)
    short = " ".join(parts[:2]) if parts else text

    if len(short) > max_len:
        short = short[:max_len].rstrip()
        if not short.endswith((".", "!", "?")):
            short = short.rsplit(" ", 1)[0] + "..."

    return short


def description_pipeline(html_text, max_len: int = 400, remove_marketing: bool = True) -> str:
    s = clean_description(html_text, max_len=max_len)

    if remove_marketing:
        s = remove_marketing_sentences(s)

    if len(s) > max_len:
        s = s[:max_len].rstrip()
        if not s.endswith((".", "!", "?")):
            s = s.rsplit(" ", 1)[0] + "..."

    return s