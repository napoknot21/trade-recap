from __future__ import annotations

import os
import polars as pl
import datetime as dt

import win32com.client as win32
import pythoncom as pycom

from html import unescape
from typing import Dict, List, Optional, Sequence, Any

from src.config import (
    EMAIL_DEFAULT_TO, EMAIL_DEFAULT_CC, EMAIL_DEFAULT_FROM, EMAIL_DEFAULT_BODY_INTRO,
    RECAP_EMAIL_ABS_DIR
)
from src.recap import build_recap_from_roots, build_email_body_from_df


# ---------------- Utilities ----------------

def _now_stamp() -> str:
    return dt.datetime.now().strftime("%Y-%m-%dT%H_%M")


def _ensure_dir(path: str) -> None:
    if path and not os.path.isdir(path):
        os.makedirs(path, exist_ok=True)


def _join_recipients(val: Optional[Sequence[str] | str]) -> str:
    """
    Accepts None / str / list[str] and returns the semicolon-separated string Outlook expects.
    """
    if val is None:
        return ""
    if isinstance(val, str):
        return val
    return "; ".join([v for v in val if v])


def _apply_from_account(mail, outlook_app, from_email: Optional[str]) -> None:
    """
    Prefer SendUsingAccount when from_email matches one of your configured Outlook accounts.
    Fall back to SentOnBehalfOfName (delegate; requires permission) if not.
    Silently ignore if not permitted.
    """
    if not from_email:
        return

    # 1) Try an account you own (no special rights needed)
    try:
        session = outlook_app.Session
        accounts = session.Accounts  # 1-based COM collection
        for i in range(1, accounts.Count + 1):
            acc = accounts.Item(i)
            smtp = getattr(acc, "SmtpAddress", None)
            if smtp and smtp.lower() == from_email.lower():
                mail.SendUsingAccount = acc
                return
    except Exception:
        pass

    # 2) Delegate / on-behalf (Exchange permissions required)
    try:
        mail.SentOnBehalfOfName = from_email  # note the 't' in Sent
    except Exception:
        # No rights or unresolvable name – just skip and use default From
        pass


# ------------- DF -> HTML -------------------

def generate_html_template_body (
        
        dataframe: pl.DataFrame,
        *,
        intro: Optional[str] = None,
        caption: str = "Trades Recap",
        max_rows: int = 2000,
        zebra: bool = True,
    
    ) -> str:
    """
    Build the HTML block (intro paragraph + table) from a Polars DF.
    """
    intro = EMAIL_DEFAULT_BODY_INTRO if intro is None else intro
    recap_df = build_recap_from_roots(dataframe)
    
    html_block = build_email_body_from_df(
    
        recap_df,
        intro_text=intro,
        caption=caption,
        max_rows=max_rows,
        zebra=zebra,
    
    )
    
    html_block = unescape(html_block)
    
    return html_block


def generate_html_from_df(dataframe: pl.DataFrame):
    return None


# ------------- Outlook: create/save/send -------------------

def create_email_item(
    *,
    to_email: Optional[List[str]] = None,
    cc_email: Optional[List[str]] = None,
    from_email: Optional[str] = None,
    subject: str = "",
    intro: Optional[str] = None,
    body: Optional[str] = None,
    dataframe: Optional[pl.DataFrame] = None,
    display: bool = True,
    attachments: Optional[List[str]] = None,
    use_signature: bool = True,           # <<< preserve default signature
    place_html_above_signature: bool = False,
) -> Any:
    """
    Create (and optionally display) an Outlook draft. Never auto-sends.
    If `body` is None and `dataframe` is provided, an HTML recap is generated.
    """
    # Build HTML if needed
    intro = EMAIL_DEFAULT_BODY_INTRO if intro is None else intro
    
    if body is None and dataframe is not None:
        body = generate_html_template_body(dataframe, intro=intro)

    if body is None:
        body = f"<p>{EMAIL_DEFAULT_BODY_INTRO}</p>"

    # Initialize COM
    pycom.CoInitialize()

    try:
        outlook_app = win32.Dispatch('Outlook.Application')
        mail = outlook_app.CreateItem(0)

        # Recipients (fallback to defaults)
        to_line = _join_recipients(to_email) or _join_recipients(EMAIL_DEFAULT_TO)
        cc_line = _join_recipients(cc_email) or _join_recipients(EMAIL_DEFAULT_CC)
        mail.To = to_line
        mail.CC = cc_line

        # From (account or delegate)
        #eff_from = (EMAIL_DEFAULT_FROM if from_email is None else from_email) or ""
        #_apply_from_account(mail, outlook_app, eff_from)

        # Subject
        mail.Subject = subject or ""

        # Body + signature handling
        block = body
        if use_signature and display:
            # Display first so Outlook injects the default signature/stationery
            mail.Display()
            existing = mail.HTMLBody or ""
            if place_html_above_signature:
                mail.HTMLBody = f"<html><body>{block}{existing}</body></html>"
            else:
                mail.HTMLBody = f"<html><body>{existing}{block}</body></html>"
        else:
            mail.HTMLBody = f"<html><body>{block}</body></html>"
            if display:
                mail.Display()

        # Attachments
        for p in attachments or []:
            if p and os.path.isfile(p):
                mail.Attachments.Add(Source=os.path.abspath(p))

        return mail

    finally:
        # keep COM initialized if caller continues; otherwise you could CoUninitialize()
        pass


def save_email_item(
    email_item: Any,
    *,
    abs_path_directory: Optional[str] = None
) -> Optional[Dict]:
    """
    Saves an email item to .msg and returns a status dict.
    """
    file_name = f"message_{_now_stamp()}.msg"
    save_dir = RECAP_EMAIL_ABS_DIR if abs_path_directory is None else abs_path_directory
    _ensure_dir(save_dir)
    save_path = os.path.join(save_dir, file_name)

    status: Dict[str, Optional[str] | bool] = {
        "success": False,
        "message": None,
        "path": None
    }

    try:
        # 3 = olMSGUnicode
        email_item.SaveAs(save_path, 3)
        status["success"] = True
        status["message"] = "Email saved successfully"
        status["path"] = save_path
    except Exception as e:
        status["message"] = f"Failed to save email: {str(e)}"

    return status


def send_email(email_item: Any) -> bool:
    """
    Sends the given Outlook mail item immediately via Outlook.
    """
    try:
        email_item.Send()
        return True
    except Exception as e:
        print(f"[-] Failed to send email: {e}")
        return False


def generate_timestamped_name() -> str:
    # Keep for backwards-compat; reuse single implementation
    return _now_stamp()
