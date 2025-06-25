import smtplib
from email.mime.text import MIMEText
import os
import time

def send_error_email(error_message: str):
    """Sends an email notification when the automation fails."""
    sender_email = os.getenv("SMTP_EMAIL")
    password = os.getenv("SMTP_PASS")

    if not sender_email or not password:
        print("Warning: SMTP credentials not found in .env. Cannot send error email.")
        return

    subject = f"Critical Failure in Apollo-Instantly Automation - {time.strftime('%Y-%m-%d')}"
    body = f"""
    Hello,

    The automation script encountered a critical error and could not complete its run.

    Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}
    
    Error Details:
    ----------------
    {error_message}
    ----------------

    Please review the logs and the script's status immediately.

    - Automated Notification System
    """
    
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = sender_email
    msg['To'] = sender_email # Sends to self

    try:
        print("Attempting to send error notification email...")
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
        print("✅ Error notification email sent successfully.")
    except Exception as e:
        print(f"❌ FATAL: Failed to send error notification email. Error: {e}")