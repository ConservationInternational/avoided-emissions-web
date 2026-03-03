"""Email service using SparkPost for transactional emails.

Follows the same pattern as the Trends.Earth API email service.
Falls back to logging when the SparkPost API key is not configured.
"""

import logging

from config import Config, report_exception

logger = logging.getLogger(__name__)


def send_html_email(recipients, html, subject, from_email=None):
    """Send an HTML email via SparkPost.

    Parameters
    ----------
    recipients : list[str]
        Email addresses to send to.
    html : str
        HTML body of the email.
    subject : str
        Email subject line.
    from_email : str, optional
        Sender address.  Defaults to ``Config.SPARKPOST_FROM_EMAIL``.

    Returns
    -------
    dict
        SparkPost API response, or a dict with an ``errors`` key when
        email is disabled.
    """
    if from_email is None:
        from_email = Config.SPARKPOST_FROM_EMAIL

    api_key = Config.SPARKPOST_API_KEY
    if not api_key:
        logger.warning(
            "Cannot send email with subject '%s' to %d recipients: "
            "SPARKPOST_API_KEY is not configured. Email functionality is disabled.",
            subject,
            len(recipients),
        )
        return {"errors": ["Email disabled: SPARKPOST_API_KEY not configured"]}

    try:
        from sparkpost import SparkPost

        sp = SparkPost(api_key)
        response = sp.transmissions.send(
            recipients=recipients,
            html=html,
            from_email=from_email,
            subject=subject,
        )
        logger.info("Email sent: subject='%s', recipients=%s", subject, recipients)
        return response
    except Exception:
        logger.exception("Failed to send email with subject '%s'", subject)
        report_exception(subject=subject, recipients=str(recipients))
        raise
