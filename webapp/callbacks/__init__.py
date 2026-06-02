"""Callbacks package — all Dash callback registration.

Re-exports the public API used by app.py.
"""

from callbacks._registration import register_callbacks

__all__ = ["register_callbacks"]
