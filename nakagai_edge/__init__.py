"""Nakagai edge: the connector runtime a user runs on their own machine.

This package holds broker credentials and calls brokers. It holds no judgment: the
platform decides what may run, and signs the grants this package verifies. That
division is why it ships without pandas and without any nakagai.* import, which is
what makes `uvx nakagai-edge setup <code>` possible.

Destined to become its own public repository. Keep it self-contained.
"""
