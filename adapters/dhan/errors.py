class DhanAuthError(Exception):
    """Raised when Dhan authentication fails."""
    pass


class DhanOrderError(Exception):
    """Raised for normal order failures."""
    pass


class DhanSuperOrderError(Exception):
    """Raised for Super Order failures."""
    pass
