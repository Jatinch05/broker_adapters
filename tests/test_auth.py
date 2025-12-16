"""
Tests for Dhan Authentication
"""
import pytest
from unittest.mock import Mock, patch
from apis.dhan.auth import authenticate, DhanAuthError


class TestDhanAuthentication:
    """Test Dhan authentication"""

    def test_successful_authentication(self):
        """Test successful authentication"""
        with patch('apis.dhan.auth.dhanhq') as mock_dhanhq:
            mock_client = Mock()
            mock_client.get_positions.return_value = []
            mock_dhanhq.return_value = mock_client

            result = authenticate("1000000003", "test_token")

            assert result == mock_client
            mock_dhanhq.assert_called_once_with("1000000003", "test_token")
            mock_client.get_positions.assert_called_once()

    def test_missing_credentials(self):
        """Test authentication with missing credentials"""
        with pytest.raises(DhanAuthError) as exc:
            authenticate("", "test_token")
        assert "required" in str(exc.value)

        with pytest.raises(DhanAuthError) as exc:
            authenticate("1000000003", "")
        assert "required" in str(exc.value)

    def test_authentication_failure(self):
        """Test authentication failure"""
        with patch('apis.dhan.auth.dhanhq') as mock_dhanhq:
            mock_client = Mock()
            mock_client.get_positions.side_effect = Exception("Invalid credentials")
            mock_dhanhq.return_value = mock_client

            with pytest.raises(DhanAuthError) as exc:
                authenticate("1000000003", "bad_token")
            assert "authentication failed" in str(exc.value).lower()
