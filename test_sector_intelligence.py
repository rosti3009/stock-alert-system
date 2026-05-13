from __future__ import annotations

import unittest

import sector_intelligence


class SectorIntelligenceTests(unittest.TestCase):
    def test_static_mapping_classifies_symbol(self):
        classification = sector_intelligence.classify_symbol("AAPL")

        self.assertEqual(classification["sector"], "Technology")
        self.assertEqual(classification["industry"], "Consumer Electronics")
        self.assertEqual(classification["correlation_cluster"], "MEGA_CAP_TECH")
        self.assertEqual(classification["source"], "static_mapping")

    def test_etf_theme_inference_classifies_sector_etf(self):
        classification = sector_intelligence.classify_symbol("SMH")

        self.assertEqual(classification["sector"], "Technology")
        self.assertEqual(classification["theme"], "Artificial Intelligence")
        self.assertEqual(classification["source"], "etf_theme_inference")

    def test_cached_enrichment_before_unknown_fallback(self):
        classification = sector_intelligence.classify_symbol(
            "TEST",
            cached_enrichment={
                "TEST": {
                    "sector": "Utilities",
                    "industry": "Electric Utilities",
                    "subsector": "Regulated Electric",
                    "theme": "Defensive Yield",
                    "volatility_group": "LOW",
                    "correlation_cluster": "UTILITIES_DEFENSIVE",
                }
            },
        )

        self.assertEqual(classification["sector"], "Utilities")
        self.assertEqual(classification["source"], "cached_enrichment")

    def test_unknown_fallback_is_explicit(self):
        classification = sector_intelligence.classify_symbol("ZZZZ")

        self.assertEqual(classification["sector"], "UNKNOWN")
        self.assertEqual(classification["industry"], "UNKNOWN")
        self.assertEqual(classification["source"], "fallback_unknown")

    def test_portfolio_summary_exposure_and_diversification(self):
        summary = sector_intelligence.build_portfolio_summary(
            positions=[
                {"symbol": "AAPL", "quantity": 5, "current_price": 100},
                {"symbol": "MSFT", "quantity": 5, "current_price": 100},
                {"symbol": "JPM", "quantity": 5, "current_price": 100},
            ],
            account_equity=5000,
            checked_at="2026-05-13T00:00:00+00:00",
        )

        self.assertTrue(summary["read_only"])
        self.assertTrue(summary["no_trading_actions"])
        self.assertEqual(summary["portfolio"]["total_market_value"], 1500.0)
        self.assertEqual(summary["exposure_by_sector"][0]["sector"], "Technology")
        self.assertAlmostEqual(summary["exposure_by_sector"][0]["exposure_percent"], 20.0)
        self.assertEqual(summary["exposure_by_industry"][0]["industry"], "Consumer Electronics")
        self.assertGreater(summary["diversification_score"], 0)
        self.assertTrue(summary["top_correlated_groups"])


if __name__ == "__main__":
    unittest.main()
