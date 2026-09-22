"""里程换算纯逻辑测试：缺片、重叠、轮径版本、推算标记。"""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from inspection.positioning import compute_positioning, compute_scale, explain_point


def seg(sid, seq, a, b):
    return {"id": sid, "seq": seq, "encoder_start": a, "encoder_end": b}


class PositioningTest(unittest.TestCase):
    def test_scale_formula(self):
        self.assertAlmostEqual(compute_scale(200.0, 1000), 0.0006283185, places=9)

    def test_continuous_segments_have_no_blockers(self):
        result = compute_positioning(
            [seg("a", 0, 0, 10000), seg("b", 1, 10000, 20000)],
            wheel_diameter_mm=200.0, encoder_ppr=1000,
        )
        self.assertEqual(result.blockers, [])
        self.assertTrue(result.coverage_complete)
        self.assertFalse(any(r.provisional for r in result.rows))
        self.assertAlmostEqual(result.rows[1].chainage_start_m,
                               result.rows[0].chainage_end_m, places=6)

    def test_missing_seq_and_mileage_gap_both_raise_blockers(self):
        result = compute_positioning(
            [seg("a", 0, 0, 10000), seg("c", 2, 25000, 35000)],
            wheel_diameter_mm=200.0, encoder_ppr=1000,
        )
        kinds = {f.kind for f in result.blockers}
        self.assertIn("missing_segment", kinds)
        keys = {f.dedup_key for f in result.blockers}
        self.assertIn("seq-gap:1-1", keys)
        self.assertTrue(any("mileage-gap" in k for k in keys))
        # 冲突点之前可信，之后全部为推算
        self.assertFalse(result.rows[0].provisional)
        self.assertTrue(result.rows[1].provisional)

    def test_overlap_is_conflict_not_silently_stitched(self):
        result = compute_positioning(
            [seg("a", 0, 0, 20000), seg("b", 1, 18000, 30000)],
            wheel_diameter_mm=200.0, encoder_ppr=1000,
        )
        self.assertEqual(len(result.blockers), 1)
        self.assertEqual(result.blockers[0].kind, "overlap")
        self.assertTrue(result.rows[1].provisional)

    def test_new_wheel_diameter_recomputes_chainage(self):
        segs = [seg("a", 0, 0, 20000)]
        old = compute_positioning(segs, 200.0, 1000)
        new = compute_positioning(segs, 190.0, 1000)
        self.assertNotAlmostEqual(
            old.rows[0].chainage_end_m, new.rows[0].chainage_end_m
        )
        self.assertLess(new.rows[0].chainage_end_m, old.rows[0].chainage_end_m)

    def test_explain_point_math(self):
        position = compute_positioning(
            [seg("a", 0, 0, 20000), seg("b", 1, 20000, 40000)], 200.0, 1000
        )
        seg_b = seg("b", 1, 20000, 40000)
        explanation = explain_point(
            30000, seg_b,
            {"chainage_start_m": position.rows[1].chainage_start_m,
             "provisional": False},
            200.0, 1000,
        )
        self.assertAlmostEqual(explanation["pulses_into_segment"], 10000)
        self.assertAlmostEqual(
            explanation["chainage_m"],
            position.rows[1].chainage_start_m + explanation["meters_into_segment"],
            places=6,
        )


if __name__ == "__main__":
    unittest.main()
