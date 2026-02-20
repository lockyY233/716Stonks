import unittest

from stockbot.services.money import money


class MoneyTests(unittest.TestCase):
    def test_money_rounds_half_up(self) -> None:
        self.assertEqual(money(1), 1.0)
        self.assertEqual(money(1.234), 1.23)
        self.assertEqual(money(1.235), 1.24)
        self.assertEqual(money("2.675"), 2.68)


if __name__ == "__main__":
    unittest.main()
