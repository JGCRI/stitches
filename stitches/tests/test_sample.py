import unittest

import stitches.sample as samp


class TestSample(unittest.TestCase):

    def test_plot_my_dog(self):
        """Test to make sure `sum_ints` returns the expected value."""

        # plot my dog and return a matplotlib axis
        my_dog = samp.plot_my_dog()

        # ensure the plot returns an axes image object
        self.assertEqual("<class 'matplotlib.image.AxesImage'>", str(type(my_dog)))


if __name__ == '__main__':
    unittest.main()
